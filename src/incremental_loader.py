import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from pymongo import ASCENDING, DESCENDING, ReplaceOne
from src.mongo_setup import create_mongo_client, get_database
from config.settings import VALIDATED_COLLECTION
STATE_COLLECTION = "incremental_state"
CHANGE_LOG_COLLECTION = "mv_change_log"
STATE_ID = "materialized_views"
DELTA_STATE_ID = "delta_loader"
def utc_now_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)
def parse_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        except ValueError:
            return None
    return None
def to_number(value):
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value
    try:
        number = float(str(value).replace(",", "").strip())
        return int(number) if number.is_integer() else number
    except (TypeError, ValueError):
        return 0
def parse_items(items_json):
    if not items_json:
        return []
    if isinstance(items_json, list):
        return items_json
    if isinstance(items_json, dict):
        return [items_json]
    if isinstance(items_json, str):
        try:
            parsed = json.loads(items_json)
        except (json.JSONDecodeError, TypeError):
            return []
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
    return []
def build_contribution(order):
    order_date = order.get("order_date")
    if not order_date:
        return None
    day = str(order_date)[:10]
    products = {}
    for item in parse_items(order.get("items_json")):
        sku = str(item.get("sku", "")).strip()
        if not sku:
            continue
        name = str(item.get("name", "")).strip()
        quantity = to_number(item.get("qty", 0))
        item_total = item.get("total")
        if item_total is None:
            item_total = quantity * to_number(item.get("unit_price", 0))
        else:
            item_total = to_number(item_total)
        if sku not in products:
            products[sku] = {
                "sku": sku,
                "name": name,
                "quantity": 0,
                "sales": 0,
            }
        products[sku]["quantity"] += quantity
        products[sku]["sales"] += item_total
    return {
        "day": day,
        "total_sales": to_number(order.get("total_amount", 0)),
        "products": list(products.values()),
    }
def ensure_incremental_state(db):
    state = db[STATE_COLLECTION]
    state.create_index(
        [("watermark", ASCENDING)],
        name="ix_incremental_watermark",
    )
    change_log = db[CHANGE_LOG_COLLECTION]
    change_log.create_index(
        [("operation_id", ASCENDING)],
        unique=True,
        name="uq_mv_change_operation_id",
    )
    change_log.create_index(
        [("applied_at", ASCENDING)],
        name="ix_mv_change_applied_at",
    )
    change_log.create_index(
        [("order_id", ASCENDING), ("event_time", DESCENDING)],
        name="ix_mv_change_order_event",
    )
    return state, change_log
def get_watermark(db):
    document = db[STATE_COLLECTION].find_one({"_id": STATE_ID})
    if not document:
        return None
    return document.get("watermark")
def set_watermark(db, watermark):
    parsed = parse_datetime(watermark)
    if parsed is None:
        return
    db[STATE_COLLECTION].update_one(
        {"_id": STATE_ID},
        {"$set": {"watermark": parsed, "updated_at": utc_now_naive()}},
        upsert=True,
    )
def get_delta_watermark(db):
    document = db[STATE_COLLECTION].find_one({"_id": DELTA_STATE_ID})
    if not document:
        return None
    return document.get("watermark")
def set_delta_watermark(db, watermark):
    parsed = parse_datetime(watermark)
    if parsed is None:
        return
    db[STATE_COLLECTION].update_one(
        {"_id": DELTA_STATE_ID},
        {"$set": {"watermark": parsed, "updated_at": utc_now_naive()}},
        upsert=True,
    )
def get_changed_orders(db, watermark=None):
    query = {}
    if watermark is not None:
        query["last_updated_at"] = {"$gt": parse_datetime(watermark)}
    return db[VALIDATED_COLLECTION].find(
        query,
        {
            "_id": 0,
            "order_id": 1,
            "order_date": 1,
            "total_amount": 1,
            "items_json": 1,
            "last_updated_at": 1,
            "lineage": 1,
        },
    ).sort("last_updated_at", ASCENDING)
def get_previous_contribution(db, current_order):
    order_id = current_order.get("order_id")
    current_time = parse_datetime(current_order.get("last_updated_at"))
    if order_id and current_time is not None:
        previous = db[CHANGE_LOG_COLLECTION].find_one(
            {
                "order_id": order_id,
                "status": "applied",
                "event_time": current_time,
            },
            {"previous_contribution": 1, "_id": 0},
        )
        if previous is not None:
            return previous.get("previous_contribution")
    return None
def get_latest_update_time(db):
    document = db[VALIDATED_COLLECTION].find_one(
        {"last_updated_at": {"$exists": True}},
        {"last_updated_at": 1, "_id": 0},
        sort=[("last_updated_at", DESCENDING)],
    )
    if not document:
        return None
    return document.get("last_updated_at")
def normalize_delta_record(record, fallback_operation_id=None):
    document = dict(record)
    operation_id = str(
        document.pop("operation_id", fallback_operation_id or "")
    ).strip()
    if not operation_id:
        raise ValueError("Delta record is missing operation_id")
    operation = str(document.pop("operation", "update")).strip().lower()
    if operation not in {"insert", "update"}:
        raise ValueError(
            f"Unsupported operation {operation!r}; use insert or update"
        )
    order_id = str(document.get("order_id", "")).strip()
    if not order_id:
        raise ValueError("Delta record is missing order_id")
    event_time = parse_datetime(document.get("last_updated_at"))
    if event_time is None:
        event_time = utc_now_naive()
    document["order_id"] = order_id
    document["last_updated_at"] = event_time
    document.setdefault("incremental_operation_id", operation_id)
    document.setdefault("incremental_operation", operation)
    return operation_id, operation, event_time, document
def load_delta_file(path):
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(file_path)
    text = file_path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text.startswith("["):
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError("Delta JSON must contain a list of records")
        return data
    records = []
    for line_number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Invalid JSON at line {line_number}: {exc}"
            ) from exc
        if not isinstance(record, dict):
            raise ValueError(
                f"Delta line {line_number} must be a JSON object"
            )
        records.append(record)
    return records
def apply_delta(db, records):
    ensure_incremental_state(db)
    collection = db[VALIDATED_COLLECTION]
    change_log = db[CHANGE_LOG_COLLECTION]
    collection.create_index(
        [("order_id", ASCENDING)],
        unique=True,
        name="uq_orders_validated_order_id",
    )
    inserted = 0
    updated = 0
    unchanged = 0
    applied = 0
    max_event_time = get_delta_watermark(db)
    for index, raw_record in enumerate(records, 1):
        operation_id, operation, event_time, new_document = normalize_delta_record(
            raw_record,
            fallback_operation_id=f"delta-{index}",
        )
        existing_event = change_log.find_one(
            {"operation_id": operation_id},
            {"_id": 1},
        )
        if existing_event:
            unchanged += 1
            continue
        order_id = new_document["order_id"]
        old_document = collection.find_one({"order_id": order_id})
        old_event_time = (
            parse_datetime(old_document.get("last_updated_at"))
            if old_document
            else None
        )
        if old_event_time is not None and event_time <= old_event_time:
            change_log.insert_one(
                {
                    "operation_id": operation_id,
                    "order_id": order_id,
                    "operation": operation,
                    "status": "unchanged_stale_version",
                    "event_time": event_time,
                    "applied_at": utc_now_naive(),
                    "previous_contribution": build_contribution(old_document),
                    "new_contribution": None,
                }
            )
            unchanged += 1
            continue
        previous_contribution = build_contribution(old_document) if old_document else None
        new_document.setdefault(
            "first_processed_at",
            old_document.get("first_processed_at") if old_document else utc_now_naive(),
        )
        new_document["last_updated_at"] = event_time
        collection.replace_one(
            {"order_id": order_id},
            new_document,
            upsert=True,
        )
        new_contribution = build_contribution(new_document)
        change_log.insert_one(
            {
                "operation_id": operation_id,
                "order_id": order_id,
                "operation": operation,
                "status": "applied",
                "event_time": event_time,
                "applied_at": utc_now_naive(),
                "previous_contribution": previous_contribution,
                "new_contribution": new_contribution,
            }
        )
        applied += 1
        if old_document:
            updated += 1
        else:
            inserted += 1
        if max_event_time is None or event_time > max_event_time:
            max_event_time = event_time
    if applied > 0 and max_event_time is not None:
        set_delta_watermark(db, max_event_time)
    return {
        "applied": applied,
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "watermark": max_event_time,
    }
def show_state(db):
    state = db[STATE_COLLECTION].find_one({"_id": STATE_ID})
    pending = db[CHANGE_LOG_COLLECTION].count_documents({"status": "applied"})
    print(
        json.dumps(
            {
                "materialized_views_watermark": state.get("watermark") if state else None,
                "delta_loader_watermark": get_delta_watermark(db),
                "change_log_applied": pending,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )
def main():
    parser = argparse.ArgumentParser(
        description="Incremental delta loader with idempotent upsert and change log"
    )
    parser.add_argument("--delta", help="JSON or JSONL delta file")
    parser.add_argument("--state", action="store_true")
    args = parser.parse_args()
    if not args.delta and not args.state:
        parser.error("Use --delta PATH or --state")
    client = None
    try:
        client = create_mongo_client()
        db = get_database(client)
        ensure_incremental_state(db)
        if args.delta:
            records = load_delta_file(args.delta)
            result = apply_delta(db, records)
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        if args.state:
            show_state(db)
    finally:
        if client is not None:
            client.close()
if __name__ == "__main__":
    main()
