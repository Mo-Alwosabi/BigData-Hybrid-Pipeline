import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pymongo import ASCENDING, DESCENDING, UpdateOne
from src.mongo_setup import create_mongo_client, get_database
from src.incremental_loader import (
    get_watermark,
    set_watermark,
    get_latest_update_time,
    get_changed_orders,
    get_previous_contribution,
    build_contribution,
    ensure_incremental_state,
)

DAILY_MV = "daily_sales_summary"
PRODUCT_MV = "top_products_summary"
VALIDATED = "orders_validated"

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

def ensure_mv_indexes(db):
    ensure_incremental_state(db)
    db[VALIDATED].create_index(
        [("last_updated_at", ASCENDING)],
        name="ix_validated_last_updated_at",
    )
    db[DAILY_MV].create_index(
        [("day", ASCENDING)],
        unique=True,
        name="uq_daily_sales_day",
    )
    db[PRODUCT_MV].create_index(
        [("sku", ASCENDING)],
        unique=True,
        name="uq_top_products_sku",
    )
    db[PRODUCT_MV].create_index(
        [("sales", DESCENDING)],
        name="ix_top_products_sales",
    )

def initial_daily_sales(db):
    pipeline = [
        {
            "$project": {
                "day": {
                    "$substrBytes": [
                        "$order_date",
                        0,
                        10,
                    ]
                },
                "total_amount": 1,
            }
        },
        {
            "$group": {
                "_id": "$day",
                "order_count": {"$sum": 1},
                "total_sales": {
                    "$sum": {
                        "$ifNull": [
                            "$total_amount",
                            0,
                        ]
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "day": "$_id",
                "order_count": 1,
                "total_sales": 1,
            }
        },
        {
            "$sort": {
                "day": 1,
            }
        },
    ]
    db[DAILY_MV].delete_many({})
    documents = list(
        db[VALIDATED].aggregate(
            pipeline,
            allowDiskUse=True,
        )
    )
    if documents:
        db[DAILY_MV].insert_many(
            documents,
            ordered=False,
        )
    return len(documents)

def initial_top_products(db):
    db[PRODUCT_MV].delete_many({})
    counters = defaultdict(
        lambda: {
            "name": "",
            "quantity": 0,
            "sales": 0,
        }
    )
    cursor = db[VALIDATED].find(
        {},
        {
            "order_date": 1,
            "items_json": 1,
            "_id": 0,
        },
    ).batch_size(5000)
    scanned = 0
    for order in cursor:
        scanned += 1
        contribution = build_contribution(order)
        if not contribution:
            continue
        for item in contribution["products"]:
            sku = item["sku"]
            counters[sku]["name"] = (
                item.get("name", "")
                or counters[sku]["name"]
            )
            counters[sku]["quantity"] += to_number(
                item.get("quantity", 0)
            )
            counters[sku]["sales"] += to_number(
                item.get("sales", 0)
            )
        if scanned % 500000 == 0:
            print(
                f"Top products initial scan: "
                f"{scanned:,}"
            )
    if counters:
        operations = []
        for sku, item in counters.items():
            operations.append(
                UpdateOne(
                    {"sku": sku},
                    {
                        "$set": {
                            "sku": sku,
                            "name": item["name"],
                            "quantity": item["quantity"],
                            "sales": item["sales"],
                        }
                    },
                    upsert=True,
                )
            )
            if len(operations) >= 5000:
                db[PRODUCT_MV].bulk_write(
                    operations,
                    ordered=False,
                )
                operations.clear()
        if operations:
            db[PRODUCT_MV].bulk_write(
                operations,
                ordered=False,
            )
    return scanned, len(counters)

def initial_build(db):
    ensure_mv_indexes(db)
    print("Building daily_sales_summary...")
    daily_count = initial_daily_sales(db)
    print(f"Daily MV groups: {daily_count:,}")
    print("Building top_products_summary...")
    scanned, product_count = initial_top_products(db)
    latest = get_latest_update_time(db)
    set_watermark(db, latest)
    return {
        "mode": "initial",
        "daily_groups": daily_count,
        "orders_scanned_for_products": scanned,
        "product_count": product_count,
        "watermark": latest,
    }

def apply_daily_delta(
    db,
    old_contribution,
    new_contribution,
):
    updates = {}
    if old_contribution:
        day = old_contribution["day"]
        updates.setdefault(
            day,
            {
                "order_count": 0,
                "total_sales": 0,
            },
        )
        updates[day]["order_count"] -= 1
        updates[day]["total_sales"] -= to_number(
            old_contribution["total_sales"]
        )
    if new_contribution:
        day = new_contribution["day"]
        updates.setdefault(
            day,
            {
                "order_count": 0,
                "total_sales": 0,
            },
        )
        updates[day]["order_count"] += 1
        updates[day]["total_sales"] += to_number(
            new_contribution["total_sales"]
        )
    for day, delta in updates.items():
        if (
            delta["order_count"] == 0
            and delta["total_sales"] == 0
        ):
            continue
        db[DAILY_MV].update_one(
            {"day": day},
            {
                "$inc": {
                    "order_count": delta["order_count"],
                    "total_sales": delta["total_sales"],
                }
            },
            upsert=True,
        )

def apply_product_delta(
    db,
    old_contribution,
    new_contribution,
):
    deltas = defaultdict(
        lambda: {
            "name": "",
            "quantity": 0,
            "sales": 0,
        }
    )
    for contribution, sign in (
        (old_contribution, -1),
        (new_contribution, 1),
    ):
        if not contribution:
            continue
        for item in contribution["products"]:
            sku = item["sku"]
            deltas[sku]["name"] = (
                item.get("name", "")
                or deltas[sku]["name"]
            )
            deltas[sku]["quantity"] += (
                sign
                * to_number(
                    item.get("quantity", 0)
                )
            )
            deltas[sku]["sales"] += (
                sign
                * to_number(
                    item.get("sales", 0)
                )
            )
    operations = []
    for sku, delta in deltas.items():
        operations.append(
            UpdateOne(
                {"sku": sku},
                {
                    "$setOnInsert": {
                        "sku": sku,
                        "name": delta["name"],
                    },
                    "$inc": {
                        "quantity": delta["quantity"],
                        "sales": delta["sales"],
                    },
                },
                upsert=True,
            )
        )
    if operations:
        db[PRODUCT_MV].bulk_write(
            operations,
            ordered=False,
        )

def incremental_refresh(db):
    ensure_mv_indexes(db)
    watermark = get_watermark(db)
    if watermark is None:
        raise RuntimeError(
            "Incremental watermark is not initialized. "
            "Run --initial first."
        )
    changed = get_changed_orders(
        db,
        watermark,
    )
    processed = 0
    inserted = 0
    updated = 0
    unchanged = 0
    max_watermark = watermark
    for current in changed:
        processed += 1
        current_contribution = build_contribution(
            current
        )
        previous_contribution = (
            get_previous_contribution(
                db,
                current,
            )
        )
        if previous_contribution:
            updated += 1
        else:
            inserted += 1
        if (
            previous_contribution is not None
            and current_contribution
            == previous_contribution
        ):
            unchanged += 1
        else:
            apply_daily_delta(
                db,
                previous_contribution,
                current_contribution,
            )
            apply_product_delta(
                db,
                previous_contribution,
                current_contribution,
            )
        current_time = current.get(
            "last_updated_at"
        )
        if (
            current_time
            and (
                max_watermark is None
                or current_time > max_watermark
            )
        ):
            max_watermark = current_time
        if processed % 100000 == 0:
            print(
                f"Incremental refresh: "
                f"{processed:,}"
            )
    if processed > 0:
        set_watermark(
            db,
            max_watermark,
        )
    return {
        "mode": "incremental",
        "processed": processed,
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "old_watermark": watermark,
        "new_watermark": max_watermark,
    }

def show_views(db, limit=10):
    daily = list(
        db[DAILY_MV]
        .find(
            {},
            {"_id": 0},
        )
        .sort(
            "total_sales",
            DESCENDING,
        )
        .limit(limit)
    )
    products = list(
        db[PRODUCT_MV]
        .find(
            {},
            {"_id": 0},
        )
        .sort(
            "sales",
            DESCENDING,
        )
        .limit(limit)
    )
    print("daily_sales_summary:")
    print(
        json.dumps(
            daily,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )
    print("top_products_summary:")
    print(
        json.dumps(
            products,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Materialized Views "
            "with Incremental Refresh"
        )
    )
    parser.add_argument(
        "--initial",
        action="store_true",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
    )
    parser.add_argument(
        "--show",
        action="store_true",
    )
    args = parser.parse_args()
    if not any(
        (
            args.initial,
            args.refresh,
            args.show,
        )
    ):
        parser.error(
            "Use --initial, --refresh, or --show"
        )
    client = None
    try:
        client = create_mongo_client()
        db = get_database(client)
        if args.initial:
            result = initial_build(db)
            print(
                json.dumps(
                    result,
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
            )
        if args.refresh:
            result = incremental_refresh(db)
            print(
                json.dumps(
                    result,
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
            )
        if args.show:
            show_views(db)
    finally:
        if client is not None:
            client.close()

if __name__ == "__main__":
    main()