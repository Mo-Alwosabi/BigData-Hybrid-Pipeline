import hashlib
import argparse
import json
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

from pymongo import (
    ASCENDING,
    DESCENDING,
    ReplaceOne,
)

from config.settings import (
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    REPORTS_DIR,
)

from src.mongo_setup import (
    create_mongo_client,
    get_database,
)

from src.quality_rules import (
    classify_record,
    QUALITY_VALID,
    QUALITY_CORRECTED,
    QUALITY_QUARANTINED,
)


WRITE_BATCH_SIZE = 1000

# Maximum Raw documents whose existing final-state
# fingerprints are held in Python memory at one time.
STATE_LOOKUP_BATCH_SIZE = 2000


# ============================================================
# GENERIC HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def stable_json(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def fingerprint(value):
    return hashlib.sha256(
        stable_json(value).encode("utf-8")
    ).hexdigest()


def normalized_order_id(raw):
    value = raw.get("order_id")

    if value is None:
        return ""

    return str(value).strip()


# ============================================================
# DUPLICATE DETECTION
# ============================================================

def find_duplicate_order_ids(
    collection,
    run_id,
):
    pipeline = [
        {
            "$match": {
                "run_id": run_id
            }
        },
        {
            "$project": {
                "order_id": {
                    "$trim": {
                        "input": {
                            "$ifNull": [
                                "$raw_record.order_id",
                                "",
                            ]
                        }
                    }
                }
            }
        },
        {
            "$match": {
                "order_id": {
                    "$ne": ""
                }
            }
        },
        {
            "$group": {
                "_id": "$order_id",
                "record_count": {
                    "$sum": 1
                },
            }
        },
        {
            "$match": {
                "record_count": {
                    "$gt": 1
                }
            }
        },
    ]

    duplicate_ids = set()
    duplicate_records = 0

    for item in collection.aggregate(
        pipeline,
        allowDiskUse=True,
    ):
        duplicate_ids.add(
            item["_id"]
        )

        duplicate_records += int(
            item["record_count"]
        )

    return (
        duplicate_ids,
        duplicate_records,
    )


# ============================================================
# LOAD EXISTING FINAL STATE
# ============================================================

def load_existing_state(
    collection,
    key_field,
):
    state = {}

    cursor = collection.find(
        {
            key_field: {
                "$exists": True
            }
        },
        {
            "_id": 0,
            key_field: 1,
            "record_fingerprint": 1,
            "first_processed_at": 1,
        },
    )

    for document in cursor:

        key = document.get(
            key_field
        )

        if key is None:
            continue

        state[key] = {
            "fingerprint": document.get(
                "record_fingerprint"
            ),
            "first_processed_at": (
                document.get(
                    "first_processed_at"
                )
            ),
        }

    return state


# ============================================================
# BOUNDED-MEMORY EXISTING-STATE LOOKUP
# ============================================================

def iter_cursor_batches(
    cursor,
    batch_size,
):
    """
    Yield a bounded number of Raw MongoDB documents.

    Memory use depends on batch_size, not total dataset size.
    """

    batch = []

    for document in cursor:

        batch.append(
            document
        )

        if len(batch) >= batch_size:

            yield batch

            batch = []

    if batch:
        yield batch


def load_existing_state_for_keys(
    collection,
    key_field,
    keys,
):
    """
    Load final-state fingerprints only for the current
    bounded Raw batch.

    This replaces full-collection materialization for the
    production-scale ELT path.
    """

    unique_keys = {
        key
        for key in keys
        if key is not None
    }

    if not unique_keys:
        return {}

    state = {}

    cursor = collection.find(
        {
            key_field: {
                "$in": list(
                    unique_keys
                )
            }
        },
        {
            "_id": 0,
            key_field: 1,
            "record_fingerprint": 1,
            "first_processed_at": 1,
        },
    )

    for document in cursor:

        key = document.get(
            key_field
        )

        if key is None:
            continue

        state[key] = {
            "fingerprint": document.get(
                "record_fingerprint"
            ),
            "first_processed_at": (
                document.get(
                    "first_processed_at"
                )
            ),
        }

    return state


# ============================================================
# FINAL DOCUMENT BUILDERS
# ============================================================

def build_validated_document(
    raw_document,
    result,
    raw_run_id,
    processing_run_id,
    first_processed_at,
):
    cleaned = dict(
        result["cleaned_record"]
    )

    order_id = str(
        cleaned["order_id"]
    ).strip()

    cleaned["order_id"] = order_id

    stable_state = {
        "cleaned_record": cleaned,
        "quality_status": result[
            "quality_status"
        ],
        "corrections": result[
            "corrections"
        ],
    }

    record_fingerprint = fingerprint(
        stable_state
    )

    now = utc_now()

    final_document = dict(cleaned)

    final_document.update(
        {
            "quality_status": result[
                "quality_status"
            ],

            "corrections": result[
                "corrections"
            ],

            "record_fingerprint": (
                record_fingerprint
            ),

            "lineage": {
                "raw_run_id": raw_run_id,

                "source_file": (
                    raw_document.get(
                        "source_file"
                    )
                ),

                "source_path": (
                    raw_document.get(
                        "source_path"
                    )
                ),

                "source_row_number": (
                    raw_document.get(
                        "source_row_number"
                    )
                ),

                "raw_ingested_at": (
                    raw_document.get(
                        "ingested_at"
                    )
                ),

                "engine_used": (
                    raw_document.get(
                        "engine_used"
                    )
                ),
            },

            "last_processing_run_id": (
                processing_run_id
            ),

            "first_processed_at": (
                first_processed_at
                if first_processed_at
                is not None
                else now
            ),

            "last_updated_at": now,
        }
    )

    return (
        order_id,
        record_fingerprint,
        final_document,
    )


def build_quarantine_document(
    raw_document,
    result,
    raw_run_id,
    processing_run_id,
    first_processed_at,
):
    source_row_number = (
        raw_document.get(
            "source_row_number"
        )
    )

    raw_id = raw_document.get(
        "_id"
    )

    if source_row_number is not None:

        quarantine_key = (
            f"{raw_run_id}:"
            f"{source_row_number}"
        )

    else:

        quarantine_key = (
            f"{raw_run_id}:"
            f"{raw_id}"
        )

    stable_state = {
        "raw_record": raw_document.get(
            "raw_record"
        ),

        "cleaned_preview": result.get(
            "cleaned_record"
        ),

        "corrections": result.get(
            "corrections",
            [],
        ),

        "error_codes": result.get(
            "codes_error",
            [],
        ),

        "error_details": result.get(
            "details_error",
            [],
        ),
    }

    record_fingerprint = fingerprint(
        stable_state
    )

    now = utc_now()

    document = {
        "quarantine_key": (
            quarantine_key
        ),

        "order_id": (
            normalized_order_id(
                raw_document.get(
                    "raw_record",
                    {},
                )
            )
            or None
        ),

        "quality_status": (
            QUALITY_QUARANTINED
        ),

        "raw_record": raw_document.get(
            "raw_record"
        ),

        "cleaned_preview": result.get(
            "cleaned_record"
        ),

        "corrections": result.get(
            "corrections",
            [],
        ),

        "error_codes": result.get(
            "codes_error",
            [],
        ),

        "error_details": result.get(
            "details_error",
            [],
        ),

        "record_fingerprint": (
            record_fingerprint
        ),

        "source_run_id": raw_run_id,

        "source_file": raw_document.get(
            "source_file"
        ),

        "source_path": raw_document.get(
            "source_path"
        ),

        "source_row_number": (
            source_row_number
        ),

        "raw_ingested_at": (
            raw_document.get(
                "ingested_at"
            )
        ),

        "engine_used": raw_document.get(
            "engine_used"
        ),

        "last_processing_run_id": (
            processing_run_id
        ),

        "first_processed_at": (
            first_processed_at
            if first_processed_at
            is not None
            else now
        ),

        "last_updated_at": now,
    }

    return (
        quarantine_key,
        record_fingerprint,
        document,
    )


# ============================================================
# BULK WRITE
# ============================================================

def flush_operations(
    collection,
    operations,
):
    if not operations:
        return {
            "upserted": 0,
            "modified": 0,
            "matched": 0,
        }

    result = collection.bulk_write(
        operations,
        ordered=False,
    )

    summary = {
        "upserted": int(
            result.upserted_count
        ),
        "modified": int(
            result.modified_count
        ),
        "matched": int(
            result.matched_count
        ),
    }

    operations.clear()

    return summary


# ============================================================
# DRY-RUN CONTRACT
# ============================================================

def load_dry_run_contract(
    run_id,
):
    path = (
        REPORTS_DIR
        / "classification_dry_run.json"
    )

    if not path.exists():
        raise RuntimeError(
            "classification_dry_run.json "
            "does not exist."
        )

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:
        report = json.load(file)

    if report.get("run_id") != run_id:
        raise RuntimeError(
            "Dry-run report belongs to "
            "a different raw run."
        )

    consistency = report.get(
        "consistency",
        {}
    )

    if not consistency.get(
        "raw_equals_classified"
    ):
        raise RuntimeError(
            "Dry-run consistency was not PASS."
        )

    return report


# ============================================================
# MAIN ELT WRITE
# ============================================================


# ============================================================
# CLI
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Quality / Cleaning / Classification ELT "
            "for one Raw ingestion run."
        )
    )

    parser.add_argument(
        "--raw-run-id",
        default=None,
        help=(
            "Process this exact orders_raw run_id. "
            "If omitted, legacy latest-run behavior is used."
        ),
    )

    parser.add_argument(
        "--skip-dry-run-contract",
        action="store_true",
        help=(
            "Skip the 100K Phase-11 dry-run comparison. "
            "Intended for the explicitly selected official "
            "large production run."
        ),
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if (
        args.skip_dry_run_contract
        and not args.raw_run_id
    ):
        raise RuntimeError(
            "--skip-dry-run-contract requires "
            "an explicit --raw-run-id."
        )
    client = None

    try:
        client = create_mongo_client()
        db = get_database(client)

        raw_collection = db[
            RAW_COLLECTION
        ]

        validated_collection = db[
            VALIDATED_COLLECTION
        ]

        quarantine_collection = db[
            QUARANTINE_COLLECTION
        ]

        # ====================================================
        # SOURCE RUN
        # ====================================================

        if args.raw_run_id:

            raw_run_id = (
                args.raw_run_id.strip()
            )

            source_probe = (
                raw_collection.find_one(
                    {
                        "run_id": raw_run_id
                    },
                    {
                        "_id": 1,
                        "run_id": 1,
                    },
                )
            )

            if not source_probe:
                raise RuntimeError(
                    "Requested raw_run_id does not exist "
                    f"in orders_raw: {raw_run_id}"
                )

            raw_run_selection = (
                "explicit"
            )

        else:

            latest = raw_collection.find_one(
                {},
                sort=[
                    (
                        "ingested_at",
                        DESCENDING,
                    )
                ],
                projection={
                    "run_id": 1
                },
            )

            if not latest:
                raise RuntimeError(
                    "orders_raw is empty."
                )

            raw_run_id = latest[
                "run_id"
            ]

            raw_run_selection = (
                "latest_legacy"
            )

        processing_run_id = (
            "quality-"
            + datetime.now(
                timezone.utc
            ).strftime(
                "%Y%m%dT%H%M%SZ"
            )
            + "-"
            + uuid.uuid4().hex[:8]
        )

        raw_query = {
            "run_id": raw_run_id
        }

        raw_count = (
            raw_collection.count_documents(
                raw_query
            )
        )

        print("=" * 92)
        print(
            "PHASE 12 - ELT FINAL WRITE"
        )
        print("=" * 92)

        print(
            f"raw_run_id              : "
            f"{raw_run_id}"
        )

        print(
            f"processing_run_id       : "
            f"{processing_run_id}"
        )

        print(
            f"raw records             : "
            f"{raw_count:,}"
        )

        print(
            f"validated before        : "
            f"{validated_collection.count_documents({}):,}"
        )

        print(
            f"quarantine before       : "
            f"{quarantine_collection.count_documents({}):,}"
        )

        print(
            "MongoDB write mode      : ENABLED"
        )

        print("=" * 92)

        # ====================================================
        # SAFETY GATE: DRY RUN
        # ====================================================

        if args.skip_dry_run_contract:

            print(
                "\n[1/6] Phase 11 dry-run contract: SKIPPED"
            )

            print(
                "Reason                  : "
                "explicit official large run"
            )

            dry_report = None
            dry_classification = None

        else:

            print(
                "\n[1/6] Loading Phase 11 contract..."
            )

            dry_report = (
                load_dry_run_contract(
                    raw_run_id
                )
            )

            dry_classification = (
                dry_report[
                    "classification"
                ]
            )

            print(
                "Dry-run contract        : PASS"
            )

        # ====================================================
        # DUPLICATES
        # ====================================================

        print(
            "\n[2/6] Detecting duplicate order IDs..."
        )

        (
            duplicate_ids,
            duplicate_record_count,
        ) = find_duplicate_order_ids(
            raw_collection,
            raw_run_id,
        )

        print(
            f"Duplicate groups        : "
            f"{len(duplicate_ids):,}"
        )

        print(
            f"Duplicate records       : "
            f"{duplicate_record_count:,}"
        )

        # ====================================================
        # INDEXES
        # ====================================================

        print(
            "\n[3/6] Creating / verifying indexes..."
        )

        validated_collection.create_index(
            [
                (
                    "order_id",
                    ASCENDING,
                )
            ],
            unique=True,
            name="uq_orders_validated_order_id",
        )

        validated_collection.create_index(
            [
                (
                    "quality_status",
                    ASCENDING,
                )
            ],
            name="ix_validated_quality_status",
        )

        quarantine_collection.create_index(
            [
                (
                    "quarantine_key",
                    ASCENDING,
                )
            ],
            unique=True,
            name="uq_quarantine_key",
        )

        quarantine_collection.create_index(
            [
                (
                    "source_run_id",
                    ASCENDING,
                )
            ],
            name="ix_quarantine_source_run",
        )

        quarantine_collection.create_index(
            [
                (
                    "error_codes",
                    ASCENDING,
                )
            ],
            name="ix_quarantine_error_codes",
        )

        print(
            "Indexes                 : PASS"
        )

        # ====================================================
        # EXISTING FINAL STATE - BOUNDED LOOKUP
        # ====================================================

        print(
            "\n[4/6] Configuring bounded existing-state lookups..."
        )

        print(
            f"State lookup batch      : "
            f"{STATE_LOOKUP_BATCH_SIZE:,}"
        )

        print(
            "Full-state materialize  : DISABLED"
        )

        print(
            "State lookup strategy   : MongoDB $in per Raw batch"
        )

        # ====================================================
        # PROCESS + WRITE
        # ====================================================

        print(
            "\n[5/6] Classifying and writing..."
        )

        status_counts = Counter()

        validated_write_counts = Counter()
        quarantine_write_counts = Counter()

        error_code_counts = Counter()
        correction_rule_counts = Counter()

        validated_operations = []
        quarantine_operations = []

        mongo_actual = Counter()

        scanned = 0

        started = time.perf_counter()

        cursor = raw_collection.find(
            raw_query,
            projection={
                "_id": 1,
                "run_id": 1,
                "source_file": 1,
                "source_path": 1,
                "source_row_number": 1,
                "ingested_at": 1,
                "engine_used": 1,
                "raw_record": 1,
            },
        ).batch_size(2000)

        for raw_batch in iter_cursor_batches(
            cursor,
            STATE_LOOKUP_BATCH_SIZE,
        ):
            # -------------------------------------------
            # PREFETCH EXISTING STATE FOR THIS BATCH ONLY
            # -------------------------------------------

            validated_lookup_keys = []
            quarantine_lookup_keys = []

            for batch_document in raw_batch:

                batch_raw = batch_document.get(
                    "raw_record",
                    {},
                )

                batch_order_id = (
                    normalized_order_id(
                        batch_raw
                    )
                )

                if batch_order_id:
                    validated_lookup_keys.append(
                        batch_order_id
                    )

                batch_source_row = (
                    batch_document.get(
                        "source_row_number"
                    )
                )

                batch_quarantine_key = (
                    f"{raw_run_id}:"
                    f"{batch_source_row}"
                    if batch_source_row is not None
                    else
                    f"{raw_run_id}:"
                    f"{batch_document['_id']}"
                )

                quarantine_lookup_keys.append(
                    batch_quarantine_key
                )

            validated_state = (
                load_existing_state_for_keys(
                    validated_collection,
                    "order_id",
                    validated_lookup_keys,
                )
            )

            quarantine_state = (
                load_existing_state_for_keys(
                    quarantine_collection,
                    "quarantine_key",
                    quarantine_lookup_keys,
                )
            )

            for raw_document in raw_batch:

                scanned += 1

                raw = raw_document.get(
                    "raw_record",
                    {},
                )

                order_id = (
                    normalized_order_id(
                        raw
                    )
                )

                duplicate_conflict = (
                    order_id
                    in duplicate_ids
                )

                result = classify_record(
                    raw,
                    duplicate_conflict=(
                        duplicate_conflict
                    ),
                )

                status = result[
                    "quality_status"
                ]

                status_counts[
                    status
                ] += 1

                # -----------------------------------------------
                # RULE + ERROR METRICS
                # -----------------------------------------------

                for correction in result.get(
                    "corrections",
                    [],
                ):
                    correction_rule_counts[
                        correction.get(
                            "rule_code",
                            "UNKNOWN_RULE",
                        )
                    ] += 1

                for code in set(
                    result.get(
                        "codes_error",
                        [],
                    )
                ):
                    error_code_counts[
                        code
                    ] += 1

                # -----------------------------------------------
                # VALID / CORRECTED
                # -----------------------------------------------

                if status in {
                    QUALITY_VALID,
                    QUALITY_CORRECTED,
                }:

                    if not order_id:
                        raise RuntimeError(
                            "Validated candidate has "
                            "no order_id."
                        )

                    existing = (
                        validated_state.get(
                            order_id
                        )
                    )

                    first_processed_at = (
                        existing.get(
                            "first_processed_at"
                        )
                        if existing
                        else None
                    )

                    (
                        key,
                        new_fingerprint,
                        final_document,
                    ) = build_validated_document(
                        raw_document,
                        result,
                        raw_run_id,
                        processing_run_id,
                        first_processed_at,
                    )

                    old_fingerprint = (
                        existing.get(
                            "fingerprint"
                        )
                        if existing
                        else None
                    )

                    if (
                        existing
                        and old_fingerprint
                        == new_fingerprint
                    ):
                        validated_write_counts[
                            "unchanged"
                        ] += 1

                    else:

                        if existing:
                            validated_write_counts[
                                "updated"
                            ] += 1

                        else:
                            validated_write_counts[
                                "inserted"
                            ] += 1

                        validated_operations.append(
                            ReplaceOne(
                                {
                                    "order_id": key
                                },
                                final_document,
                                upsert=True,
                            )
                        )

                        validated_state[key] = {
                            "fingerprint": (
                                new_fingerprint
                            ),
                            "first_processed_at": (
                                final_document[
                                    "first_processed_at"
                                ]
                            ),
                        }

                # -----------------------------------------------
                # QUARANTINE
                # -----------------------------------------------

                elif status == QUALITY_QUARANTINED:

                    source_row_number = (
                        raw_document.get(
                            "source_row_number"
                        )
                    )

                    provisional_key = (
                        f"{raw_run_id}:"
                        f"{source_row_number}"
                        if source_row_number
                        is not None
                        else
                        f"{raw_run_id}:"
                        f"{raw_document['_id']}"
                    )

                    existing = (
                        quarantine_state.get(
                            provisional_key
                        )
                    )

                    first_processed_at = (
                        existing.get(
                            "first_processed_at"
                        )
                        if existing
                        else None
                    )

                    (
                        key,
                        new_fingerprint,
                        quarantine_document,
                    ) = build_quarantine_document(
                        raw_document,
                        result,
                        raw_run_id,
                        processing_run_id,
                        first_processed_at,
                    )

                    old_fingerprint = (
                        existing.get(
                            "fingerprint"
                        )
                        if existing
                        else None
                    )

                    if (
                        existing
                        and old_fingerprint
                        == new_fingerprint
                    ):
                        quarantine_write_counts[
                            "unchanged"
                        ] += 1

                    else:

                        if existing:
                            quarantine_write_counts[
                                "updated"
                            ] += 1

                        else:
                            quarantine_write_counts[
                                "inserted"
                            ] += 1

                        quarantine_operations.append(
                            ReplaceOne(
                                {
                                    "quarantine_key": key
                                },
                                quarantine_document,
                                upsert=True,
                            )
                        )

                        quarantine_state[key] = {
                            "fingerprint": (
                                new_fingerprint
                            ),
                            "first_processed_at": (
                                quarantine_document[
                                    "first_processed_at"
                                ]
                            ),
                        }

                else:
                    raise RuntimeError(
                        f"Unexpected quality status: "
                        f"{status!r}"
                    )

                # -----------------------------------------------
                # FLUSH VALIDATED
                # -----------------------------------------------

                if (
                    len(
                        validated_operations
                    )
                    >= WRITE_BATCH_SIZE
                ):
                    summary = flush_operations(
                        validated_collection,
                        validated_operations,
                    )

                    mongo_actual[
                        "validated_upserted"
                    ] += summary[
                        "upserted"
                    ]

                    mongo_actual[
                        "validated_modified"
                    ] += summary[
                        "modified"
                    ]

                # -----------------------------------------------
                # FLUSH QUARANTINE
                # -----------------------------------------------

                if (
                    len(
                        quarantine_operations
                    )
                    >= WRITE_BATCH_SIZE
                ):
                    summary = flush_operations(
                        quarantine_collection,
                        quarantine_operations,
                    )

                    mongo_actual[
                        "quarantine_upserted"
                    ] += summary[
                        "upserted"
                    ]

                    mongo_actual[
                        "quarantine_modified"
                    ] += summary[
                        "modified"
                    ]

                if (
                    scanned % 10000 == 0
                    or scanned == raw_count
                ):
                    elapsed_now = (
                        time.perf_counter()
                        - started
                    )

                    speed = (
                        scanned / elapsed_now
                        if elapsed_now > 0
                        else 0
                    )

                    print(
                        f"  Progress: "
                        f"{scanned:,}/"
                        f"{raw_count:,} "
                        f"| "
                        f"{speed:,.2f} "
                        f"records/sec"
                    )

        # Flush remaining writes.
        summary = flush_operations(
            validated_collection,
            validated_operations,
        )

        mongo_actual[
            "validated_upserted"
        ] += summary[
            "upserted"
        ]

        mongo_actual[
            "validated_modified"
        ] += summary[
            "modified"
        ]

        summary = flush_operations(
            quarantine_collection,
            quarantine_operations,
        )

        mongo_actual[
            "quarantine_upserted"
        ] += summary[
            "upserted"
        ]

        mongo_actual[
            "quarantine_modified"
        ] += summary[
            "modified"
        ]

        elapsed_seconds = (
            time.perf_counter()
            - started
        )

        throughput = (
            scanned / elapsed_seconds
            if elapsed_seconds > 0
            else 0
        )

        # ====================================================
        # FINAL CONSISTENCY GATES
        # ====================================================

        print(
            "\n[6/6] Running final consistency gates..."
        )

        valid_count = status_counts[
            QUALITY_VALID
        ]

        corrected_count = status_counts[
            QUALITY_CORRECTED
        ]

        quarantine_count = status_counts[
            QUALITY_QUARANTINED
        ]

        classified_total = (
            valid_count
            + corrected_count
            + quarantine_count
        )

        if classified_total != raw_count:
            raise RuntimeError(
                "Classification equation failed: "
                f"{raw_count} != "
                f"{valid_count} + "
                f"{corrected_count} + "
                f"{quarantine_count}"
            )

        # Compare with the approved 100K Dry Run only
        # when that contract applies.
        if dry_classification is not None:

            expected_valid = int(
                dry_classification[
                    "valid_count"
                ]
            )

            expected_corrected = int(
                dry_classification[
                    "corrected_count"
                ]
            )

            expected_quarantine = int(
                dry_classification[
                    "quarantine_count"
                ]
            )

            if (
                valid_count
                != expected_valid
                or corrected_count
                != expected_corrected
                or quarantine_count
                != expected_quarantine
            ):
                raise RuntimeError(
                    "Classification changed since "
                    "the approved Dry Run."
                )

        expected_validated = (
            valid_count
            + corrected_count
        )

        validated_after = (
            validated_collection.count_documents(
                {}
            )
        )

        quarantine_current_run = (
            quarantine_collection.count_documents(
                {
                    "source_run_id": (
                        raw_run_id
                    )
                }
            )
        )

        if (
            validated_after
            != expected_validated
        ):
            raise RuntimeError(
                "Validated count mismatch: "
                f"expected="
                f"{expected_validated}, "
                f"actual="
                f"{validated_after}"
            )

        if (
            quarantine_current_run
            != quarantine_count
        ):
            raise RuntimeError(
                "Quarantine count mismatch: "
                f"expected="
                f"{quarantine_count}, "
                f"actual="
                f"{quarantine_current_run}"
            )

        # Logical insert counts should equal Mongo upserts.
        if (
            validated_write_counts[
                "inserted"
            ]
            != mongo_actual[
                "validated_upserted"
            ]
        ):
            raise RuntimeError(
                "Validated inserted/upserted "
                "count mismatch."
            )

        if (
            quarantine_write_counts[
                "inserted"
            ]
            != mongo_actual[
                "quarantine_upserted"
            ]
        ):
            raise RuntimeError(
                "Quarantine inserted/upserted "
                "count mismatch."
            )

        # ====================================================
        # REPORT
        # ====================================================

        report = {
            "phase": "elt_final_write",

            "raw_run_id": raw_run_id,

            "processing_run_id": (
                processing_run_id
            ),

            "raw_count": raw_count,

            "classification": {
                "valid_count": (
                    valid_count
                ),

                "corrected_count": (
                    corrected_count
                ),

                "quarantine_count": (
                    quarantine_count
                ),

                "classified_total": (
                    classified_total
                ),
            },

            "validated_write": {
                "inserted_count": (
                    validated_write_counts[
                        "inserted"
                    ]
                ),

                "updated_count": (
                    validated_write_counts[
                        "updated"
                    ]
                ),

                "unchanged_count": (
                    validated_write_counts[
                        "unchanged"
                    ]
                ),
            },

            "quarantine_write": {
                "inserted_count": (
                    quarantine_write_counts[
                        "inserted"
                    ]
                ),

                "updated_count": (
                    quarantine_write_counts[
                        "updated"
                    ]
                ),

                "unchanged_count": (
                    quarantine_write_counts[
                        "unchanged"
                    ]
                ),
            },

            "collection_counts": {
                "orders_validated": (
                    validated_after
                ),

                "orders_quarantine_current_run": (
                    quarantine_current_run
                ),
            },

            "duplicates": {
                "group_count": len(
                    duplicate_ids
                ),

                "record_count": (
                    duplicate_record_count
                ),
            },

            "error_code_counts": dict(
                error_code_counts.most_common()
            ),

            "correction_rule_counts": dict(
                correction_rule_counts.most_common()
            ),

            "performance": {
                "elapsed_seconds": round(
                    elapsed_seconds,
                    4,
                ),

                "throughput_records_per_second": (
                    round(
                        throughput,
                        2,
                    )
                ),
            },

            "consistency": {
                "raw_equals_classified": (
                    raw_count
                    == classified_total
                ),

                "matches_dry_run": (
                    True
                    if dry_classification
                    is not None
                    else None
                ),

                "dry_run_contract_used": (
                    dry_classification
                    is not None
                ),

                "raw_run_selection": (
                    raw_run_selection
                ),

                "validated_count_correct": (
                    validated_after
                    == expected_validated
                ),

                "quarantine_count_correct": (
                    quarantine_current_run
                    == quarantine_count
                ),
            },
        }

        output_path = (
            REPORTS_DIR
            / "elt_write_report.json"
        )

        with output_path.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                report,
                file,
                ensure_ascii=False,
                indent=2,
                default=str,
            )

        # ====================================================
        # PRINT FINAL SUMMARY
        # ====================================================

        print("\n" + "=" * 92)
        print(
            "PHASE 12 ELT WRITE SUMMARY"
        )
        print("=" * 92)

        print(
            f"Raw                    : "
            f"{raw_count:>10,}"
        )

        print(
            f"Valid                  : "
            f"{valid_count:>10,}"
        )

        print(
            f"Corrected              : "
            f"{corrected_count:>10,}"
        )

        print(
            f"Quarantined            : "
            f"{quarantine_count:>10,}"
        )

        print(
            f"Classified total       : "
            f"{classified_total:>10,}"
        )

        print(
            "\nVALIDATED WRITE:"
        )

        print(
            f"  Inserted             : "
            f"{validated_write_counts['inserted']:>10,}"
        )

        print(
            f"  Updated              : "
            f"{validated_write_counts['updated']:>10,}"
        )

        print(
            f"  Unchanged            : "
            f"{validated_write_counts['unchanged']:>10,}"
        )

        print(
            "\nQUARANTINE WRITE:"
        )

        print(
            f"  Inserted             : "
            f"{quarantine_write_counts['inserted']:>10,}"
        )

        print(
            f"  Updated              : "
            f"{quarantine_write_counts['updated']:>10,}"
        )

        print(
            f"  Unchanged            : "
            f"{quarantine_write_counts['unchanged']:>10,}"
        )

        print(
            "\nFINAL COLLECTIONS:"
        )

        print(
            f"  orders_validated     : "
            f"{validated_after:>10,}"
        )

        print(
            f"  orders_quarantine    : "
            f"{quarantine_current_run:>10,}"
        )

        print(
            "\nCONSISTENCY:"
        )

        print(
            f"  {raw_count:,} = "
            f"{valid_count:,} + "
            f"{corrected_count:,} + "
            f"{quarantine_count:,}"
        )

        print(
            "  Raw classification equation : PASS"
        )

        if dry_classification is not None:

            print(
                "  Matches approved Dry Run     : PASS"
            )

        else:

            print(
                "  Dry Run comparison           : "
                "N/A - official large run"
            )

        print(
            "  Unique order_id index        : PASS"
        )

        print(
            "\nPERFORMANCE:"
        )

        print(
            f"  Elapsed               : "
            f"{elapsed_seconds:.2f} sec"
        )

        print(
            f"  Throughput            : "
            f"{throughput:,.2f} "
            f"records/sec"
        )

        print(
            "\nReport:"
        )

        print(output_path)

        print("\n" + "=" * 92)
        print(
            "PHASE 12 ELT FINAL WRITE: PASS"
        )
        print("=" * 92)

    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    main()
