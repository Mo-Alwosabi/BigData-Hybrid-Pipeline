import json
import time
from collections import Counter
from datetime import datetime, timezone

from pymongo import DESCENDING

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


VALID_STATUSES = {
    QUALITY_VALID,
    QUALITY_CORRECTED,
    QUALITY_QUARANTINED,
}


def compact_valid_sample(row_number, raw, result):
    cleaned = result["cleaned_record"]

    return {
        "source_row_number": row_number,
        "order_id": cleaned.get("order_id"),
        "customer_id": cleaned.get("customer_id"),
        "status": cleaned.get("status"),
        "quality_status": result["quality_status"],
    }


def compact_corrected_sample(row_number, raw, result):
    cleaned = result["cleaned_record"]

    return {
        "source_row_number": row_number,
        "order_id": cleaned.get("order_id"),
        "quality_status": result["quality_status"],
        "correction_count": len(
            result["corrections"]
        ),
        "corrections": result["corrections"][:8],
    }


def compact_quarantine_sample(row_number, raw, result):
    return {
        "source_row_number": row_number,
        "order_id": raw.get("order_id"),
        "quality_status": result["quality_status"],
        "codes_error": result["codes_error"],
        "details_error": result["details_error"][:8],
    }


def find_conflicting_duplicate_ids(
    collection,
    run_id,
):
    """
    Find duplicate order_id groups in the same Raw run.

    The order_id is trimmed before grouping because our
    quality pipeline performs deterministic whitespace trim.

    READ ONLY aggregation.
    """

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
    duplicate_record_count = 0
    group_sizes = Counter()

    cursor = collection.aggregate(
        pipeline,
        allowDiskUse=True,
    )

    for item in cursor:
        duplicate_ids.add(
            item["_id"]
        )

        count = int(
            item["record_count"]
        )

        duplicate_record_count += count
        group_sizes[count] += 1

    return (
        duplicate_ids,
        duplicate_record_count,
        group_sizes,
    )


def main():
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
        # LATEST RAW RUN
        # ====================================================

        latest = raw_collection.find_one(
            {},
            sort=[
                (
                    "ingested_at",
                    DESCENDING,
                )
            ],
            projection={
                "run_id": 1,
            },
        )

        if not latest:
            raise RuntimeError(
                "orders_raw is empty."
            )

        run_id = latest["run_id"]

        raw_query = {
            "run_id": run_id
        }

        expected_raw_count = (
            raw_collection.count_documents(
                raw_query
            )
        )

        # ====================================================
        # PROVE DRY RUN DOES NOT WRITE FINAL COLLECTIONS
        # ====================================================

        validated_before = (
            validated_collection.count_documents(
                {}
            )
        )

        quarantine_before = (
            quarantine_collection.count_documents(
                {}
            )
        )

        print("=" * 88)
        print("PHASE 11 - CLASSIFICATION DRY RUN")
        print("=" * 88)

        print(
            f"run_id                  : {run_id}"
        )

        print(
            f"raw documents           : "
            f"{expected_raw_count:,}"
        )

        print(
            f"validated before        : "
            f"{validated_before:,}"
        )

        print(
            f"quarantine before       : "
            f"{quarantine_before:,}"
        )

        print(
            "MongoDB write mode       : DISABLED"
        )

        print("=" * 88)

        # ====================================================
        # DUPLICATES
        # ====================================================

        print(
            "\n[1/3] Detecting duplicate order_id groups..."
        )

        (
            duplicate_ids,
            duplicate_record_count,
            duplicate_group_sizes,
        ) = find_conflicting_duplicate_ids(
            raw_collection,
            run_id,
        )

        duplicate_group_count = len(
            duplicate_ids
        )

        print(
            f"Duplicate groups         : "
            f"{duplicate_group_count:,}"
        )

        print(
            f"Records inside groups    : "
            f"{duplicate_record_count:,}"
        )

        if duplicate_group_sizes:
            print(
                "Duplicate group sizes     : "
                + ", ".join(
                    f"{size} records × "
                    f"{groups} groups"
                    for size, groups
                    in sorted(
                        duplicate_group_sizes.items()
                    )
                )
            )

        # ====================================================
        # CLASSIFICATION
        # ====================================================

        print(
            "\n[2/3] Classifying Raw records..."
        )

        status_counts = Counter()
        error_code_counts = Counter()
        correction_rule_counts = Counter()
        error_combination_counts = Counter()

        corrections_per_record = Counter()
        errors_per_record = Counter()

        samples = {
            "valid": [],
            "corrected": [],
            "quarantined": [],
        }

        scanned = 0
        duplicate_records_seen = 0

        started = time.perf_counter()

        cursor = raw_collection.find(
            raw_query,
            projection={
                "_id": 0,
                "source_row_number": 1,
                "raw_record": 1,
            },
        ).batch_size(2000)

        for document in cursor:
            scanned += 1

            row_number = document.get(
                "source_row_number"
            )

            raw = document.get(
                "raw_record"
            )

            if not isinstance(raw, dict):
                raise RuntimeError(
                    "Invalid raw_record structure "
                    f"at source row {row_number}."
                )

            order_id_value = raw.get(
                "order_id"
            )

            normalized_order_id = (
                str(order_id_value).strip()
                if order_id_value is not None
                else ""
            )

            duplicate_conflict = (
                normalized_order_id
                in duplicate_ids
            )

            if duplicate_conflict:
                duplicate_records_seen += 1

            try:
                result = classify_record(
                    raw,
                    duplicate_conflict=(
                        duplicate_conflict
                    ),
                )

            except Exception as exc:
                raise RuntimeError(
                    "Classification crashed at "
                    f"source_row_number="
                    f"{row_number}, "
                    f"order_id="
                    f"{order_id_value!r}"
                ) from exc

            quality_status = result.get(
                "quality_status"
            )

            if quality_status not in VALID_STATUSES:
                raise RuntimeError(
                    "Unexpected quality_status "
                    f"{quality_status!r} "
                    f"at row {row_number}."
                )

            # Exactly one classification per record.
            status_counts[
                quality_status
            ] += 1

            # -----------------------------------------------
            # Corrections
            # -----------------------------------------------

            corrections = result.get(
                "corrections",
                [],
            )

            corrections_per_record[
                len(corrections)
            ] += 1

            for correction in corrections:
                rule_code = correction.get(
                    "rule_code",
                    "UNKNOWN_RULE",
                )

                correction_rule_counts[
                    rule_code
                ] += 1

            # -----------------------------------------------
            # Errors
            # -----------------------------------------------

            codes_error = result.get(
                "codes_error",
                [],
            )

            unique_error_codes = sorted(
                set(codes_error)
            )

            errors_per_record[
                len(unique_error_codes)
            ] += 1

            for code in unique_error_codes:
                error_code_counts[
                    code
                ] += 1

            if unique_error_codes:
                combination_key = " + ".join(
                    unique_error_codes
                )

                error_combination_counts[
                    combination_key
                ] += 1

            # -----------------------------------------------
            # Samples
            # -----------------------------------------------

            if (
                quality_status
                == QUALITY_VALID
                and len(
                    samples["valid"]
                ) < 5
            ):
                samples[
                    "valid"
                ].append(
                    compact_valid_sample(
                        row_number,
                        raw,
                        result,
                    )
                )

            elif (
                quality_status
                == QUALITY_CORRECTED
                and len(
                    samples["corrected"]
                ) < 5
            ):
                samples[
                    "corrected"
                ].append(
                    compact_corrected_sample(
                        row_number,
                        raw,
                        result,
                    )
                )

            elif (
                quality_status
                == QUALITY_QUARANTINED
                and len(
                    samples["quarantined"]
                ) < 5
            ):
                samples[
                    "quarantined"
                ].append(
                    compact_quarantine_sample(
                        row_number,
                        raw,
                        result,
                    )
                )

            if (
                scanned % 10000 == 0
                or scanned == expected_raw_count
            ):
                elapsed_now = (
                    time.perf_counter()
                    - started
                )

                throughput_now = (
                    scanned / elapsed_now
                    if elapsed_now > 0
                    else 0
                )

                print(
                    f"  Progress: "
                    f"{scanned:,}/"
                    f"{expected_raw_count:,} "
                    f"| "
                    f"{throughput_now:,.2f} "
                    f"records/sec"
                )

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
        # CONSISTENCY GATES
        # ====================================================

        print(
            "\n[3/3] Running consistency gates..."
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

        if scanned != expected_raw_count:
            raise RuntimeError(
                "RAW SCAN CONSISTENCY FAILED: "
                f"expected={expected_raw_count}, "
                f"scanned={scanned}"
            )

        if classified_total != scanned:
            raise RuntimeError(
                "CLASSIFICATION CONSISTENCY FAILED: "
                f"{scanned} != "
                f"{valid_count} + "
                f"{corrected_count} + "
                f"{quarantine_count}"
            )

        if (
            duplicate_records_seen
            != duplicate_record_count
        ):
            raise RuntimeError(
                "DUPLICATE CONSISTENCY FAILED: "
                f"aggregation="
                f"{duplicate_record_count}, "
                f"classification="
                f"{duplicate_records_seen}"
            )

        # ----------------------------------------------------
        # Confirm MongoDB final collections were untouched.
        # ----------------------------------------------------

        validated_after = (
            validated_collection.count_documents(
                {}
            )
        )

        quarantine_after = (
            quarantine_collection.count_documents(
                {}
            )
        )

        if (
            validated_after
            != validated_before
        ):
            raise RuntimeError(
                "DRY RUN VIOLATION: "
                "orders_validated changed."
            )

        if (
            quarantine_after
            != quarantine_before
        ):
            raise RuntimeError(
                "DRY RUN VIOLATION: "
                "orders_quarantine changed."
            )

        # ====================================================
        # REPORT
        # ====================================================

        report = {
            "phase": (
                "classification_dry_run"
            ),
            "mode": "read_only",

            "generated_at": (
                datetime.now(
                    timezone.utc
                ).isoformat()
            ),

            "run_id": run_id,

            "raw_count": (
                expected_raw_count
            ),

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

            "consistency": {
                "raw_equals_classified": (
                    expected_raw_count
                    == classified_total
                ),
                "duplicate_records_expected": (
                    duplicate_record_count
                ),
                "duplicate_records_seen": (
                    duplicate_records_seen
                ),
                "validated_unchanged": (
                    validated_before
                    == validated_after
                ),
                "quarantine_unchanged": (
                    quarantine_before
                    == quarantine_after
                ),
            },

            "duplicates": {
                "group_count": (
                    duplicate_group_count
                ),
                "record_count": (
                    duplicate_record_count
                ),
                "group_size_distribution": {
                    str(key): value
                    for key, value
                    in sorted(
                        duplicate_group_sizes.items()
                    )
                },
            },

            "error_code_counts": dict(
                error_code_counts.most_common()
            ),

            "correction_rule_counts": dict(
                correction_rule_counts.most_common()
            ),

            "error_combination_counts": dict(
                error_combination_counts.most_common()
            ),

            "corrections_per_record": {
                str(key): value
                for key, value
                in sorted(
                    corrections_per_record.items()
                )
            },

            "errors_per_record": {
                str(key): value
                for key, value
                in sorted(
                    errors_per_record.items()
                )
            },

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

            "samples": samples,

            "mongo_collection_counts": {
                "validated_before": (
                    validated_before
                ),
                "validated_after": (
                    validated_after
                ),
                "quarantine_before": (
                    quarantine_before
                ),
                "quarantine_after": (
                    quarantine_after
                ),
            },
        }

        output_path = (
            REPORTS_DIR
            / "classification_dry_run.json"
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
        # TERMINAL SUMMARY
        # ====================================================

        print("\n" + "=" * 88)
        print("CLASSIFICATION DRY RUN SUMMARY")
        print("=" * 88)

        print(
            f"Raw records             : "
            f"{expected_raw_count:>10,}"
        )

        print(
            f"Valid                   : "
            f"{valid_count:>10,}"
        )

        print(
            f"Corrected               : "
            f"{corrected_count:>10,}"
        )

        print(
            f"Quarantined             : "
            f"{quarantine_count:>10,}"
        )

        print(
            f"Classified total        : "
            f"{classified_total:>10,}"
        )

        print(
            "\nCONSISTENCY:"
        )

        print(
            f"  {expected_raw_count:,} "
            f"= "
            f"{valid_count:,} "
            f"+ "
            f"{corrected_count:,} "
            f"+ "
            f"{quarantine_count:,}"
        )

        print(
            "  RAW = VALID + CORRECTED "
            "+ QUARANTINED : PASS"
        )

        print(
            f"\nDuplicate groups        : "
            f"{duplicate_group_count:,}"
        )

        print(
            f"Duplicate records       : "
            f"{duplicate_record_count:,}"
        )

        print(
            "\nTOP QUARANTINE ERROR CODES:"
        )

        if error_code_counts:
            for code, count in (
                error_code_counts.most_common(
                    15
                )
            ):
                print(
                    f"  {code:42} "
                    f"{count:>10,}"
                )
        else:
            print("  None")

        print(
            "\nTOP CORRECTION RULES:"
        )

        if correction_rule_counts:
            for code, count in (
                correction_rule_counts.most_common(
                    20
                )
            ):
                print(
                    f"  {code:42} "
                    f"{count:>10,}"
                )
        else:
            print("  None")

        print(
            "\nTOP ERROR COMBINATIONS:"
        )

        if error_combination_counts:
            for combination, count in (
                error_combination_counts.most_common(
                    10
                )
            ):
                print(
                    f"  {count:>8,}  "
                    f"{combination}"
                )
        else:
            print("  None")

        print(
            "\nPERFORMANCE:"
        )

        print(
            f"  Elapsed                : "
            f"{elapsed_seconds:.2f} sec"
        )

        print(
            f"  Throughput             : "
            f"{throughput:,.2f} "
            f"records/sec"
        )

        print(
            "\nREAD-ONLY PROOF:"
        )

        print(
            f"  validated: "
            f"{validated_before:,} "
            f"→ "
            f"{validated_after:,}"
        )

        print(
            f"  quarantine: "
            f"{quarantine_before:,} "
            f"→ "
            f"{quarantine_after:,}"
        )

        print(
            "\nReport:"
        )

        print(output_path)

        print("\n" + "=" * 88)
        print(
            "PHASE 11 CLASSIFICATION DRY RUN: PASS"
        )
        print("=" * 88)

    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    main()
