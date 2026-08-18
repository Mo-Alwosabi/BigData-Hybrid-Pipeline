import copy
import json
from datetime import datetime, timezone
from pathlib import Path

from pymongo import ASCENDING, ReplaceOne

from config.settings import (
    VALIDATED_COLLECTION,
    REPORTS_DIR,
)

from src.mongo_setup import (
    create_mongo_client,
    get_database,
)


TEST_COLLECTION = "_upsert_update_proof_temp"


def now_utc():
    return datetime.now(
        timezone.utc
    )


def main():
    client = None

    try:
        client = create_mongo_client()
        db = get_database(client)

        validated = db[
            VALIDATED_COLLECTION
        ]

        test_collection = db[
            TEST_COLLECTION
        ]

        print("=" * 88)
        print("PHASE 13B - UPSERT UPDATE PROOF")
        print("=" * 88)

        # ====================================================
        # 1. GET ONE REAL VALIDATED RECORD
        # ====================================================

        source = validated.find_one(
            {
                "order_id": {
                    "$exists": True,
                    "$ne": None,
                }
            }
        )

        if not source:
            raise RuntimeError(
                "orders_validated has no usable record."
            )

        original_order_id = source[
            "order_id"
        ]

        print(
            f"Source order_id          : "
            f"{original_order_id}"
        )

        # ====================================================
        # 2. RESET ISOLATED TEST COLLECTION
        # ====================================================

        test_collection.drop()

        test_collection = db[
            TEST_COLLECTION
        ]

        test_collection.create_index(
            [
                (
                    "order_id",
                    ASCENDING,
                )
            ],
            unique=True,
            name="uq_test_order_id",
        )

        print(
            "Temporary collection     : "
            f"{TEST_COLLECTION}"
        )

        print(
            "Unique order_id index    : PASS"
        )

        # ====================================================
        # 3. INSERT BASELINE COPY
        # ====================================================

        baseline = copy.deepcopy(
            source
        )

        baseline.pop(
            "_id",
            None,
        )

        # Test-only metadata.
        baseline[
            "_proof_test"
        ] = {
            "version": 1,
            "note": "baseline",
            "timestamp": now_utc(),
        }

        first_result = (
            test_collection.replace_one(
                {
                    "order_id": (
                        original_order_id
                    )
                },
                baseline,
                upsert=True,
            )
        )

        count_after_first = (
            test_collection.count_documents(
                {}
            )
        )

        print(
            "\nFIRST UPSERT:"
        )

        print(
            f"  Upserted              : "
            f"{(1 if first_result.upserted_id is not None else 0)}"
        )

        print(
            f"  Modified              : "
            f"{first_result.modified_count}"
        )

        print(
            f"  Collection count      : "
            f"{count_after_first}"
        )

        # ====================================================
        # 4. CHANGE SAME BUSINESS RECORD
        # ====================================================

        changed = copy.deepcopy(
            baseline
        )

        # Controlled change only inside temporary copy.
        original_customer_name = (
            changed.get(
                "customer_name"
            )
        )

        changed[
            "customer_name"
        ] = (
            f"{original_customer_name} "
            f"[UPSERT-PROOF]"
        )

        changed[
            "_proof_test"
        ] = {
            "version": 2,
            "note": (
                "same business key, "
                "changed payload"
            ),
            "timestamp": now_utc(),
        }

        second_result = (
            test_collection.replace_one(
                {
                    "order_id": (
                        original_order_id
                    )
                },
                changed,
                upsert=True,
            )
        )

        count_after_second = (
            test_collection.count_documents(
                {}
            )
        )

        final_document = (
            test_collection.find_one(
                {
                    "order_id": (
                        original_order_id
                    )
                }
            )
        )

        print(
            "\nSECOND UPSERT - SAME order_id, "
            "CHANGED DATA:"
        )

        print(
            f"  Upserted              : "
            f"{(1 if second_result.upserted_id is not None else 0)}"
        )

        print(
            f"  Matched               : "
            f"{second_result.matched_count}"
        )

        print(
            f"  Modified              : "
            f"{second_result.modified_count}"
        )

        print(
            f"  Collection count      : "
            f"{count_after_second}"
        )

        # ====================================================
        # 5. ASSERTIONS
        # ====================================================

        first_inserted = (
            (1 if first_result.upserted_id is not None else 0)
            == 1
        )

        second_not_inserted = (
            (1 if second_result.upserted_id is not None else 0)
            == 0
        )

        second_matched = (
            second_result.matched_count
            == 1
        )

        second_updated = (
            second_result.modified_count
            == 1
        )

        count_stable = (
            count_after_first == 1
            and count_after_second == 1
        )

        same_business_key = (
            final_document[
                "order_id"
            ]
            == original_order_id
        )

        payload_changed = (
            final_document.get(
                "customer_name"
            )
            == changed[
                "customer_name"
            ]
        )

        passed = all(
            [
                first_inserted,
                second_not_inserted,
                second_matched,
                second_updated,
                count_stable,
                same_business_key,
                payload_changed,
            ]
        )

        if not passed:
            raise RuntimeError(
                "Upsert update proof failed."
            )

        # ====================================================
        # 6. SAVE EVIDENCE
        # ====================================================

        report = {
            "phase": (
                "phase_13b_upsert_update_proof"
            ),

            "mode": (
                "isolated_temporary_collection"
            ),

            "source_collection": (
                VALIDATED_COLLECTION
            ),

            "temporary_collection": (
                TEST_COLLECTION
            ),

            "business_key": (
                "order_id"
            ),

            "order_id": (
                original_order_id
            ),

            "first_upsert": {
                "upserted_count": (
                    (1 if first_result.upserted_id is not None else 0)
                ),

                "matched_count": (
                    first_result.matched_count
                ),

                "modified_count": (
                    first_result.modified_count
                ),

                "collection_count": (
                    count_after_first
                ),
            },

            "second_upsert_changed_payload": {
                "upserted_count": (
                    (1 if second_result.upserted_id is not None else 0)
                ),

                "matched_count": (
                    second_result.matched_count
                ),

                "modified_count": (
                    second_result.modified_count
                ),

                "collection_count": (
                    count_after_second
                ),
            },

            "proof": {
                "same_order_id": (
                    same_business_key
                ),

                "payload_updated": (
                    payload_changed
                ),

                "duplicate_created": False,

                "collection_count_stable": (
                    count_stable
                ),

                "result": "PASS",
            },

            "production_data_modified": False,
        }

        report_path = (
            REPORTS_DIR
            / "upsert_update_proof.json"
        )

        with report_path.open(
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
        # 7. CLEAN TEMPORARY COLLECTION
        # ====================================================

        test_collection.drop()

        temp_exists_after_cleanup = (
            TEST_COLLECTION
            in db.list_collection_names()
        )

        if temp_exists_after_cleanup:
            raise RuntimeError(
                "Temporary proof collection "
                "was not removed."
            )

        # ====================================================
        # FINAL SUMMARY
        # ====================================================

        print("\n" + "=" * 88)
        print("PHASE 13B UPSERT UPDATE SUMMARY")
        print("=" * 88)

        print(
            f"Business key            : "
            f"order_id"
        )

        print(
            f"Test order_id           : "
            f"{original_order_id}"
        )

        print(
            "\nBASELINE:"
        )

        print(
            "  First upsert inserted : 1"
        )

        print(
            "  Collection count      : 1"
        )

        print(
            "\nCHANGED SAME BUSINESS KEY:"
        )

        print(
            "  Inserted              : 0"
        )

        print(
            "  Matched               : 1"
        )

        print(
            "  Updated               : 1"
        )

        print(
            "  Collection count      : 1"
        )

        print(
            "\nPROOF:"
        )

        print(
            "  Same order_id         : PASS"
        )

        print(
            "  Existing row updated  : PASS"
        )

        print(
            "  Duplicate created     : NO"
        )

        print(
            "  Production modified   : NO"
        )

        print(
            "  Temporary data cleaned: PASS"
        )

        print(
            "\nReport:"
        )

        print(report_path)

        print("\n" + "=" * 88)
        print(
            "PHASE 13B UPSERT UPDATE PROOF: PASS"
        )
        print("=" * 88)

    finally:

        if client is not None:

            # Defensive cleanup if an exception occurs.
            try:
                db[
                    TEST_COLLECTION
                ].drop()
            except Exception:
                pass

            client.close()


if __name__ == "__main__":
    main()
