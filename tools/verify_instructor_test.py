from collections import Counter
from pathlib import Path
import json

from src.mongo_setup import create_mongo_client, get_database


DB_NAME = "bigdata_student_test"


EXPECTED = {
    "raw": 20000,
    "valid": 12000,
    "corrected": 5000,
    "validated": 17000,
    "quarantine": 3000,
}

EXPECTED_QUARANTINE = {
    "MISSING_ORDER_ID": 250,
    "MISSING_CUSTOMER_ID": 250,
    "PHONE_INVALID_UNRECOVERABLE": 250,
    "EMAIL_INVALID_UNRECOVERABLE": 250,
    "INVALID_IMPOSSIBLE_DATE": 250,
    "STATUS_UNKNOWN": 250,
    "EMPTY_ITEMS": 250,
    "CORRUPTED_ITEMS_JSON": 250,
    "MISSING_ITEM_SKU": 250,
    "AMBIGUOUS_NEGATIVE_VALUE": 250,
    "CURRENCY_UNKNOWN": 250,
    "MULTIPLE_CONFLICTING_ERRORS": 250,
}

EXPECTED_CORRECTIONS = {
    "delivery_cost_arabic_digits": 500,
    "payment_amount_arabic_digits": 500,
    "total_amount_thousands": 500,
    "email_repeated_symbols": 500,
    "phone_country_code": 500,
    "date_normalized": 500,
    "currency_normalized": 500,
    "status_whitespace": 500,
    "qty_as_string": 500,
    "total_recalculated": 500,
}


def status_counts(collection):
    result = {}
    for doc in collection.aggregate([
        {"$group": {"_id": "$quality_status", "count": {"$sum": 1}}},
    ]):
        result[doc["_id"]] = doc["count"]
    return result


def quarantine_counts(collection):
    counts = Counter()

    for doc in collection.find({}, {"error_codes": 1}):
        codes = doc.get("error_codes", [])
        if not isinstance(codes, list):
            codes = [codes]

        # A MULTIPLE_CONFLICTING_ERRORS record is one official
        # quarantine category, even when detailed secondary codes
        # are also retained in the document.
        if "MULTIPLE_CONFLICTING_ERRORS" in codes:
            counts["MULTIPLE_CONFLICTING_ERRORS"] += 1
        else:
            for code in codes:
                counts[code] += 1

    return dict(counts)


def correction_counts(collection):
    counts = Counter()

    rule_map = {
        "currency_normalized": {
            ("currency", "CURRENCY_NORMALIZED_YER"),
        },
        "email_repeated_symbols": {
            ("customer_email", "EMAIL_REPEATED_SYMBOLS"),
        },
        "phone_country_code": {
            ("customer_phone", "PHONE_NORMALIZED_YE"),
        },
        "date_normalized": {
            ("order_date", "DATE_NORMALIZED"),
        },
        "status_whitespace": {
            ("status", "WHITESPACE_TRIMMED"),
        },
        "qty_as_string": {
            ("items_json[0].qty", "QTY_AS_STRING_IN_ITEMS"),
        },
        "total_recalculated": {
            ("total_amount", "ORDER_TOTAL_RECALCULATED"),
        },
        "total_amount_thousands": {
            ("total_amount", "THOUSANDS_SEPARATOR_NORMALIZED"),
        },
    }

    for doc in collection.find(
        {"quality_status": "corrected"},
        {"corrections": 1},
    ):
        seen = set()

        for correction in doc.get("corrections", []):
            field = correction.get("field")
            rule = correction.get("rule_code")

            if field == "delivery_cost" and rule in {
                "ARABIC_DIGITS_TO_LATIN",
                "ARABIC_DECIMAL_SEPARATOR_NORMALIZED",
            }:
                seen.add("delivery_cost_arabic_digits")

            if field == "payment_amount" and rule in {
                "ARABIC_DIGITS_TO_LATIN",
                "ARABIC_DECIMAL_SEPARATOR_NORMALIZED",
            }:
                seen.add("payment_amount_arabic_digits")

            for category, signatures in rule_map.items():
                if (field, rule) in signatures:
                    seen.add(category)

        for category in seen:
            counts[category] += 1

    return dict(counts)


def check(label, actual, expected):
    ok = actual == expected
    print(
        f"{'PASS' if ok else 'FAIL'}  "
        f"{label}: actual={actual} expected={expected}"
    )
    return ok


def main():
    client = create_mongo_client()
    db = get_database(client)

    raw = db["orders_raw"]
    validated = db["orders_validated"]
    quarantine = db["orders_quarantine"]

    status = status_counts(validated)
    q_counts = quarantine_counts(quarantine)
    c_counts = correction_counts(validated)

    checks = []

    print("=" * 72)
    print("OFFICIAL INSTRUCTOR SMALL-TEST VERIFICATION")
    print("=" * 72)

    checks.append(check(
        "orders_raw",
        raw.count_documents({}),
        EXPECTED["raw"],
    ))

    checks.append(check(
        "valid",
        status.get("valid", 0),
        EXPECTED["valid"],
    ))

    checks.append(check(
        "corrected",
        status.get("corrected", 0),
        EXPECTED["corrected"],
    ))

    checks.append(check(
        "orders_validated",
        validated.count_documents({}),
        EXPECTED["validated"],
    ))

    checks.append(check(
        "orders_quarantine",
        quarantine.count_documents({}),
        EXPECTED["quarantine"],
    ))

    print("\n=== QUARANTINE CATEGORIES ===")
    for code, expected in EXPECTED_QUARANTINE.items():
        checks.append(check(
            code,
            q_counts.get(code, 0),
            expected,
        ))

    print("\n=== CORRECTION CATEGORIES ===")
    for category, expected in EXPECTED_CORRECTIONS.items():
        checks.append(check(
            category,
            c_counts.get(category, 0),
            expected,
        ))

    print("\n" + "=" * 72)

    if all(checks):
        print("RESULT: OFFICIAL INSTRUCTOR TEST = PASS")
    else:
        print("RESULT: OFFICIAL INSTRUCTOR TEST = FAIL")

    print("=" * 72)

    client.close()


if __name__ == "__main__":
    main()
