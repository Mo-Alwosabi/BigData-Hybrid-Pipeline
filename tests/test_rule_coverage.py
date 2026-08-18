import json

from tests.test_cleaning_rules import base_record

from src.quality_rules import (
    classify_record,

    QUALITY_CORRECTED,
    QUALITY_QUARANTINED,

    RULE_ARABIC_DECIMAL,
    RULE_THOUSANDS,
    RULE_CURRENCY_SUFFIX,
    RULE_CURRENCY_YER,
    RULE_WHITESPACE,
    RULE_STATUS_SYNONYM,
    RULE_EMAIL,
    RULE_DATE,
    RULE_ITEM_TOTAL,
    RULE_ITEM_PRICE_DIRECT,
    RULE_ORDER_TOTAL_DERIVED,

    ERR_PHONE_INVALID,
    ERR_ITEMS_EMPTY,
    ERR_ID_ORDER_MISSING,
    ERR_ID_CUSTOMER_MISSING,
    ERR_STATUS_UNKNOWN,
    ERR_VALUE_NEGATIVE_AMBIGUOUS,
    ERR_ITEM_COMPONENTS_CONFLICT,
    ERR_PRICE_UNKNOWN,
)


def rule_codes(result):
    return {
        correction["rule_code"]
        for correction in result["corrections"]
    }


# ============================================================
# NUMERIC FORMATTING
# ============================================================

def test_western_thousands_separator():
    record = base_record()

    record["delivery_cost"] = "2,000"

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_CORRECTED

    assert (
        result["cleaned_record"]["delivery_cost"]
        == 2000
    )

    assert RULE_THOUSANDS in rule_codes(result)


def test_arabic_decimal_separator():
    record = base_record()

    record["payment_amount"] = "١٢٠٠٠٫٠"

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_CORRECTED

    assert (
        result["cleaned_record"]["payment_amount"]
        == 12000
    )

    assert RULE_ARABIC_DECIMAL in rule_codes(result)


def test_currency_suffix_in_numeric_field():
    record = base_record()

    record["delivery_cost"] = "2000 ريال"

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_CORRECTED

    assert (
        result["cleaned_record"]["delivery_cost"]
        == 2000
    )

    assert RULE_CURRENCY_SUFFIX in rule_codes(result)


# ============================================================
# CURRENCY
# ============================================================

def test_arabic_currency_normalization():
    record = base_record()

    record["currency"] = "ريال يمني"

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_CORRECTED

    assert (
        result["cleaned_record"]["currency"]
        == "YER"
    )

    assert RULE_CURRENCY_YER in rule_codes(result)


# ============================================================
# WHITESPACE / STATUS
# ============================================================

def test_whitespace_trim():
    record = base_record()

    record["status"] = "  مؤكد  "

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_CORRECTED

    assert (
        result["cleaned_record"]["status"]
        == "مؤكد"
    )

    assert RULE_WHITESPACE in rule_codes(result)


def test_payment_status_synonym():
    record = base_record()

    record["payment_status"] = "مدفوع"

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_CORRECTED

    assert (
        result["cleaned_record"]["payment_status"]
        == "تم الدفع"
    )

    assert RULE_STATUS_SYNONYM in rule_codes(result)


# ============================================================
# EMAIL
# ============================================================

def test_repeated_dot_email():
    record = base_record()

    record["customer_email"] = "user@mail..com"

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_CORRECTED

    assert (
        result["cleaned_record"]["customer_email"]
        == "user@mail.com"
    )

    assert RULE_EMAIL in rule_codes(result)


# ============================================================
# DATE
# ============================================================

def test_ymd_slash_date():
    record = base_record()

    record["order_date"] = "2025/04/11 13:41:00"

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_CORRECTED

    assert (
        result["cleaned_record"]["order_date"]
        == "2025-04-11T13:41:00"
    )

    assert RULE_DATE in rule_codes(result)


# ============================================================
# DIRECT ITEM DERIVATIONS
# ============================================================

def test_item_total_direct_derivation():
    record = base_record()

    record["items_json"] = json.dumps(
        [
            {
                "sku": "SKU-1",
                "name": "منتج",
                "qty": 2,
                "unit_price": 5000,
                "total": "غير معروف",
            }
        ],
        ensure_ascii=False,
    )

    result = classify_record(record)

    items = json.loads(
        result["cleaned_record"]["items_json"]
    )

    assert result["quality_status"] == QUALITY_CORRECTED

    assert items[0]["total"] == 10000

    assert RULE_ITEM_TOTAL in rule_codes(result)


def test_item_price_direct_derivation():
    record = base_record()

    record["items_json"] = json.dumps(
        [
            {
                "sku": "SKU-1",
                "name": "منتج",
                "qty": 2,
                "unit_price": "غير معروف",
                "total": 10000,
            }
        ],
        ensure_ascii=False,
    )

    result = classify_record(record)

    items = json.loads(
        result["cleaned_record"]["items_json"]
    )

    assert result["quality_status"] == QUALITY_CORRECTED

    assert items[0]["unit_price"] == 5000

    assert RULE_ITEM_PRICE_DIRECT in rule_codes(result)


# ============================================================
# ORDER TOTAL DERIVATION
# ============================================================

def test_unknown_order_total_safe_derivation():
    record = base_record()

    record["total_amount"] = "???"

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_CORRECTED

    assert (
        result["cleaned_record"]["total_amount"]
        == 12000
    )

    assert RULE_ORDER_TOTAL_DERIVED in rule_codes(result)


# ============================================================
# QUARANTINE: REQUIRED IDENTIFIERS
# ============================================================

def test_missing_order_id():
    record = base_record()

    record["order_id"] = ""

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_QUARANTINED

    assert ERR_ID_ORDER_MISSING in result["codes_error"]


def test_missing_customer_id():
    record = base_record()

    record["customer_id"] = ""

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_QUARANTINED

    assert ERR_ID_CUSTOMER_MISSING in result["codes_error"]


# ============================================================
# QUARANTINE: PHONE / STATUS / ITEMS
# ============================================================

def test_short_phone_quarantine():
    record = base_record()

    record["customer_phone"] = "12345"

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_QUARANTINED

    assert ERR_PHONE_INVALID in result["codes_error"]


def test_empty_items_quarantine():
    record = base_record()

    record["items_json"] = "[]"

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_QUARANTINED

    assert ERR_ITEMS_EMPTY in result["codes_error"]


def test_unknown_status_quarantine():
    record = base_record()

    record["status"] = "حالة غير معروفة تمامًا"

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_QUARANTINED

    assert ERR_STATUS_UNKNOWN in result["codes_error"]


# ============================================================
# QUARANTINE: AMBIGUOUS NEGATIVE
# ============================================================

def test_ambiguous_negative_qty_quarantine():
    record = base_record()

    record["items_json"] = json.dumps(
        [
            {
                "sku": "SKU-1",
                "name": "منتج",
                "qty": -2,
                "unit_price": 5000,
                "total": 14000,
            }
        ],
        ensure_ascii=False,
    )

    record["total_amount"] = "16000"
    record["payment_amount"] = "16000"

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_QUARANTINED

    assert (
        ERR_VALUE_NEGATIVE_AMBIGUOUS
        in result["codes_error"]
    )


# ============================================================
# QUARANTINE: COMPONENT CONFLICT
# ============================================================

def test_item_components_conflict_quarantine():
    record = base_record()

    record["items_json"] = json.dumps(
        [
            {
                "sku": "SKU-1",
                "name": "منتج",
                "qty": 2,
                "unit_price": 5000,
                "total": 9000,
            }
        ],
        ensure_ascii=False,
    )

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_QUARANTINED

    assert (
        ERR_ITEM_COMPONENTS_CONFLICT
        in result["codes_error"]
    )


# ============================================================
# QUARANTINE: UNKNOWN ITEM VALUE WITH NO SAFE DERIVATION
# ============================================================

def test_unknown_item_price_unrecoverable():
    record = base_record()

    record["items_json"] = json.dumps(
        [
            {
                "sku": "SKU-1",
                "name": "منتج",
                "qty": 2,
                "unit_price": "غير معروف",
                "total": "غير معروف",
            }
        ],
        ensure_ascii=False,
    )

    # Prevent residual inference:
    # payment amount does not corroborate order total.
    record["payment_amount"] = "11000"

    result = classify_record(record)

    assert result["quality_status"] == QUALITY_QUARANTINED

    assert ERR_PRICE_UNKNOWN in result["codes_error"]
