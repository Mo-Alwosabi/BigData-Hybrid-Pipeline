import json

from src.quality_rules import (
    classify_record,
    QUALITY_VALID,
    QUALITY_CORRECTED,
    QUALITY_QUARANTINED,
    ERR_DATE_IMPOSSIBLE_INVALID,
    ERR_JSON_ITEMS_CORRUPTED,
    ERR_CURRENCY_UNKNOWN,
    ERR_EMAIL_INVALID,
    RULE_ARABIC_DIGITS,
    RULE_KNOWN_PRICE_WORD,
    RULE_PHONE,
    RULE_EMAIL,
    RULE_DATE,
    RULE_NEGATIVE_QTY,
    RULE_ITEM_PRICE_RESIDUAL,
    RULE_ORDER_TOTAL_RECALCULATED,
)


def base_record():
    return {
        "order_id": "طلب-1",
        "order_date": "2025-01-31T12:00:00",
        "status": "مؤكد",
        "customer_id": "عميل-1",
        "customer_name": "محمد",
        "customer_phone": "777123456",
        "customer_email": "user@example.com",
        "city": "صنعاء",
        "district": "التحرير",
        "delivery_type": "توصيل",
        "delivery_cost": "2000",
        "payment_method": "نقدي",
        "payment_status": "تم الدفع",
        "payment_amount": "12000",
        "currency": "YER",
        "total_amount": "12000",
        "items_json": json.dumps(
            [
                {
                    "sku": "SKU-1",
                    "name": "منتج",
                    "qty": 2,
                    "unit_price": 5000,
                    "total": 10000,
                }
            ],
            ensure_ascii=False,
        ),
    }


def rule_codes(result):
    return {
        correction["rule_code"]
        for correction
        in result["corrections"]
    }


def test_valid_record():
    result = classify_record(
        base_record()
    )

    assert (
        result["quality_status"]
        == QUALITY_VALID
    )

    assert result["codes_error"] == []


def test_arabic_digits_are_corrected():
    record = base_record()

    record["total_amount"] = (
        "١٢٠٠٠٫٠"
    )

    result = classify_record(record)

    assert (
        result["quality_status"]
        == QUALITY_CORRECTED
    )

    assert (
        result["cleaned_record"]
        ["total_amount"]
        == 12000
    )

    assert (
        RULE_ARABIC_DIGITS
        in rule_codes(result)
    )


def test_known_price_word():
    record = base_record()

    record["delivery_cost"] = (
        "ألفان"
    )

    result = classify_record(record)

    assert (
        result["cleaned_record"]
        ["delivery_cost"]
        == 2000
    )

    assert (
        RULE_KNOWN_PRICE_WORD
        in rule_codes(result)
    )


def test_phone_formatting():
    record = base_record()

    record["customer_phone"] = (
        "+967 777123456"
    )

    result = classify_record(record)

    assert (
        result["cleaned_record"]
        ["customer_phone"]
        == "777123456"
    )

    assert (
        RULE_PHONE
        in rule_codes(result)
    )


def test_email_repair():
    record = base_record()

    record["customer_email"] = (
        "user@@mail..com"
    )

    result = classify_record(record)

    assert (
        result["cleaned_record"]
        ["customer_email"]
        == "user@mail.com"
    )

    assert (
        RULE_EMAIL
        in rule_codes(result)
    )


def test_date_normalization():
    record = base_record()

    record["order_date"] = (
        "17-01-2025 04:50:00"
    )

    result = classify_record(record)

    assert (
        result["cleaned_record"]
        ["order_date"]
        == "2025-01-17T04:50:00"
    )

    assert (
        RULE_DATE
        in rule_codes(result)
    )


def test_negative_qty_is_derived():
    record = base_record()

    record["items_json"] = json.dumps(
        [
            {
                "sku": "SKU-1",
                "name": "منتج",
                "qty": -2,
                "unit_price": 5000,
                "total": 15000,
            }
        ],
        ensure_ascii=False,
    )

    record["total_amount"] = "17000"
    record["payment_amount"] = "17000"

    result = classify_record(record)

    items = json.loads(
        result["cleaned_record"]
        ["items_json"]
    )

    assert items[0]["qty"] == 3

    assert (
        RULE_NEGATIVE_QTY
        in rule_codes(result)
    )

    assert (
        result["quality_status"]
        == QUALITY_CORRECTED
    )


def test_residual_price_recovery():
    record = base_record()

    record["items_json"] = json.dumps(
        [
            {
                "sku": "SKU-1",
                "name": "منتج 1",
                "qty": 2,
                "unit_price": 5000,
                "total": 10000,
            },
            {
                "sku": "SKU-2",
                "name": "منتج 2",
                "qty": 2,
                "unit_price": "غير معروف",
                "total": "غير معروف",
            },
        ],
        ensure_ascii=False,
    )

    record["delivery_cost"] = "2000"
    record["total_amount"] = "22000"
    record["payment_amount"] = "22000"

    result = classify_record(record)

    items = json.loads(
        result["cleaned_record"]
        ["items_json"]
    )

    assert (
        items[1]["total"]
        == 10000
    )

    assert (
        items[1]["unit_price"]
        == 5000
    )

    assert (
        RULE_ITEM_PRICE_RESIDUAL
        in rule_codes(result)
    )

    assert (
        result["quality_status"]
        == QUALITY_CORRECTED
    )


def test_order_total_recalculation():
    record = base_record()

    record["total_amount"] = "9999"

    result = classify_record(record)

    assert (
        result["cleaned_record"]
        ["total_amount"]
        == 12000
    )

    assert (
        RULE_ORDER_TOTAL_RECALCULATED
        in rule_codes(result)
    )


def test_impossible_date_quarantine():
    record = base_record()

    record["order_date"] = (
        "2025-19-45 99:70:00"
    )

    result = classify_record(record)

    assert (
        result["quality_status"]
        == QUALITY_QUARANTINED
    )

    assert (
        ERR_DATE_IMPOSSIBLE_INVALID
        in result["codes_error"]
    )


def test_corrupted_json_quarantine():
    record = base_record()

    record["items_json"] = (
        "not-json"
    )

    result = classify_record(record)

    assert (
        result["quality_status"]
        == QUALITY_QUARANTINED
    )

    assert (
        ERR_JSON_ITEMS_CORRUPTED
        in result["codes_error"]
    )


def test_unknown_currency_quarantine():
    record = base_record()

    record["currency"] = (
        "عملة غير معروفة"
    )

    result = classify_record(record)

    assert (
        result["quality_status"]
        == QUALITY_QUARANTINED
    )

    assert (
        ERR_CURRENCY_UNKNOWN
        in result["codes_error"]
    )


def test_unrecoverable_email_quarantine():
    record = base_record()

    record["customer_email"] = (
        "user-without-domain"
    )

    result = classify_record(record)

    assert (
        result["quality_status"]
        == QUALITY_QUARANTINED
    )

    assert (
        ERR_EMAIL_INVALID
        in result["codes_error"]
    )
