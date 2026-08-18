from tests.test_cleaning_rules import (
    base_record,
)

from src.quality_rules import (
    classify_record,
    QUALITY_VALID,
    QUALITY_CORRECTED,
    QUALITY_QUARANTINED,
    ERR_ID_ORDER_MISSING,
    ERR_ID_ORDER_DUPLICATE,
    ERR_ERRORS_CONFLICTING_MULTIPLE,
)


def test_classification_valid():
    result = classify_record(
        base_record()
    )

    assert (
        result["quality_status"]
        == QUALITY_VALID
    )


def test_classification_corrected():
    record = base_record()

    record["status"] = "  مؤكد  "

    result = classify_record(record)

    assert (
        result["quality_status"]
        == QUALITY_CORRECTED
    )


def test_classification_duplicate_quarantine():
    result = classify_record(
        base_record(),
        duplicate_conflict=True,
    )

    assert (
        result["quality_status"]
        == QUALITY_QUARANTINED
    )

    assert (
        ERR_ID_ORDER_DUPLICATE
        in result["codes_error"]
    )


def test_classification_multiple_errors():
    record = base_record()

    record["order_id"] = ""
    record["order_date"] = (
        "2025-19-45 99:70:00"
    )

    result = classify_record(record)

    assert (
        result["quality_status"]
        == QUALITY_QUARANTINED
    )

    assert (
        ERR_ID_ORDER_MISSING
        in result["codes_error"]
    )

    assert (
        ERR_ERRORS_CONFLICTING_MULTIPLE
        in result["codes_error"]
    )
