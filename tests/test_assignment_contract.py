from config.settings import (
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)

from src.quality_rules import (
    ERR_ID_ORDER_MISSING,
    ERR_ID_CUSTOMER_MISSING,
    ERR_DATE_IMPOSSIBLE_INVALID,
    ERR_JSON_ITEMS_CORRUPTED,
    ERR_ITEMS_EMPTY,
    ERR_PRICE_UNKNOWN,
    ERR_VALUE_NEGATIVE_AMBIGUOUS,
    ERR_ID_ORDER_DUPLICATE,
    ERR_ERRORS_CONFLICTING_MULTIPLE,
)


def test_official_collection_names():

    assert RAW_COLLECTION == "orders_raw"

    assert (
        VALIDATED_COLLECTION
        == "orders_validated"
    )

    assert (
        QUARANTINE_COLLECTION
        == "orders_quarantine"
    )


def test_official_quarantine_codes():

    assert (
        ERR_ID_ORDER_MISSING
        == "MISSING_ORDER_ID"
    )

    assert (
        ERR_ID_CUSTOMER_MISSING
        == "MISSING_CUSTOMER_ID"
    )

    assert (
        ERR_DATE_IMPOSSIBLE_INVALID
        == "INVALID_IMPOSSIBLE_DATE"
    )

    assert (
        ERR_JSON_ITEMS_CORRUPTED
        == "CORRUPTED_ITEMS_JSON"
    )

    assert (
        ERR_ITEMS_EMPTY
        == "EMPTY_ITEMS"
    )

    assert (
        ERR_PRICE_UNKNOWN
        == "UNKNOWN_PRICE"
    )

    assert (
        ERR_VALUE_NEGATIVE_AMBIGUOUS
        == "AMBIGUOUS_NEGATIVE_VALUE"
    )

    assert (
        ERR_ID_ORDER_DUPLICATE
        == "DUPLICATE_ORDER_ID"
    )

    assert (
        ERR_ERRORS_CONFLICTING_MULTIPLE
        == "MULTIPLE_CONFLICTING_ERRORS"
    )
