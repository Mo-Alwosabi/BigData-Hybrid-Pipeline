from pyspark.sql.types import (
    StringType,
)

from src.spark_loader import (
    RAW_COLUMNS,
    CORRUPT_COLUMN,
    build_csv_schema,
)


def test_spark_raw_source_columns():
    assert RAW_COLUMNS == [
        "order_id",
        "order_date",
        "status",
        "customer_id",
        "customer_name",
        "customer_phone",
        "customer_email",
        "city",
        "district",
        "delivery_type",
        "delivery_cost",
        "payment_method",
        "payment_status",
        "payment_amount",
        "currency",
        "total_amount",
        "items_json",
    ]


def test_spark_raw_schema_is_explicit_string_schema():
    schema = build_csv_schema()

    expected_names = (
        RAW_COLUMNS
        + [CORRUPT_COLUMN]
    )

    assert (
        schema.fieldNames()
        == expected_names
    )

    assert all(
        isinstance(
            field.dataType,
            StringType,
        )
        for field in schema.fields
    )
