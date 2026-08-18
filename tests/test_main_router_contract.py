import pytest


from config.settings import (
    ENGINE_PYSPARK,
    ENGINE_PYTHON_BATCH,
)

from src.main import (
    normalize_engine,
)


def test_main_normalizes_string_engine():

    assert (
        normalize_engine(
            ENGINE_PYTHON_BATCH
        )
        == ENGINE_PYTHON_BATCH
    )

    assert (
        normalize_engine(
            ENGINE_PYSPARK
        )
        == ENGINE_PYSPARK
    )


def test_main_normalizes_mapping_engine():

    assert (
        normalize_engine(
            {
                "engine":
                    ENGINE_PYSPARK
            }
        )
        == ENGINE_PYSPARK
    )


def test_main_rejects_unknown_router_result():

    with pytest.raises(
        RuntimeError
    ):

        normalize_engine(
            "unknown_engine"
        )
