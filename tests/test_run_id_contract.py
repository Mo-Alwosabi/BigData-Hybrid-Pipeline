import inspect


from src import elt_pipeline
from src import main as main_module
from src import spark_loader


def test_spark_accepts_external_run_id():

    source = inspect.getsource(
        spark_loader.parse_args
    )

    assert "--run-id" in source


def test_elt_accepts_exact_raw_run():

    source = inspect.getsource(
        elt_pipeline.parse_args
    )

    assert "--raw-run-id" in source
    assert "--skip-dry-run-contract" in source


def test_main_passes_same_run_to_spark_and_elt():

    spark_source = inspect.getsource(
        main_module.run_spark_engine
    )

    elt_source = inspect.getsource(
        main_module.run_large_elt
    )

    assert "--run-id" in spark_source
    assert "--raw-run-id" in elt_source


def test_main_has_raw_only_control():

    source = inspect.getsource(
        main_module.parse_args
    )

    assert "--raw-only" in source
