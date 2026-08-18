import inspect

from src import main as main_module


def test_python_batch_path_continues_to_exact_elt_run():

    source = inspect.getsource(
        main_module.main
    )

    assert "batch_result" in source
    assert "batch_run_id" in source
    assert "run_large_elt" in source


def test_main_has_both_safe_control_modes():

    source = inspect.getsource(
        main_module.parse_args
    )

    assert "--dry-route" in source
    assert "--raw-only" in source
