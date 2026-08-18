import inspect


from src.elt_pipeline import (
    STATE_LOOKUP_BATCH_SIZE,
    iter_cursor_batches,
    main,
)


def test_state_lookup_batch_is_bounded():

    assert (
        STATE_LOOKUP_BATCH_SIZE
        == 2000
    )


def test_cursor_batching_is_bounded():

    values = list(
        iter_cursor_batches(
            iter(
                range(4501)
            ),
            2000,
        )
    )

    assert [
        len(batch)
        for batch in values
    ] == [
        2000,
        2000,
        501,
    ]


def test_main_does_not_load_entire_final_state():

    source = inspect.getsource(
        main
    )

    assert (
        "load_existing_state("
        not in source
    )

    assert (
        "load_existing_state_for_keys("
        in source
    )


def test_main_uses_raw_batches():

    source = inspect.getsource(
        main
    )

    assert (
        "iter_cursor_batches("
        in source
    )
