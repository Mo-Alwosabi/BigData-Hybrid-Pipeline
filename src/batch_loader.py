import argparse
import csv
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from config.settings import (
    BATCH_SIZE,
    RAW_COLLECTION,
    ENGINE_PYTHON_BATCH,
)

from src.mongo_setup import (
    create_mongo_client,
    get_database,
)


def configure_csv_field_limit():
    """Allow large CSV fields such as items_json."""

    limit = sys.maxsize

    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10


def generate_run_id():
    """
    Generate a unique identifier for this ingestion run.
    """

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    suffix = uuid.uuid4().hex[:8]

    return f"run-{timestamp}-{suffix}"


def build_raw_document(
    header,
    row,
    run_id,
    source_file,
    source_row_number,
):
    """
    Preserve the CSV record as raw strings and attach ingestion metadata.
    """

    raw_record = {}

    for index, column_name in enumerate(header):
        if index < len(row):
            raw_record[column_name] = row[index]
        else:
            raw_record[column_name] = None

    # Preserve any unexpected extra columns instead of silently dropping them.
    if len(row) > len(header):
        raw_record["_extra_values"] = row[len(header):]

    return {
        "run_id": run_id,
        "source_file": source_file.name,
        "source_path": str(source_file.resolve()),
        "source_row_number": source_row_number,
        "ingested_at": datetime.now(timezone.utc),
        "engine_used": ENGINE_PYTHON_BATCH,
        "raw_record": raw_record,
    }


def insert_batch(collection, batch, batch_number):
    """
    Insert one batch and return insertion statistics.
    """

    batch_start = time.perf_counter()

    try:
        result = collection.insert_many(
            batch,
            ordered=True
        )
    except Exception as exc:
        print(
            f"BATCH {batch_number} FAILED: {exc}",
            file=sys.stderr
        )
        raise

    batch_elapsed = time.perf_counter() - batch_start

    inserted = len(result.inserted_ids)

    rate = (
        inserted / batch_elapsed
        if batch_elapsed > 0
        else 0
    )

    print(
        f"BATCH {batch_number:03d} | "
        f"rows={len(batch):,} | "
        f"inserted={inserted:,} | "
        f"seconds={batch_elapsed:.3f} | "
        f"rate={rate:,.2f} rows/sec"
    )

    return inserted


def load_csv_to_raw(
    input_path: Path,
    batch_size: int = BATCH_SIZE,
):
    """
    Stream a CSV file into MongoDB orders_raw.

    No cleaning, validation, or transformation is performed.
    """

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    if batch_size <= 0:
        raise ValueError(
            "batch_size must be greater than zero."
        )

    configure_csv_field_limit()

    run_id = generate_run_id()

    rows_read = 0
    loaded_raw = 0
    batch_number = 0

    total_start = time.perf_counter()

    client = None

    try:
        client = create_mongo_client()

        database = get_database(client)

        collection = database[RAW_COLLECTION]

        print("=" * 70)
        print("PYTHON BATCH RAW LOADER")
        print("=" * 70)
        print(f"run_id       : {run_id}")
        print(f"input_file   : {input_path}")
        print(f"collection   : {RAW_COLLECTION}")
        print(f"batch_size   : {batch_size}")
        print(f"engine       : {ENGINE_PYTHON_BATCH}")
        print("=" * 70)

        batch = []

        with input_path.open(
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as source:

            reader = csv.reader(source)

            header = next(reader, None)

            if not header:
                raise ValueError(
                    "Input CSV is empty or has no header."
                )

            for row in reader:

                rows_read += 1

                # Header is logical CSV row 1.
                source_row_number = rows_read + 1

                document = build_raw_document(
                    header=header,
                    row=row,
                    run_id=run_id,
                    source_file=input_path,
                    source_row_number=source_row_number,
                )

                batch.append(document)

                if len(batch) >= batch_size:

                    batch_number += 1

                    loaded_raw += insert_batch(
                        collection,
                        batch,
                        batch_number,
                    )

                    batch = []

            # Insert the final partial batch, if any.
            if batch:

                batch_number += 1

                loaded_raw += insert_batch(
                    collection,
                    batch,
                    batch_number,
                )

        elapsed = time.perf_counter() - total_start

        throughput = (
            loaded_raw / elapsed
            if elapsed > 0
            else 0
        )

        # Verify only documents from this exact run.
        mongo_run_count = collection.count_documents(
            {"run_id": run_id}
        )

        print("\n" + "=" * 70)
        print("RAW LOAD SUMMARY")
        print("=" * 70)
        print(f"run_id             : {run_id}")
        print(f"rows_read          : {rows_read:,}")
        print(f"loaded_raw         : {loaded_raw:,}")
        print(f"mongo_run_count    : {mongo_run_count:,}")
        print(f"batches            : {batch_number}")
        print(f"batch_size         : {batch_size:,}")
        print(f"elapsed_seconds    : {elapsed:.3f}")
        print(f"throughput_rows_s  : {throughput:,.2f}")
        print("=" * 70)

        if rows_read != loaded_raw:
            raise RuntimeError(
                "Raw load consistency failed: "
                f"rows_read={rows_read}, "
                f"loaded_raw={loaded_raw}"
            )

        if loaded_raw != mongo_run_count:
            raise RuntimeError(
                "MongoDB verification failed: "
                f"loaded_raw={loaded_raw}, "
                f"mongo_run_count={mongo_run_count}"
            )

        print("RAW LOAD CONSISTENCY: PASS")

        return {
            "run_id": run_id,
            "rows_read": rows_read,
            "loaded_raw": loaded_raw,
            "batches": batch_number,
            "batch_size": batch_size,
            "elapsed_seconds": elapsed,
            "throughput": throughput,
        }

    finally:
        if client is not None:
            client.close()


def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Stream a small CSV file into MongoDB orders_raw "
            "using configurable Python batches."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the CSV input file."
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=(
            "Number of documents per MongoDB batch. "
            f"Default: {BATCH_SIZE}"
        )
    )

    return parser.parse_args()


def main():

    args = parse_args()

    load_csv_to_raw(
        input_path=Path(args.input),
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
