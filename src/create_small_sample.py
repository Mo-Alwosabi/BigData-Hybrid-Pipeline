import argparse
import csv
import sys
import time
from pathlib import Path


def configure_csv_field_limit():
    """Allow large CSV fields such as items_json."""
    limit = sys.maxsize

    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10


def create_sample(input_path: Path, output_path: Path, rows: int) -> int:
    """
    Copy the first N CSV records into a reproducible sample.

    The source file is opened read-only.
    No cleaning or transformation is performed.
    """

    if rows <= 0:
        raise ValueError("--rows must be greater than zero.")

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    configure_csv_field_limit()

    copied_rows = 0
    start_time = time.perf_counter()

    with (
        input_path.open(
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as source,
        output_path.open(
            "w",
            encoding="utf-8-sig",
            newline=""
        ) as destination
    ):
        reader = csv.reader(source)
        writer = csv.writer(destination)

        # Copy header.
        header = next(reader, None)

        if header is None:
            raise ValueError("Input CSV is empty.")

        writer.writerow(header)

        # Stream only the requested number of data records.
        for row in reader:
            if copied_rows >= rows:
                break

            writer.writerow(row)
            copied_rows += 1

    elapsed = time.perf_counter() - start_time
    output_size_mb = output_path.stat().st_size / (1024 * 1024)

    print("=" * 60)
    print("SMALL SAMPLE CREATION")
    print("=" * 60)
    print(f"Input file     : {input_path}")
    print(f"Output file    : {output_path}")
    print(f"Requested rows : {rows:,}")
    print(f"Copied rows    : {copied_rows:,}")
    print(f"Output size MB : {output_size_mb:.2f}")
    print(f"Elapsed seconds: {elapsed:.2f}")
    print("=" * 60)

    if copied_rows != rows:
        print(
            f"WARNING: Requested {rows:,} rows, "
            f"but source contained only {copied_rows:,}."
        )

    return copied_rows


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Create a reproducible small CSV sample "
            "from the original dirty dataset."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the original CSV file."
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Path for the generated sample CSV."
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=100000,
        help="Number of data rows to copy. Default: 100000."
    )

    return parser.parse_args()


def main():
    args = parse_args()

    create_sample(
        input_path=Path(args.input),
        output_path=Path(args.output),
        rows=args.rows
    )


if __name__ == "__main__":
    main()
