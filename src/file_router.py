import argparse
from pathlib import Path

from config.settings import (
    SMALL_FILE_THRESHOLD_MB,
    ENGINE_PYTHON_BATCH,
    ENGINE_PYSPARK,
)


def get_file_size_mb(file_path: Path) -> float:
    """Return file size in MiB without reading the file contents."""

    if not file_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {file_path}"
        )

    if not file_path.is_file():
        raise ValueError(
            f"Input path is not a file: {file_path}"
        )

    return file_path.stat().st_size / (1024 * 1024)


def choose_engine(file_path: Path) -> tuple[str, float]:
    """
    Choose the processing engine according to file size.
    """

    size_mb = get_file_size_mb(file_path)

    if size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = ENGINE_PYTHON_BATCH
    else:
        engine = ENGINE_PYSPARK

    return engine, size_mb


def parse_args():
    parser = argparse.ArgumentParser(
        description="Choose Python Batch or PySpark by file size."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the input CSV file."
    )

    return parser.parse_args()


def main():
    args = parse_args()

    file_path = Path(args.input)

    engine, size_mb = choose_engine(file_path)

    print("=" * 60)
    print("FILE ROUTER")
    print("=" * 60)
    print(f"Input file   : {file_path}")
    print(f"File size MB : {size_mb:.2f}")
    print(f"Threshold MB : {SMALL_FILE_THRESHOLD_MB}")
    print(f"Engine       : {engine}")
    print("=" * 60)


if __name__ == "__main__":
    main()
