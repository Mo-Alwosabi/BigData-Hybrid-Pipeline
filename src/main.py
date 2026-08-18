import argparse
import os
import subprocess
import sys
import uuid

from datetime import datetime, timezone

from pathlib import Path


from config.settings import (
    BATCH_SIZE,
    ENGINE_PYSPARK,
    ENGINE_PYTHON_BATCH,
    RAW_COLLECTION,
    SMALL_FILE_THRESHOLD_MB,
)

from src.batch_loader import (
    load_csv_to_raw,
)

from src.file_router import (
    choose_engine,
    get_file_size_mb,
)


# ============================================================
# ROUTER NORMALIZATION
# ============================================================

def normalize_engine(
    decision,
):
    """
    Normalize the existing file_router output.

    The Router remains the authority for selecting
    python_batch vs pyspark.
    """

    valid = {
        ENGINE_PYTHON_BATCH,
        ENGINE_PYSPARK,
    }

    if isinstance(
        decision,
        str,
    ):

        if decision in valid:
            return decision

    if isinstance(
        decision,
        dict,
    ):

        for key in (
            "engine",
            "selected_engine",
        ):

            value = decision.get(
                key
            )

            if value in valid:
                return value

    if isinstance(
        decision,
        (
            tuple,
            list,
        ),
    ):

        for value in decision:

            if value in valid:
                return value

    raise RuntimeError(
        "Unsupported result returned by "
        f"file_router.choose_engine(): {decision!r}"
    )


# ============================================================
# ROUTE DECISION
# ============================================================

def resolve_route(
    input_path,
):
    path = Path(
        input_path
    ).resolve()

    if not path.exists():
        raise FileNotFoundError(
            path
        )

    size_mb = get_file_size_mb(
        path
    )

    raw_decision = choose_engine(
        path
    )

    engine = normalize_engine(
        raw_decision
    )

    expected_engine = (
        ENGINE_PYTHON_BATCH
        if size_mb
        <= SMALL_FILE_THRESHOLD_MB
        else ENGINE_PYSPARK
    )

    # Independent consistency gate.
    if engine != expected_engine:
        raise RuntimeError(
            "Router consistency failure: "
            f"router={engine}, "
            f"expected={expected_engine}, "
            f"size={size_mb:,.2f} MB, "
            f"threshold="
            f"{SMALL_FILE_THRESHOLD_MB} MB"
        )

    reason = (
        "file size <= configured threshold"
        if engine
        == ENGINE_PYTHON_BATCH
        else
        "file size > configured threshold"
    )

    return {
        "path": path,
        "file_size_mb": size_mb,
        "engine": engine,
        "reason": reason,
    }


# ============================================================
# PYSPARK RUNTIME
# ============================================================

def build_spark_environment():
    """
    Build the environment used to launch PySpark.
    Uses the installed Temurin Java 21 runtime.
    """

    env = os.environ.copy()

    java_home = Path(
        r"C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot"
    )

    java_exe = (
        java_home
        / "bin"
        / "java.exe"
    )

    if not java_exe.exists():
        raise RuntimeError(
            "Configured Java was not found at "
            f"{java_exe}"
        )

    import pyspark

    spark_home = Path(
        pyspark.__file__
    ).resolve().parent

    env[
        "JAVA_HOME"
    ] = str(
        java_home
    )

    env[
        "SPARK_HOME"
    ] = str(
        spark_home
    )

    env[
        "PYSPARK_PYTHON"
    ] = sys.executable

    env[
        "PYSPARK_DRIVER_PYTHON"
    ] = sys.executable

    env[
        "SPARK_LOCAL_IP"
    ] = "127.0.0.1"

    env[
        "PYTHONUTF8"
    ] = "1"

    env[
        "PATH"
    ] = (
        str(
            java_home
            / "bin"
        )
        + os.pathsep
        + str(
            spark_home
            / "bin"
        )
        + os.pathsep
        + env.get(
            "PATH",
            "",
        )
    )

    return env

# ============================================================
# PIPELINE RUN-ID
# ============================================================

def generate_pipeline_run_id():

    return (
        "pipeline-"
        + datetime.now(
            timezone.utc
        ).strftime(
            "%Y%m%dT%H%M%SZ"
        )
        + "-"
        + uuid.uuid4().hex[:8]
    )


# ============================================================
# ENGINE EXECUTION
# ============================================================

def run_batch_engine(
    input_path,
    batch_size,
):
    print()
    print(
        "Dispatching             : "
        "Python Batch Loader"
    )

    return load_csv_to_raw(
        input_path,
        batch_size,
    )


def run_spark_engine(
    input_path,
    run_id,
):
    print()
    print(
        "Dispatching             : "
        "PySpark Loader"
    )

    command = [
        sys.executable,
        "-m",
        "src.spark_loader",

        "--input",
        str(
            input_path
        ),

        "--collection",
        RAW_COLLECTION,

        "--production-raw",

        "--run-id",
        run_id,
    ]

    print(
        "Spark target            : "
        f"{RAW_COLLECTION}"
    )

    print(
        "Spark Raw mode          : "
        "append"
    )

    result = subprocess.run(
        command,
        env=build_spark_environment(),
        check=False,
    )

    if result.returncode != 0:

        raise RuntimeError(
            "PySpark loader failed with "
            f"exit code {result.returncode}."
        )

    return result.returncode


def run_large_elt(
    raw_run_id,
):

    print()
    print(
        "Dispatching             : "
        "Quality / Cleaning ELT"
    )

    print(
        f"ELT raw_run_id          : "
        f"{raw_run_id}"
    )

    command = [
        sys.executable,
        "-m",
        "src.elt_pipeline",

        "--raw-run-id",
        raw_run_id,

        "--skip-dry-run-contract",
    ]

    result = subprocess.run(
        command,
        check=False,
    )

    if result.returncode != 0:

        raise RuntimeError(
            "ELT pipeline failed with "
            f"exit code {result.returncode}."
        )

    return result.returncode


# ============================================================
# CLI
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Hybrid Big Data Pipeline: "
            "File Router -> Python Batch / PySpark"
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to dirty CSV input file.",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=(
            "Python Batch insert size. "
            "Used only for the small-file path."
        ),
    )

    parser.add_argument(
        "--dry-route",
        action="store_true",
        help=(
            "Show Router decision without "
            "loading or modifying any data."
        ),
    )

    parser.add_argument(
        "--raw-only",
        action="store_true",
        help=(
            "Stop after Raw ingestion. "
            "Useful for controlled Spark evidence capture."
        ),
    )

    return parser.parse_args()


# ============================================================
# MAIN ENTRY POINT
# ============================================================

def main():
    args = parse_args()

    route = resolve_route(
        args.input
    )

    print("=" * 88)
    print(
        "HYBRID BIG DATA PIPELINE - FILE ROUTER"
    )
    print("=" * 88)

    print(
        f"Input file              : "
        f"{route['path']}"
    )

    print(
        f"File size               : "
        f"{route['file_size_mb']:,.2f} MB"
    )

    print(
        f"Configured threshold    : "
        f"{SMALL_FILE_THRESHOLD_MB} MB"
    )

    print(
        f"Selected engine         : "
        f"{route['engine']}"
    )

    print(
        f"Reason                  : "
        f"{route['reason']}"
    )

    print(
        f"Dry-route mode          : "
        f"{'YES' if args.dry_route else 'NO'}"
    )

    print("=" * 88)

    if args.dry_route:

        print()
        print(
            "ROUTER DECISION VERIFIED"
        )

        print(
            "No file processing executed."
        )

        print(
            "No MongoDB write executed."
        )

        return 0

    if (
        route[
            "engine"
        ]
        == ENGINE_PYTHON_BATCH
    ):

        batch_result = (
            run_batch_engine(
                route[
                    "path"
                ],
                args.batch_size,
            )
        )

        batch_run_id = (
            batch_result.get(
                "run_id"
            )
        )

        if not batch_run_id:
            raise RuntimeError(
                "Python Batch loader did not "
                "return a run_id."
            )

        print(
            f"Pipeline run_id         : "
            f"{batch_run_id}"
        )

        if args.raw_only:

            print()
            print(
                "RAW-ONLY MODE: COMPLETE"
            )

            print(
                "ELT was intentionally not started."
            )

        else:

            run_large_elt(
                batch_run_id
            )

    elif (
        route[
            "engine"
        ]
        == ENGINE_PYSPARK
    ):

        pipeline_run_id = (
            generate_pipeline_run_id()
        )

        print(
            f"Pipeline run_id         : "
            f"{pipeline_run_id}"
        )

        run_spark_engine(
            route[
                "path"
            ],
            pipeline_run_id,
        )

        if args.raw_only:

            print()
            print(
                "RAW-ONLY MODE: COMPLETE"
            )

            print(
                "ELT was intentionally not started."
            )

        else:

            run_large_elt(
                pipeline_run_id
            )

    else:

        raise RuntimeError(
            "Unsupported engine: "
            f"{route['engine']}"
        )

    print()
    print("=" * 88)
    print(
        "HYBRID PIPELINE ENGINE EXECUTION: PASS"
    )
    print("=" * 88)

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
