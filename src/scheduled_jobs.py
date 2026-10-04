import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from src.mongo_setup import create_mongo_client, get_database
from src.materialized_views import incremental_refresh
from config.settings import REPORTS_DIR

JOB_RUNS_COLLECTION = "scheduled_job_runs"
DAILY_REPORT_FILE = REPORTS_DIR / "scheduled_daily_sales_report.json"

JOBS = {
    "refresh_materialized_views": {
        "description": "Incrementally refresh materialized views",
        "hour": int(os.getenv("MV_REFRESH_HOUR", "2")),
        "minute": int(os.getenv("MV_REFRESH_MINUTE", "0")),
    },
    "generate_daily_sales_report": {
        "description": "Generate daily sales summary report",
        "hour": int(os.getenv("DAILY_REPORT_HOUR", "2")),
        "minute": int(os.getenv("DAILY_REPORT_MINUTE", "30")),
    },
}

def utc_now():
    return datetime.now(timezone.utc)

def ensure_job_collection(db):
    collection = db[JOB_RUNS_COLLECTION]
    collection.create_index(
        [("job_name", 1), ("started_at", -1)],
        name="ix_job_name_started_at",
    )
    collection.create_index(
        [("status", 1)],
        name="ix_job_status",
    )
    return collection

def record_job_run(
    collection,
    job_name,
    started_at,
    ended_at,
    status,
    result=None,
    error=None,
):
    duration = (
        ended_at - started_at
    ).total_seconds()

    document = {
        "job_name": job_name,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": duration,
        "status": status,
        "result": result,
        "error": error,
    }

    collection.insert_one(document)

def run_refresh_materialized_views(db):
    return incremental_refresh(db)

def run_daily_sales_report(db):
    cursor = (
        db["daily_sales_summary"]
        .find(
            {},
            {"_id": 0},
        )
        .sort(
            "day",
            -1,
        )
    )

    rows = list(cursor)

    total_orders = sum(
        int(
            row.get(
                "order_count",
                0,
            )
        )
        for row in rows
    )

    total_sales = sum(
        float(
            row.get(
                "total_sales",
                0,
            )
        )
        for row in rows
    )

    report = {
        "report": "daily_sales_summary",
        "generated_at": utc_now(),
        "days": len(rows),
        "total_orders": total_orders,
        "total_sales": total_sales,
        "daily_rows": rows,
    }

    REPORTS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    with DAILY_REPORT_FILE.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            report,
            file,
            ensure_ascii=False,
            indent=2,
            default=str,
        )

    return {
        "report_file": str(
            DAILY_REPORT_FILE
        ),
        "days": len(rows),
        "total_orders": total_orders,
        "total_sales": total_sales,
    }

def run_job(job_name):
    if job_name not in JOBS:
        raise ValueError(
            f"Unknown job: {job_name}"
        )

    client = None
    started_at = utc_now()
    run_collection = None

    try:
        client = create_mongo_client()
        db = get_database(client)
        run_collection = ensure_job_collection(db)

        if job_name == "refresh_materialized_views":
            result = run_refresh_materialized_views(
                db
            )
        elif job_name == "generate_daily_sales_report":
            result = run_daily_sales_report(
                db
            )
        else:
            raise ValueError(
                f"Unsupported job: {job_name}"
            )

        ended_at = utc_now()

        record_job_run(
            run_collection,
            job_name,
            started_at,
            ended_at,
            "success",
            result=result,
        )

        return {
            "job_name": job_name,
            "status": "success",
            "started_at": started_at,
            "ended_at": ended_at,
            "result": result,
        }

    except Exception as exc:
        ended_at = utc_now()

        if run_collection is None:
            try:
                if client is not None:
                    db = get_database(client)
                    run_collection = ensure_job_collection(
                        db
                    )
            except Exception:
                run_collection = None

        if run_collection is not None:
            record_job_run(
                run_collection,
                job_name,
                started_at,
                ended_at,
                "failed",
                error=str(exc),
            )

        raise

    finally:
        if client is not None:
            client.close()

def list_jobs():
    for name, config in JOBS.items():
        print(
            f"{name}: "
            f"{config['hour']:02d}:"
            f"{config['minute']:02d}"
        )
        print(
            f"  {config['description']}"
        )

def scheduler_loop():
    print("=" * 70)
    print("SCHEDULED JOBS SERVICE")
    print("=" * 70)

    list_jobs()

    print("=" * 70)
    print("Scheduler is running...")
    print("Press Ctrl+C to stop.")
    print("=" * 70)

    executed_today = set()
    current_date = None

    while True:
        now = datetime.now()

        if current_date != now.date():
            current_date = now.date()
            executed_today.clear()

        for job_name, config in JOBS.items():
            key = (
                job_name,
                current_date,
            )

            if (
                now.hour == config["hour"]
                and now.minute
                == config["minute"]
                and key not in executed_today
            ):
                print(
                    f"\nRunning scheduled job: "
                    f"{job_name}"
                )

                try:
                    result = run_job(
                        job_name
                    )
                    print(
                        json.dumps(
                            result,
                            ensure_ascii=False,
                            indent=2,
                            default=str,
                        )
                    )
                except Exception as exc:
                    print(
                        f"Job failed: {exc}"
                    )

                executed_today.add(key)

        time.sleep(20)

def main():
    parser = argparse.ArgumentParser(
        description="Project Scheduled Jobs"
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List configured scheduled jobs.",
    )

    parser.add_argument(
        "--run",
        choices=JOBS.keys(),
        help="Run one job manually.",
    )

    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Start the scheduler service.",
    )

    args = parser.parse_args()

    if args.list:
        list_jobs()
        return

    if args.run:
        result = run_job(
            args.run
        )
        print(
            json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )
        return

    if args.schedule:
        try:
            scheduler_loop()
        except KeyboardInterrupt:
            print(
                "\nScheduler stopped."
            )
        return

    parser.error(
        "Use --list, --run JOB_NAME, or --schedule"
    )

if __name__ == "__main__":
    main()