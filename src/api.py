import json
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from config.settings import (
    BATCH_SIZE,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)

from src.mongo_setup import (
    create_mongo_client,
    get_database,
)

from src.main import (
    resolve_route,
    run_batch_engine,
    run_spark_engine,
    run_large_elt,
    generate_pipeline_run_id,
)

from src.aggregation_reports import REPORTS

from src.materialized_views import (
    initial_build,
    incremental_refresh,
    show_views,
)

from src.scheduled_jobs import (
    JOBS,
    run_job,
)


app = FastAPI(
    title="Big Data Hybrid Pipeline API",
    description=(
        "Unified API for the Big Data final project. "
        "Exposes ingestion, indexes, queries, aggregations, "
        "materialized views, and scheduled jobs."
    ),
    version="1.0.0",
)


class IngestRequest(BaseModel):
    input_path: str
    batch_size: int = BATCH_SIZE
    raw_only: bool = False
    dry_route: bool = False


def get_db_connection():
    client = create_mongo_client()
    db = get_database(client)
    return client, db


def serialize(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            str(k): serialize(v)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [
            serialize(v)
            for v in value
        ]
    return value


@app.get("/health")
def health():
    client = None
    try:
        client, db = get_db_connection()
        ping = db.command("ping")
        return {
            "status": "ok",
            "mongodb": ping.get("ok") == 1,
            "database": db.name,
            "timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
        }
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "error",
                "message": str(exc),
            },
        )
    finally:
        if client is not None:
            client.close()


@app.post("/ingest")
def ingest(request: IngestRequest):
    client = None

    try:
        route = resolve_route(
            request.input_path
        )

        if request.dry_route:
            return serialize(
                {
                    "mode": "dry_route",
                    "route": route,
                    "message": (
                        "No ingestion executed."
                    ),
                }
            )

        if request.batch_size <= 0:
            raise HTTPException(
                status_code=400,
                detail=(
                    "batch_size must be greater than zero."
                ),
            )

        if route["engine"] == "python_batch":
            result = run_batch_engine(
                route["path"],
                request.batch_size,
            )

            raw_run_id = result.get(
                "run_id"
            )

            if not raw_run_id:
                raise RuntimeError(
                    "Python Batch loader did not return a run_id."
                )

            elt_result = None

            if not request.raw_only:
                elt_result = run_large_elt(
                    raw_run_id
                )

            return serialize(
                {
                    "status": "success",
                    "route": route,
                    "raw_run_id": raw_run_id,
                    "loader_result": result,
                    "elt_started": (
                        not request.raw_only
                    ),
                    "elt_result": elt_result,
                }
            )

        if route["engine"] == "pyspark":
            pipeline_run_id = (
                generate_pipeline_run_id()
            )

            spark_result = run_spark_engine(
                route["path"],
                pipeline_run_id,
            )

            elt_result = None

            if not request.raw_only:
                elt_result = run_large_elt(
                    pipeline_run_id
                )

            return serialize(
                {
                    "status": "success",
                    "route": route,
                    "raw_run_id": pipeline_run_id,
                    "spark_result": spark_result,
                    "elt_started": (
                        not request.raw_only
                    ),
                    "elt_result": elt_result,
                }
            )

        raise RuntimeError(
            f"Unsupported engine: {route['engine']}"
        )

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": str(exc),
            },
        )
    finally:
        if client is not None:
            client.close()


@app.post("/indexes")
def create_indexes():
    client = None

    try:
        client, db = get_db_connection()

        validated = db[
            VALIDATED_COLLECTION
        ]

        quarantine = db[
            QUARANTINE_COLLECTION
        ]

        created = []

        created.append(
            validated.create_index(
                [("order_id", 1)],
                unique=True,
                name="uq_orders_validated_order_id",
            )
        )

        created.append(
            validated.create_index(
                [("quality_status", 1)],
                name="ix_validated_quality_status",
            )
        )

        created.append(
            validated.create_index(
                [("city", 1)],
                name="ix_validated_city",
            )
        )

        created.append(
            validated.create_index(
                [("status", 1)],
                name="ix_validated_status",
            )
        )

        created.append(
            validated.create_index(
                [
                    ("city", 1),
                    ("status", 1),
                ],
                name="ix_validated_city_status",
            )
        )

        quarantine.create_index(
            [("quarantine_key", 1)],
            unique=True,
            name="uq_quarantine_key",
        )

        quarantine.create_index(
            [("source_run_id", 1)],
            name="ix_quarantine_source_run",
        )

        quarantine.create_index(
            [("error_codes", 1)],
            name="ix_quarantine_error_codes",
        )

        return {
            "status": "success",
            "collection": VALIDATED_COLLECTION,
            "indexes": created,
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": str(exc),
            },
        )
    finally:
        if client is not None:
            client.close()


QUERY_DEFINITIONS = {
    "by_city": {
        "description": (
            "Find orders by city."
        ),
        "filter": lambda params: {
            "city": params["city"]
        },
    },
    "by_status": {
        "description": (
            "Find orders by status."
        ),
        "filter": lambda params: {
            "status": params["status"]
        },
    },
    "by_city_status": {
        "description": (
            "Find orders by city and status."
        ),
        "filter": lambda params: {
            "city": params["city"],
            "status": params["status"],
        },
    },
    "by_customer": {
        "description": (
            "Find orders by customer ID."
        ),
        "filter": lambda params: {
            "customer_id": params[
                "customer_id"
            ]
        },
    },
    "by_date_range": {
        "description": (
            "Find orders within an order-date range."
        ),
        "filter": lambda params: {
            "order_date": {
                "$gte": params["start_date"],
                "$lte": params["end_date"],
            }
        },
    },
}


@app.get("/queries")
def list_queries():
    return {
        "count": len(QUERY_DEFINITIONS),
        "queries": [
            {
                "name": name,
                "description": config[
                    "description"
                ],
            }
            for name, config
            in QUERY_DEFINITIONS.items()
        ],
    }


@app.get("/queries/{name}")
def run_query(
    name: str,
    city: Optional[str] = Query(
        default="تعز"
    ),
    status: Optional[str] = Query(
        default="مؤكد"
    ),
    customer_id: Optional[str] = Query(
        default="عميل-0"
    ),
    start_date: Optional[str] = Query(
        default="2025-01-01T00:00:00"
    ),
    end_date: Optional[str] = Query(
        default="2025-12-31T23:59:59"
    ),
    limit: int = Query(
        default=20,
        ge=1,
        le=100,
    ),
):
    if name not in QUERY_DEFINITIONS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown query: {name}",
        )

    params = {
        "city": city,
        "status": status,
        "customer_id": customer_id,
        "start_date": start_date,
        "end_date": end_date,
    }

    client = None

    try:
        client, db = get_db_connection()

        collection = db[
            VALIDATED_COLLECTION
        ]

        query_filter = (
            QUERY_DEFINITIONS[name]["filter"](
                params
            )
        )

        documents = list(
            collection.find(
                query_filter,
                {"_id": 0},
            ).limit(limit)
        )

        return serialize(
            {
                "query": name,
                "filter": query_filter,
                "count": len(documents),
                "results": documents,
            }
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": str(exc),
            },
        )
    finally:
        if client is not None:
            client.close()


@app.get("/aggregations")
def list_aggregations():
    return {
        "count": len(REPORTS),
        "aggregations": [
            {
                "name": name,
            }
            for name in REPORTS.keys()
        ],
    }


@app.get("/aggregations/{name}")
def run_aggregation(name: str):
    if name not in REPORTS:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Unknown aggregation: {name}"
            ),
        )

    client = None

    try:
        client, db = get_db_connection()

        collection = db[
            VALIDATED_COLLECTION
        ]

        result = REPORTS[name](
            collection
        )

        return serialize(
            {
                "aggregation": name,
                "count": len(result),
                "results": result,
            }
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": str(exc),
            },
        )
    finally:
        if client is not None:
            client.close()


@app.post("/refresh-mv")
def refresh_materialized_views():
    client = None

    try:
        client, db = get_db_connection()

        result = incremental_refresh(
            db
        )

        return serialize(
            {
                "status": "success",
                "result": result,
            }
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": str(exc),
            },
        )
    finally:
        if client is not None:
            client.close()


@app.get("/jobs")
def list_jobs():
    return {
        "count": len(JOBS),
        "jobs": [
            {
                "name": name,
                "description": config[
                    "description"
                ],
                "hour": config[
                    "hour"
                ],
                "minute": config[
                    "minute"
                ],
            }
            for name, config
            in JOBS.items()
        ],
    }


@app.post("/jobs/{name}/run")
def run_scheduled_job(name: str):
    if name not in JOBS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown job: {name}",
        )

    try:
        result = run_job(name)

        return serialize(
            result
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": str(exc),
            },
        )