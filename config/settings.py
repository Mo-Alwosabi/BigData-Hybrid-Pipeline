from pathlib import Path
import os


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
SAMPLES_DIR = DATA_DIR / "samples"

REPORTS_DIR = PROJECT_ROOT / "reports"
RESULTS_JSON_PATH = REPORTS_DIR / "results.json"
SCREENSHOTS_DIR = REPORTS_DIR / "screenshots"


# ============================================================
# MONGODB
# ============================================================

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://127.0.0.1:27017"
)

MONGO_DATABASE = os.getenv(
    "MONGO_DATABASE",
    "bigdata_midterm"
)

RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"


# ============================================================
# FILE ROUTER
# ============================================================

SMALL_FILE_THRESHOLD_MB = int(
    os.getenv("SMALL_FILE_THRESHOLD_MB", "200")
)


# ============================================================
# PYTHON BATCH
# ============================================================

BATCH_SIZE = int(
    os.getenv("BATCH_SIZE", "5000")
)


# ============================================================
# SPARK
# ============================================================

SPARK_APP_NAME = "BigDataHybridPipeline"

SPARK_MASTER = os.getenv(
    "SPARK_MASTER",
    "local[*]"
)

MONGO_SPARK_FORMAT = "mongodb"


# ============================================================
# PROJECT CONSTANTS
# ============================================================

ENGINE_PYTHON_BATCH = "python_batch"
ENGINE_PYSPARK = "pyspark"

QUALITY_VALID = "valid"
QUALITY_CORRECTED = "corrected"
QUALITY_QUARANTINED = "quarantined"
