from pathlib import Path
import json
import re
import subprocess
import sys

ROOT = Path.home() / "Desktop" / "BigData_Hybrid_Pipeline"
REPORTS = ROOT / "reports"

checks = []

def add(req_id, title, status, detail):
    checks.append({
        "requirement": req_id,
        "title": title,
        "status": status,
        "detail": detail,
    })
    icon = {"PASS":"PASS", "FAIL":"FAIL", "WARN":"WARN", "PENDING":"PENDING"}[status]
    print(f"[{icon:<7}] {req_id:<8} {title}")
    if detail:
        print(f"          {detail}")

def read(rel):
    p = ROOT / rel
    if not p.exists():
        return ""
    return p.read_text(encoding="utf-8-sig", errors="replace")

def exists(rel):
    return (ROOT / rel).exists()

def has_all(text, terms):
    low = text.lower()
    return all(t.lower() in low for t in terms)

def has_any(text, terms):
    low = text.lower()
    return any(t.lower() in low for t in terms)

def json_load(rel):
    p = ROOT / rel
    if not p.exists():
        return None
    with p.open("r", encoding="utf-8-sig") as f:
        return json.load(f)

print("=" * 92)
print("FINAL COMPLIANCE AUDIT - OFFICIAL INDIVIDUAL CORE REQUIREMENTS")
print("MODE: READ-ONLY FOR DATABASE / NO 30M REPROCESSING")
print("=" * 92)

required_files = [
    "README.md", "requirements.txt", "config/settings.py",
    "src/main.py", "src/file_router.py", "src/create_small_sample.py",
    "src/batch_loader.py", "src/spark_loader.py", "src/quality_rules.py",
    "src/elt_pipeline.py", "src/mongo_setup.py", "src/metrics.py",
    "reports/results.json", "reports/results.md",
    "reports/final_verification.json",
    "reports/elt_write_report_final_idempotency.json",
    "reports/upsert_update_proof.json",
]
missing = [x for x in required_files if not exists(x)]
add("STRUCT", "Required project files", "PASS" if not missing else "FAIL",
    "All required core files exist." if not missing else "Missing: " + ", ".join(missing))

settings = read("config/settings.py")
main = read("src/main.py")
router = read("src/file_router.py")
sample = read("src/create_small_sample.py")
batch = read("src/batch_loader.py")
spark = read("src/spark_loader.py")
quality = read("src/quality_rules.py")
elt = read("src/elt_pipeline.py")
readme = read("README.md")
results_md = read("reports/results.md")

# 6.1
sample_cli = has_all(sample, ["--input", "--rows"])
sample_stream = has_any(sample, ["csv.reader", "csv.dictreader"])
add("6.1", "Reproducible configurable small sample",
    "PASS" if sample_cli and sample_stream else "FAIL",
    "Independent script supports --input and --rows and streams CSV."
    if sample_cli and sample_stream else
    f"--input/--rows CLI={sample_cli}; streaming CSV={sample_stream}. Official requirement needs configurable rows/size.")

# 6.2
router_ok = (
    "SMALL_FILE_THRESHOLD_MB" in settings
    and "os.getenv" in settings
    and "ENGINE_PYTHON_BATCH" in main
    and "ENGINE_PYSPARK" in main
    and "file_size_mb" in main
    and "reason" in main
)
justification_terms = [
    "configurable", "overhead", "200 mb", "threshold"
]
threshold_justified = (
    "200 mb" in readme.lower()
    and (
        ("configurable" in readme.lower() and "spark" in readme.lower())
        or ("overhead" in readme.lower())
        or ("reason" in results_md.lower() and "threshold" in results_md.lower())
    )
)
add("6.2", "Automatic File Router + threshold justification",
    "PASS" if router_ok and threshold_justified else "FAIL",
    f"Router contract={router_ok}; explicit written threshold justification={threshold_justified}.")

# 6.3
batch_ok = (
    has_any(batch, ["csv.reader", "csv.dictreader"])
    and "insert_many" in batch
    and "batch_size" in batch.lower()
    and "list(reader)" not in batch.replace(" ", "").lower()
    and "pandas" not in batch.lower()
    and has_any(batch, ["throughput", "records/sec", "elapsed"])
)
add("6.3", "Python Batch streaming loader",
    "PASS" if batch_ok else "FAIL",
    "Streaming CSV, configurable batches, insert_many, progress/performance, no Pandas/full list."
    if batch_ok else "One or more Batch contract markers are missing.")

# 6.4
# Validate real PySpark DataFrame API usage structurally rather than
# requiring the literal word "DataFrame" to appear in source code.
# Runtime evidence from the official 30M run is also required.
spark_report = json_load("reports/spark_large_run_final.json") or {}

spark_engine = (spark_report.get("engine") or {})
spark_schema = (spark_report.get("schema") or {})
spark_counts = (spark_report.get("counts") or {})
spark_performance = (spark_report.get("performance") or {})

dataframe_api_ok = (
    ("spark.read" in spark or ".read." in spark)
    and (".csv(" in spark or '.format("csv")' in spark or ".format('csv')" in spark)
    and (".write" in spark)
)

spark_static_ok = (
    "SparkSession" in spark
    and dataframe_api_ok
    and "StructType" in spark
    and "StringType" in spark
    and "mongodb" in spark.lower()
    and "input_partitions" in spark
    and "throughput" in spark.lower()
    and "inferSchema" not in spark
    and "import pandas" not in spark.lower()
)

spark_runtime_ok = (
    spark_report.get("run_id") == "pipeline-20260816T235754Z-866233e7"
    and spark_engine.get("engine_used") == "pyspark"
    and spark_counts.get("rows_read") == 30000000
    and spark_counts.get("mongo_run_count") == 30000000
    and spark_counts.get("csv_corrupt_records") == 0
    and spark_engine.get("input_partitions") == 99
    and spark_engine.get("output_partitions") == 99
    and spark_schema.get("explicit") is True
    and spark_schema.get("raw_fields_are_strings") is True
    and float(spark_performance.get("throughput_records_per_second", 0)) > 0
    and spark_report.get("production_safety_pass") is True
)

repartition_used = ".repartition(" in spark
spark_ok = spark_static_ok and spark_runtime_ok

add("6.4", "PySpark large-file loader",
    "PASS" if spark_ok and not repartition_used else "FAIL",
    (
        "SparkSession + real DataFrame read/write API + fixed String schema + "
        "MongoDB Spark Connector + official 30M runtime evidence verified; "
        f"unjustified repartition present={repartition_used}."
    ) if spark_ok else
    (
        f"Static PySpark contract={spark_static_ok}; "
        f"official 30M runtime evidence={spark_runtime_ok}; "
        f"unjustified repartition present={repartition_used}."
    ))

# 6.5
raw_markers = ["run_id", "source_file", "ingested_at", "engine_used", "raw_record"]
raw_ok = all(x in (batch + spark) for x in raw_markers) and "source_row_number" in (batch + spark)
add("6.5", "Raw-first ELT metadata and traceability",
    "PASS" if raw_ok else "FAIL",
    "run_id/source/source-row/ingested_at/engine/raw_record markers are implemented."
    if raw_ok else "One or more mandatory Raw metadata markers are missing.")

# 6.6
rule_markers = [
    "ARABIC_DIGITS_TO_LATIN",
    "THOUSANDS_SEPARATOR_NORMALIZED",
    "KNOWN_PRICE_WORD_TO_NUMBER",
    "CURRENCY_NORMALIZED_YER",
    "PHONE_NORMALIZED_YE",
    "EMAIL_REPEATED_SYMBOLS",
    "DATE_NORMALIZED",
    "STATUS_SYNONYM_NORMALIZED",
    "ORDER_TOTAL_RECALCULATED",
]
rule_count = sum(1 for x in rule_markers if x in quality)
add("6.6", "At least eight deterministic cleaning rules",
    "PASS" if rule_count >= 8 else "FAIL",
    f"Detected {rule_count} official/expected rule markers out of {len(rule_markers)} checked.")

# 6.7
audit_ok = has_all(quality + elt, [
    "quality_status", "corrections", "field",
    "original_value", "corrected_value", "rule_code"
])
add("6.7", "Correction Audit Trail",
    "PASS" if audit_ok else "FAIL",
    "Corrected records retain field/original/corrected/rule_code."
    if audit_ok else "Audit Trail structure markers are incomplete.")

# 6.8
official_codes = [
    "MISSING_ORDER_ID",
    "MISSING_CUSTOMER_ID",
    "INVALID_IMPOSSIBLE_DATE",
    "CORRUPTED_ITEMS_JSON",
    "EMPTY_ITEMS",
    "UNKNOWN_PRICE",
    "AMBIGUOUS_NEGATIVE_VALUE",
    "DUPLICATE_ORDER_ID",
    "MULTIPLE_CONFLICTING_ERRORS",
]
codes_missing = [x for x in official_codes if x not in quality]
quarantine_ok = not codes_missing and has_all(quality + elt, ["error_codes", "error_details"])
add("6.8", "Quarantine with explicit reasons",
    "PASS" if quarantine_ok else "FAIL",
    "All nine official quarantine codes plus error_codes/error_details found."
    if quarantine_ok else "Missing: " + ", ".join(codes_missing))

# 6.9
collections_ok = has_all(settings, [
    'RAW_COLLECTION', 'VALIDATED_COLLECTION', 'QUARANTINE_COLLECTION'
]) and has_all(settings, ["orders_raw", "orders_validated", "orders_quarantine"])
add("6.9", "Required MongoDB collections",
    "PASS" if collections_ok else "FAIL",
    "orders_raw / orders_validated / orders_quarantine configured."
    if collections_ok else "Collection configuration is incomplete.")

# 6.10
upsert_ok = (
    "order_id" in elt
    and "unique" in elt.lower()
    and "upsert=True" in elt.replace(" ", "")
    and exists("reports/elt_write_report_final_idempotency.json")
    and exists("reports/upsert_update_proof.json")
)
add("6.10", "Stable business key + Unique Index + Upsert + Idempotency",
    "PASS" if upsert_ok else "FAIL",
    "order_id, unique index, upsert=True, rerun evidence and update proof found."
    if upsert_ok else "One or more idempotency/upsert evidence markers are missing.")

# 6.11 & 6.12
results = json_load("reports/results.json")
small = ((results or {}).get("runs") or {}).get("small_python_batch") or {}
large = ((results or {}).get("runs") or {}).get("large_pyspark") or {}
metric_keys = [
    "run_id", "file_name", "file_size_mb", "engine_used",
    "rows_read", "raw_loaded", "valid_count", "corrected_count",
    "quarantine_count", "elapsed_seconds", "throughput",
    "error_case_counts", "inserted_count", "updated_count", "unchanged_count",
]
metrics_ok = bool(results) and all(k in small for k in metric_keys) and all(k in large for k in metric_keys)
engine_settings_ok = ("batch_size" in small and "partitions" in large)
small_eq = (
    small.get("raw_loaded") ==
    (small.get("valid_count",0) + small.get("corrected_count",0) + small.get("quarantine_count",0))
)
large_eq = (
    large.get("raw_loaded") ==
    (large.get("valid_count",0) + large.get("corrected_count",0) + large.get("quarantine_count",0))
)
add("6.11", "Per-run consistency equation",
    "PASS" if small_eq and large_eq else "FAIL",
    f"Small equation={small_eq}; Large equation={large_eq}.")
add("6.12", "Required metrics in reports/results.json",
    "PASS" if metrics_ok and engine_settings_ok else "FAIL",
    f"Required metrics both runs={metrics_ok}; batch_size/partitions={engine_settings_ok}.")

# Code quality / tests
tests = list((ROOT / "tests").glob("test_*.py")) if (ROOT / "tests").exists() else []
resource_ok = "finally" in batch and "finally" in spark and "finally" in elt
one_command_ok = (
    "def main(" in main
    and "run_batch_engine" in main
    and "run_spark_engine" in main
    and "run_large_elt" in main
)
add("QUALITY", "Code organization, resource closing, tests, one entry point",
    "PASS" if tests and resource_ok and one_command_ok else "FAIL",
    f"Tests={len(tests)}; try/finally={resource_ok}; one-main-command={one_command_ok}.")

# Deliverables
screens = ROOT / "reports" / "screenshots"
png_count = len(list(screens.glob("*.png"))) if screens.exists() else 0
deliverables_ok = all(exists(x) for x in [
    "README.md", "requirements.txt", "reports/results.json",
    "reports/results.md", "reports/screenshots/evidence_manifest.json",
])
add("11", "Submission files and screenshot evidence",
    "PASS" if deliverables_ok and png_count >= 10 else "FAIL",
    f"Core deliverables={deliverables_ok}; screenshots={png_count}.")

# Penalty guards
all_code = "\n".join([batch, spark, main])
penalty_ok = (
    "import pandas" not in all_code.lower()
    and "list(reader)" not in all_code.replace(" ", "").lower()
    and "insert_many" in batch
    and "upsert=True" in elt.replace(" ", "")
)
add("13", "Major deduction / rejection guards",
    "PASS" if penalty_ok else "FAIL",
    "No Pandas full-load/list(reader); Batch insert_many and validated Upsert markers present."
    if penalty_ok else "Potential deduction marker detected; inspect immediately.")

# Git / GitHub
git_dir = ROOT / ".git"
git_ok = git_dir.exists()
remote = ""
if git_ok:
    try:
        p = subprocess.run(
            ["git", "-C", str(ROOT), "remote", "-v"],
            capture_output=True, text=True, check=False
        )
        remote = p.stdout.strip()
    except Exception:
        remote = ""
add("GIT", "Local Git repository",
    "PASS" if git_ok else "FAIL",
    "Local .git repository exists." if git_ok else "Git repository missing.")
add("GITHUB", "GitHub remote URL",
    "PASS" if remote else "PENDING",
    remote if remote else "Mandatory delivery item; will be completed after final fixes/commit.")

# Ensure no CSV is staged/tracked
csv_tracked = []
if git_ok:
    try:
        p = subprocess.run(
            ["git", "-C", str(ROOT), "ls-files"],
            capture_output=True, text=True, check=False
        )
        csv_tracked = [x for x in p.stdout.splitlines() if x.lower().endswith(".csv")]
    except Exception:
        pass
add("SAFETY", "No large CSV tracked in Git",
    "PASS" if not csv_tracked else "WARN",
    "No CSV files tracked." if not csv_tracked else "Tracked CSV: " + ", ".join(csv_tracked))

# Summary
counts = {s: sum(1 for c in checks if c["status"] == s)
          for s in ["PASS","FAIL","WARN","PENDING"]}

REPORTS.mkdir(parents=True, exist_ok=True)
payload = {
    "audit_scope": "Official individual core requirements",
    "database_mode": "NO DATABASE WRITE",
    "summary": counts,
    "checks": checks,
}
with (REPORTS / "final_compliance_audit.json").open("w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)

md = [
    "# Final Compliance Audit",
    "",
    "Scope: official individual core requirements.",
    "",
    f"- PASS: {counts['PASS']}",
    f"- FAIL: {counts['FAIL']}",
    f"- WARN: {counts['WARN']}",
    f"- PENDING: {counts['PENDING']}",
    "",
    "| Requirement | Status | Detail |",
    "|---|---|---|",
]
for c in checks:
    detail = c["detail"].replace("|", "/")
    md.append(f"| {c['requirement']} — {c['title']} | {c['status']} | {detail} |")
(REPORTS / "final_compliance_audit.md").write_text("\n".join(md), encoding="utf-8")

print()
print("=" * 92)
print("AUDIT SUMMARY")
print("=" * 92)
for k in ["PASS","FAIL","WARN","PENDING"]:
    print(f"{k:<8}: {counts[k]}")
print()
print("Reports:")
print(REPORTS / "final_compliance_audit.json")
print(REPORTS / "final_compliance_audit.md")
print("=" * 92)

if counts["FAIL"]:
    print("FINAL COMPLIANCE AUDIT: ACTION REQUIRED")
    sys.exit(2)

print("FINAL COMPLIANCE AUDIT: CORE PASS")
if counts["PENDING"]:
    print("Remaining pending items are delivery actions (for example GitHub remote).")
