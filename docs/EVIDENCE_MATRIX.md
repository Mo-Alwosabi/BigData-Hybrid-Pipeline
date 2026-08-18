# Assignment Evidence Matrix

| Requirement | Evidence | Status |
|---|---|---|
| Small-file Router | 41.77 MB sample -> python_batch | PASS |
| Large-file Router | 12,650.32 MB -> pyspark | PASS |
| Configurable threshold | config/settings.py, 200 MB default | PASS |
| Python streaming CSV | src/batch_loader.py | PASS |
| Batch size + insert_many | 5,000 records | PASS |
| Raw-first | orders_raw before cleaning | PASS |
| Raw metadata | run_id/source/timestamp/engine/raw_record | PASS |
| Fixed Spark String schema | src/spark_loader.py | PASS |
| Spark DataFrame API | src/spark_loader.py | PASS |
| MongoDB Spark Connector | official 30M run | PASS |
| Spark partitions | 99 | PASS |
| Valid/Corrected/Quarantine | src/quality_rules.py | PASS |
| 8+ cleaning rules | deterministic rules implemented | PASS |
| Correction audit trail | field/original/corrected/rule_code | PASS |
| Official quarantine codes | quality_rules.py | PASS |
| Stable business key | order_id | PASS |
| Unique validated index | final_verification.json | PASS |
| Upsert | elt_pipeline.py | PASS |
| Idempotency | elt_write_report_final_idempotency.json | PASS |
| Update proof | upsert_update_proof.json | PASS |
| Per-run consistency | small and large PASS | PASS |
| 100K evidence | preserved collections + reports | PASS |
| 12.35GB execution | spark_large_run_final.json | PASS |
| 30M Quality ELT | elt_write_report_large_30m_final.json | PASS |
| Final metrics | reports/results.json | PASS |
| Batch vs PySpark comparison | reports/results.md | PASS |
| Automated tests | 53 passed | PASS |
| README | README.md | PASS |
| Spark UI screenshots | reports/screenshots | ATTACH |
| MongoDB Compass screenshots | reports/screenshots | ATTACH |
| Git repository | Local Git repository initialized on `main` with final project commit | PASS |
| GitHub URL | https://github.com/RAIDAN44/BigData_Hybrid_Pipeline | PASS |
