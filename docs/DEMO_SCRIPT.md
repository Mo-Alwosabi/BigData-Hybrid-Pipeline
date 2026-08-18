# Practical Demo Script

1. Show small Router:
python -m src.main --input ".\data\samples\orders_small_sample.csv" --dry-route
Expected engine: python_batch.

2. In MongoDB Compass show orders_raw filtered by:
{"run_id":"run-20260816T195634Z-2294f5ec"}
Explain Raw-first and preserved dirty values.

3. Show:
orders_validated_100k_evidence
orders_quarantine_100k_evidence
Demonstrate Valid, Corrected with audit trail, and Quarantined with error codes.

4. Show large Router:
python -m src.main --input "D:\lec5\orders_huge_mixed_quality.csv" --dry-route
Expected engine: pyspark.

5. Show saved Spark UI screenshots: Jobs, Stages/Tasks, Executors. Mention 99 partitions.

6. Show final MongoDB counts:
orders_validated = 27,496,192
orders_quarantine = 2,503,808

7. Open reports/results.json and explain engine, rows, valid/corrected/quarantine, elapsed, throughput, batch size/partitions and error counts.

8. Open reports/elt_write_report_final_idempotency.json and explain zero duplicate insertion on rerun.

9. Open reports/upsert_update_proof.json and explain same business key updates one document without duplicate.
