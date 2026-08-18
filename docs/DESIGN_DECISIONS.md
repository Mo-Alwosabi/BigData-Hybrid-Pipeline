# Design Decisions and Engineering Rationale

This document explains the main engineering decisions in the Hybrid Big Data Data-Quality Pipeline.
It is intended to make the implementation auditable and viva-ready.

## 1. Why a Hybrid Pipeline?

The input files can vary greatly in size. Using one engine for every file is unnecessary.

- Small files are routed to the Python Batch engine.
- Large files are routed to PySpark.
- The router is the single entry point and selects the engine automatically.

This keeps small-file execution simple while allowing large files to use Spark's partitioned processing model.

## 2. Why is the routing threshold 200 MB?

The 200 MB value is a configurable engineering threshold, not a universal performance law.

It was selected as a practical boundary for this assignment because:

- the official small sample is about 41.77 MB, clearly below the threshold;
- the real large file is about 12,650.32 MB (12.35 GB), clearly above it;
- Python streaming Batch avoids Spark startup overhead for small files;
- Spark becomes more appropriate as file size and partitioned processing needs increase.

The threshold is stored in configuration / environment settings so it can be changed for a different machine or workload without changing pipeline logic.

## 3. Why Python Batch for small files?

The Batch path uses the standard Python csv reader in streaming mode.

Reasons:

- it does not load the full CSV into memory;
- batch size is configurable;
- insert_many reduces database round trips compared with one insert per record;
- it has lower startup overhead than Spark for small files;
- batch progress, elapsed time, and throughput can be measured directly.

The implementation intentionally avoids Pandas and list(reader) for the ingestion path.

## 4. Why PySpark for large files?

The large-file path uses SparkSession and the DataFrame API with an explicit schema.

Reasons:

- the file is processed as partitions rather than as one in-memory Python object;
- Spark can parallelize CSV parsing and connector writes;
- the MongoDB Spark Connector provides the database integration required by the assignment;
- partition count and Spark UI provide observable execution evidence.

In the official large run:

- rows read: 30,000,000
- input partitions: 99
- output partitions: 99
- corrupt CSV rows: 0
- MongoDB run rows: 30,000,000

No unnecessary repartition step is used because the existing partitioning was sufficient and an extra repartition would introduce an avoidable shuffle.

## 5. Why use a fixed schema and keep raw fields as String?

Dirty data should be preserved before interpretation.

If Spark inferred numeric/date types during ingestion, malformed values could become null, fail parsing, or lose their exact original form before the quality rules see them.

Therefore the raw schema keeps source fields as String. Parsing and normalization happen later in the ELT quality stage.

This supports traceability and prevents silent data loss.

## 6. Why Raw-first ELT?

Every input record is first written to the raw collection before cleaning/classification.

The raw layer preserves:

- run_id
- source information
- ingestion timestamp
- engine used
- raw record
- source row information when it can be represented reliably

This provides a permanent ingestion trace and allows the original dirty value to be inspected after cleaning.

The pipeline never silently drops a bad record.

## 7. Why only deterministic corrections?

A value is corrected only when the corrected result can be derived by a deterministic rule from available evidence.

Examples include:

- Arabic digits to Latin digits;
- thousands / decimal separator normalization;
- known price words;
- currency normalization;
- phone normalization;
- repeated email-symbol repair;
- date normalization;
- whitespace / status synonym normalization;
- total or item component derivation when the necessary components are valid.

If a safe deterministic correction cannot be established, the record is quarantined instead of guessed.

This protects data quality from speculative transformations.

## 8. Why keep a correction Audit Trail?

A corrected record stores the correction history, including:

- field
- original_value
- corrected_value
- rule_code
- details

This makes every automatic correction explainable and reviewable.

It also allows the project to demonstrate exactly why a record was classified as Corrected rather than Valid.

## 9. Why Quarantine instead of deleting invalid records?

Quarantine preserves non-correctable records together with explicit error codes and details.

This is preferable to deletion because:

- no source record disappears silently;
- the reason for rejection remains auditable;
- data can be inspected or repaired later;
- per-run accounting remains complete.

The core consistency rule is:

raw_count = valid_count + corrected_count + quarantine_count

The project verifies this equation for both the 100K evidence run and the 30M production-scale run.

## 10. Why use order_id as the business key?

The assignment dataset defines order_id as the stable business identifier.

The validated collection therefore enforces a unique index on order_id.

This prevents duplicate final business records and gives the Upsert operation a stable key.

Raw ingestion remains run-oriented, while idempotency is enforced on the final validated state.

## 11. Why Upsert and Idempotency?

The same data pipeline may be executed again.

Using Upsert allows the final state to be repeatable:

- new order_id -> insert;
- existing unchanged order_id -> unchanged;
- existing changed order_id -> update.

The preserved 100K evidence demonstrates an idempotent rerun with no duplicate validated records.
A separate update-proof run demonstrates that an existing order can be updated without increasing the number of business records.

## 12. Why bounded-memory state lookup in the 30M quality stage?

The quality stage must compare final-state keys without loading the entire validated/quarantine database state into Python memory.

A bounded lookup strategy is used so only manageable batches of state are held at a time.

This keeps memory consumption controlled and makes the quality stage scalable beyond the small sample.

## 13. Why are raw-ingestion and quality-ELT throughputs very different?

The two timings measure different workloads.

The Spark raw-ingestion path mainly performs partitioned CSV reading, raw metadata construction, and connector writing.
The quality ELT performs much more work per record: rule evaluation, classification, audit construction, business-key handling, and MongoDB final-state operations.

Also, the recorded long 30M quality run included a workstation sleep/resume interval, so its wall-clock throughput should not be treated as a clean apples-to-apples benchmark against the Spark raw-ingestion rate.

The important correctness result is that the run resumed and finished with the exact consistency equation passing.

## 14. Why not use HDFS or YARN?

The individual core assignment does not require an HDFS/YARN cluster.
The required Spark path can be demonstrated locally using PySpark, Spark UI, partitions, and the MongoDB Spark Connector.

Adding HDFS/YARN would increase operational complexity without improving compliance with the individual core requirements.

## 15. Evidence-backed final results

### 100K evidence run

- Raw: 100,000
- Valid: 70,002
- Corrected: 21,697
- Quarantined: 8,301
- Equation: PASS

### 30M run

- Raw: 30,000,000
- Valid: 20,994,411
- Corrected: 6,501,781
- Quarantined: 2,503,808
- Validated final: 27,496,192
- Quarantine final: 2,503,808
- Equation: PASS
- Unique order_id index: PASS

### Large PySpark raw ingestion

- File size: about 12.35 GB
- Rows read: 30,000,000
- MongoDB run rows: 30,000,000
- CSV corrupt rows: 0
- Input partitions: 99
- Output partitions: 99
- Raw-ingestion throughput: about 47,126 records/second

These figures are preserved in the reports directory and supported by screenshots from MongoDB Compass, Spark UI, and the runtime console.
