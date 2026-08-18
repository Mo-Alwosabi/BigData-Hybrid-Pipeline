# Viva Notes

## Why Hybrid?
Python Batch is simple for small files. PySpark is selected for large files because Spark partitions data and processes/writes partitions in parallel.

## Why Raw-first?
To preserve every dirty source record before cleaning and prevent silent data loss.

## Why strings in Raw?
Malformed dates, prices, phones and currencies must survive ingestion exactly as received.

## Why order_id?
It is the stable business identifier in the supplied dataset.

## What is Upsert?
Update an existing business-key document if present; otherwise insert it.

## What is idempotency?
Repeating the same logical processing does not create duplicate final-state records.

## What is Quarantine?
Records that cannot be corrected safely are preserved separately with explicit reasons.

## Why bounded memory?
The 30M run cannot safely materialize all final state in Python RAM. The ELT looks up only 2,000 keys per batch.

## Spark terms
SparkSession: entry point for Spark.
DataFrame: distributed table-like dataset.
Partition: independent chunk of data.
Job: action-triggered Spark work.
Stage: execution phase inside a Job.
Task: unit that processes one partition.

## Numbers to know
Small: 100,000 Raw; 70,002 Valid; 21,697 Corrected; 8,301 Quarantined.
Large: 30,000,000 Raw; 20,994,411 Valid; 6,501,781 Corrected; 2,503,808 Quarantined.
Final validated: 27,496,192.
Spark: 12,650.32 MB; 99 partitions; 0 corrupt rows; 47,126.31 Raw records/sec.

## Engineering Decision Questions

**Why 200 MB?**
It is a configurable project threshold, not a universal constant. It separates the 41.77 MB sample from the 12.35 GB real file, avoids Spark startup overhead for small input, and routes large input to partitioned Spark processing.

**Why Raw-first?**
To preserve every source record before interpretation, maintain traceability, and prevent silent loss of dirty data.

**Why keep raw fields as String in Spark?**
So malformed numeric/date/currency values remain exactly recoverable instead of being coerced or lost during ingestion.

**Why correct some records and quarantine others?**
Only deterministic corrections are safe. If a correction cannot be derived unambiguously, the record is quarantined with explicit reasons.

**Why order_id + Upsert?**
order_id is the stable business key. A unique index plus Upsert provides a repeatable final state and prevents duplicate business records.

**Why no repartition?**
The real file already produced 99 partitions. Adding repartition without a demonstrated need would add an unnecessary shuffle.

**Why was the 30M quality stage slower than raw Spark ingestion?**
It performs much more per-record work and final-state database operations. The recorded long run also included a workstation sleep/resume interval, so the wall-clock rate is not a clean direct benchmark against raw ingestion.
