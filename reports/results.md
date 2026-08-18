# Big Data Hybrid Pipeline - Results

## Final Status

Core individual assignment pipeline completed and independently verified.

## Router

- Threshold: 200 MB
- Small reproducible sample: 41.77 MB -> Python Batch
- Large source file: 12,650.32 MB -> PySpark

## Raw Loading Comparison

| Metric | Python Batch | PySpark |
|---|---:|---:|
| Records | 100,000 | 30,000,000 |
| File size | 41.77 MB | 12,650.32 MB |
| Batch size / partitions | 5,000 | 99 partitions |
| Raw load elapsed | 28.873 sec | 636.59 sec |
| Raw load throughput | 3,463.47 rows/sec | 47,126.31 rows/sec |

PySpark achieved substantially higher Raw-ingestion throughput on the large file because the CSV was processed through Spark DataFrame partitions and written using the MongoDB Spark Connector.

## Small 100K Quality Result

- Raw: 100,000
- Valid: 70,002
- Corrected: 21,697
- Quarantined: 8,301
- Consistency: PASS
- Final validated first run: 91,699
- Final quarantine first run: 8,301

## Large 30M Quality Result

- Raw: 30,000,000
- Valid: 20,994,411
- Corrected: 6,501,781
- Quarantined: 2,503,808
- Consistency: PASS
- Final validated: 27,496,192
- Final quarantine: 2,503,808

## Large Processing Performance

- Spark Raw ingestion: 636.59 sec
- Spark Raw throughput: 47,126.31 records/sec
- Quality/Classification ELT: 28965.12 sec
- Quality/Classification throughput: 1,035.73 records/sec

The quality stage is intentionally more expensive than Raw loading because every record is evaluated against deterministic cleaning, validation, duplicate, audit-trail, classification, existing-state, and Upsert/Quarantine logic.

## Reliability

- Raw-first ELT: PASS
- No silent Raw filtering: PASS
- Stable business key order_id: PASS
- Unique order_id index: PASS
- Idempotent Upsert proof: PASS
- Existing-record Update proof: PASS
- 100K evidence preserved separately: PASS
- 30M production run independently verified: PASS
