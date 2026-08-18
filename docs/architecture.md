# Final Architecture

Dirty CSV
-> File Router
-> Python Batch (small <= 200 MB) OR PySpark (large > 200 MB)
-> orders_raw
-> Quality / Cleaning / Validation
-> Valid / Corrected / Quarantined
-> orders_validated / orders_quarantine
-> Metrics

## Raw-first
All records are stored in orders_raw before cleaning. Raw values are preserved as strings with run metadata.

## Small path
Python csv streaming, batch size 5,000, insert_many, no Pandas full-file load.

## Large path
SparkSession + DataFrame API + explicit String schema + MongoDB Spark Connector.
Official large run: 12,650.32 MB, 30,000,000 rows, 99 partitions, 0 corrupt CSV rows.

## Quality layer
Each Raw record is classified exactly once as Valid, Corrected, or Quarantined.
Deterministic corrections keep an audit trail. Ambiguous records are quarantined.

## Bounded memory
Existing-state lookup is limited to 2,000 keys per Raw batch instead of loading all final-state documents into RAM.

## Business key and idempotency
order_id is the stable business key. orders_validated has a unique order_id index and uses Upsert.
