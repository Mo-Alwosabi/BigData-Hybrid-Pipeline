Hybrid Big Data Pipeline

A production-oriented hybrid Big Data pipeline for processing large-scale, mixed-quality order data using Python, PySpark, and MongoDB.

The pipeline automatically selects the appropriate processing engine according to the input file size, ingests source data into a raw layer, performs data-quality validation and correction, detects duplicates, and writes processed records into validated and quarantine collections.

Features
Automatic file-size based engine selection
Python batch processing for smaller datasets
PySpark processing for large datasets
Raw-first data ingestion
Explicit CSV schema handling
Data-quality validation
Automatic data correction
Quarantine for unrecoverable records
Duplicate detection
MongoDB bulk writes
Business-key based upserts
Idempotent processing
Record fingerprinting
Data lineage tracking
Processing metadata
Consistency validation
Performance measurement
JSON execution reports
Large-scale Spark processing
Architecture

The pipeline follows a hybrid processing architecture.

Input CSV files are first analyzed by the file router.

Small files are processed using the Python batch engine.

Large files are automatically routed to PySpark.

Both processing paths write the original records into the MongoDB raw layer.

The ELT stage then classifies each record as valid, corrected, or quarantined.

Validated records are stored in orders_validated.

Unrecoverable records are stored in orders_quarantine.

The original records remain available in orders_raw for traceability and reprocessing.

Processing Flow

Input CSV

File Router

Engine Selection

Python Batch or PySpark

orders_raw

Data Quality Processing

Validation and Correction

Duplicate Detection

Classification

Valid Records

Corrected Records

Quarantined Records

MongoDB

orders_validated

orders_quarantine

Engine Selection

The pipeline uses a configurable file-size threshold to determine the processing engine.

The configured threshold is 200 MB.

Files larger than the threshold are automatically routed to PySpark.

Example:

python -m src.main --input "path\to\orders.csv"

The router reports the selected engine and the reason for the selection.

Large-Scale Processing

Large datasets are processed using Apache Spark through PySpark.

The large-data execution path uses Spark 4.0.1 and Scala 2.13.

The Spark execution mode is local[*], allowing Spark to use the available local CPU resources.

Large CSV files are read using an explicit String schema.

This prevents unwanted schema inference and provides predictable handling of mixed-quality source data.

Large-file caching is disabled to avoid unnecessary memory consumption.

Raw-First Data Architecture

The pipeline follows a raw-first architecture.

Incoming records are first written to the orders_raw collection.

The raw layer preserves the original record together with ingestion metadata.

Each raw record can contain:

run_id

source_file

source_path

source_row_number

ingested_at

engine_used

raw_record

This provides traceability between the original source data and all subsequent processing stages.

Data Quality Classification

Every source record is classified into one of three quality states.

VALID

The record satisfies the defined quality rules without requiring modifications.

CORRECTED

The record contains recoverable data-quality problems that can be safely corrected.

QUARANTINED

The record contains an unrecoverable or conflicting data-quality problem and is moved to the quarantine collection.

The classification process ensures that problematic records are not silently discarded.

Data Correction

The pipeline automatically applies defined correction rules when enough information is available to safely repair a record.

Supported correction categories include:

Arabic digit normalization
Decimal separator normalization
Phone number normalization
Date normalization
Email normalization
Whitespace trimming
Order total recalculation
Currency normalization
Thousands separator normalization
Known price-word conversion
Item total derivation
Item price derivation
Negative quantity correction
Status synonym normalization

Every correction is recorded in the processed document.

The original raw data remains available in the raw layer.

Quarantine

Records that cannot be safely repaired are stored in orders_quarantine.

Quarantine records preserve:

Original raw record
Cleaned preview
Corrections
Error codes
Error details
Source run ID
Source file
Source path
Source row number
Raw ingestion timestamp
Processing run ID
Processing timestamps

This allows failed records to be inspected and potentially reprocessed later.

Error Detection

The pipeline tracks explicit quality error codes.

Examples include:

MISSING_ORDER_ID

MISSING_CUSTOMER_ID

EMAIL_INVALID_UNRECOVERABLE

PHONE_INVALID_UNRECOVERABLE

CORRUPTED_ITEMS_JSON

EMPTY_ITEMS

TOTAL_UNKNOWN_UNRECOVERABLE

INVALID_IMPOSSIBLE_DATE

STATUS_UNKNOWN

CURRENCY_UNKNOWN

DUPLICATE_ORDER_ID

MULTIPLE_CONFLICTING_ERRORS

Error statistics are included in the generated reports.

Duplicate Detection

Duplicate order IDs are detected before the final classification stage.

The pipeline identifies duplicate groups and duplicate records.

Conflicting duplicate order IDs can be quarantined instead of being incorrectly inserted as separate business entities.

This protects the validated collection from conflicting business-key records.

MongoDB Collections

The pipeline uses three primary MongoDB collections.

orders_raw

Contains the original ingested source records and ingestion metadata.

orders_validated

Contains records classified as valid or corrected.

orders_quarantine

Contains records that cannot safely be validated.

Upsert Strategy

The validated collection uses order_id as the primary business key.

MongoDB upsert operations are used to insert new records or update existing records.

This prevents duplicate business entities from being created when the same order is processed multiple times.

Bulk unordered writes are used to improve MongoDB write performance.

Idempotent Processing

The pipeline supports idempotent processing.

Records are assigned stable fingerprints based on their relevant data state.

If an existing record has the same fingerprint, the record is treated as unchanged.

If the business key already exists but the payload changes, the existing document is updated rather than creating another document.

The project includes an isolated upsert/update verification demonstrating that the same order ID can be updated without creating a duplicate document.

Data Lineage

Validated records contain lineage information connecting them to their original raw source.

Lineage information includes:

raw_run_id

source_file

source_path

source_row_number

raw_ingested_at

engine_used

Processing metadata also includes:

last_processing_run_id

first_processed_at

last_updated_at

This allows processed records to be traced back to their source ingestion run.

Consistency Validation

The pipeline performs multiple consistency checks before completing a processing run.

The primary classification relationship is:

Raw Records = Valid Records + Corrected Records + Quarantined Records

The pipeline also validates:

Raw record count
Classified record count
Validated collection count
Quarantine count
MongoDB upsert counts
Duplicate statistics
Processing consistency
Write consistency

These checks provide an additional integrity layer around the data-processing workflow.

Large Dataset Results

The large dataset contains 30,000,000 records.

The input file size is approximately 12.65 GB.

The recorded large-scale classification contains:

20,994,411 valid records

6,501,781 corrected records

2,503,808 quarantined records

The classification equation is:

20,994,411 + 6,501,781 + 2,503,808 = 30,000,000

The large-file Spark ingestion stage achieved approximately 125,318 records per second in the recorded execution.

The Spark input used 99 partitions and the output used 99 partitions.

Performance

The project records execution performance for both processing engines.

Performance information includes:

Total execution time
Records processed
Throughput
Input partitions
Output partitions
MongoDB write time

This allows the two processing approaches to be evaluated using measurable execution results.

Reports

The reports directory contains execution and verification artifacts.

Important reports include:

classification_dry_run.json

elt_write_report.json

elt_write_report_final_idempotency.json

elt_write_report_first_run.json

elt_write_report_large_30m_final.json

final_compliance_audit.json

final_compliance_audit.md

final_verification.json

results.json

results.md

spark_large_run.json

spark_large_run_final.json

spark_loader_test.json

upsert_update_proof.json

These reports provide machine-readable evidence of processing results, data-quality classification, performance, MongoDB writes, and consistency checks.

Project Structure

BigData_Hybrid_Pipeline

config

settings.py

src

main.py

elt_pipeline.py

reports

classification_dry_run.json

elt_write_report.json

elt_write_report_final_idempotency.json

elt_write_report_first_run.json

elt_write_report_large_30m_final.json

final_compliance_audit.json

final_compliance_audit.md

final_verification.json

results.json

results.md

spark_large_run.json

spark_large_run_final.json

spark_loader_test.json

upsert_update_proof.json

requirements.txt

README.md

Installation

Clone the repository.

Install Python 3.12 or later.

Install a compatible Java JDK.

Install MongoDB.

Create and activate a Python virtual environment.

Install the project dependencies using:

pip install -r requirements.txt

Verify Java installation using:

java -version

Verify Python installation using:

python --version

Configuration

MongoDB configuration is controlled through the project configuration.

The main configuration file is:

config/settings.py

The MongoDB URI can be configured using the MONGO_URI environment variable.

The database name can be configured using the MONGO_DATABASE environment variable.

Example values:

MONGO_URI=mongodb://127.0.0.1:27017

MONGO_DATABASE=bigdata_midterm

Running the Pipeline

To run automatic engine selection:

python -m src.main --input "path\to\orders.csv"

The system automatically determines whether Python or PySpark should process the input.

To run raw ingestion only:

python -m src.main --input "path\to\orders.csv" --raw-only

This performs the raw ingestion stage without starting the ELT processing stage.

To process a specific raw run:

python -m src.elt_pipeline --raw-run-id "<RAW_RUN_ID>"

For an explicitly selected large production run:

python -m src.elt_pipeline --raw-run-id "<RAW_RUN_ID>" --skip-dry-run-contract

Technology Stack

Python

PySpark

Apache Spark

MongoDB

PyMongo

MongoDB Spark Connector

PowerShell

JSON

Design Principles

The project is designed around the following principles:

Hybrid processing for different dataset sizes.
Raw-first data ingestion.
Safe handling of mixed-quality data.
Explicit data-quality classification.
Recoverable data correction.
Quarantine instead of silent deletion.
Duplicate detection.
Business-key based upserts.
Idempotent processing.
Record fingerprinting.
Complete data lineage.
Bounded memory usage.
Efficient bulk database writes.
Consistency validation.
Reproducible execution reports.
Performance measurement.