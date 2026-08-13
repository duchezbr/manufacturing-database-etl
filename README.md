# Manufacturing Database ETL

## Project Overview

This project demonstrates the design and implementation of a normalized
manufacturing database and Python-based ETL pipeline using DuckDB, Python,
Pandas, and SQL.

The project is designed to demonstrate data engineering concepts that are
applicable to manufacturing and laboratory data environments, including:

- Relational database design
- Data normalization
- Primary and foreign key relationships
- Surrogate key management
- Historical data migration
- Incremental batch data ingestion
- Data validation and quality controls
- Staging tables
- Rejected-record auditing
- Insert and update logic
- Transaction management
- Source-file traceability
- SQL-based data transformation
- Python-based ETL orchestration

The project represents a simplified business process in which manufacturing
business units periodically submit standardized CSV files containing
manufacturing batch parameter data. The submitted records are validated
against established manufacturing reference data before being loaded into the
database.

---

# Business Scenario

Manufacturing business units are responsible for entering and submitting
process parameter data associated with manufacturing batches.

A submitted record contains information such as:

- Batch Name
- Manufacturer
- Date of Manufacture (DoM)
- Process
- Unit Operation
- Parameter
- Value

The business units submit standardized CSV files to an upload directory.

The files may contain data for:

- New manufacturing batches
- Existing manufacturing batches
- New parameter results
- Corrections or updates to previously submitted result values

The ETL process must therefore distinguish between valid and invalid records
and support both new data and changes to previously loaded results.

The process is intentionally separated into validation and loading stages so
that invalid business-unit submissions do not enter the production
manufacturing tables.

---

# ETL Architecture

The project consists of four sequential Python scripts.

```text
                    Historical Source Data
                             |
                             v
                  +-----------------------+
                  | 01_create_database.py |
                  | Database / Schema     |
                  | Setup                 |
                  +-----------+-----------+
                              |
                              v
                  +-----------------------+
                  | 02_initial_data_load  |
                  | Historical Migration  |
                  +-----------+-----------+
                              |
                              v
                     Manufacturing DB
                              ^
                              |
                    Recurring Batch Data
                              |
                  +-----------+-----------+
                  | batch_data_uploads/   |
                  |                        |
                  | timestamped CSV files |
                  +-----------+------------+
                              |
                              v
                  +-----------------------+
                  | 03_batch_validation   |
                  | ETL                    |
                  +-----------+-----------+
                              |
                +-------------+-------------+
                |                           |
                v                           v
        Rejected Records            Validated Records
        mfg.rejected_records        mfg.stg_valid_records
                                            |
                                            v
                              +-------------+-------------+
                              | 04_batch_load_etl.py      |
                              |                            |
                              | Insert new batches        |
                              | Update changed results    |
                              | Insert new results        |
                              +-------------+-------------+
                                            |
                                            v
                                  Manufacturing DB
