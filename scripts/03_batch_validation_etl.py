# -*- coding: utf-8 -*-
"""
03_batch_validation_etl.py
--------------------------

Purpose
-------
Validates recurring manufacturing batch-data submissions before they are
loaded into the normalized manufacturing database.

Business process
----------------
Business units submit standardized CSV files containing manufacturing
parameter data. Files are placed in:

    batch_data_uploads/

The files may contain records for:

    - New manufacturing batches
    - Existing manufacturing batches
    - New parameter results
    - Corrected values for previously submitted results

This script acts as the data-quality gate between the business-unit
submission process and the database load process.

The workflow is:

    CSV files
        |
        v
    Raw staging
        |
        +---- validation failures ----> rejected_records
        |
        v
    stg_valid_records
        |
        v
    04_batch_load_etl.py

Validation philosophy
---------------------
Validation is performed sequentially. Once a record fails a validation
stage, it is excluded from subsequent validation stages.

This provides a clear rejection category and prevents cascading errors from
causing the same record to appear to fail multiple independent business rules.

Validation includes:

    1. Batch Name
    2. Date of Manufacture
    3. Batch metadata consistency within the submitted files
    4. Consistency with existing Batch metadata
    5. Manufacturer
    6. Process
    7. Unit Operation
    8. Parameter

Reference/master data is not created by this ETL.

The submitted Manufacturer, Process, Unit Operation, and Parameter must
already exist in the database.

Existing Batch metadata is also not modified by this process.

Rejected records
----------------
Rejected records are written to:

    mfg.rejected_records

The rejected record retains:

    - Validation stage
    - Source file
    - Original row data
    - Deterministic rejection hash
    - Timestamp

This creates an audit trail that allows business-unit data-entry
discrepancies to be reviewed and corrected.

Validated records
-----------------
Records that pass all validation rules are written to:

    mfg.stg_valid_records

The next pipeline stage, 04_batch_load_etl.py, consumes only this validated
dataset.

The staging tables represent the current collection of uploaded files being
processed by the ETL run.
"""

from pathlib import Path
import duckdb
import pandas as pd


# ==============================================================
# 1. CONFIGURATION
# ==============================================================

# Determine the repository/project root from the location of this script.
#
# __file__ = scripts/03_validate_batch_data.py
# .parent  = scripts/
# .parent  = manufacturing-database-etl/

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DB_PATH = PROJECT_ROOT / "manufacturing.duckdb"
UPLOAD_FOLDER = PROJECT_ROOT / "batch_data_uploads"

REQUIRED_COLUMNS = {
    "Batch Name",
    "Manufacturer",
    "DoM",
    "Process",
    "Unit Operation",
    "Parameter",
    "Value",
}

# ==============================================================
# 2. DISCOVER SOURCE FILES
# ==============================================================

if not UPLOAD_FOLDER.exists():
    raise FileNotFoundError(
        f"Upload folder not found: {UPLOAD_FOLDER.resolve()}"
    )

csv_files = sorted(UPLOAD_FOLDER.glob("*.csv"))

if not csv_files:
    raise FileNotFoundError(
        f"No CSV files were found in {UPLOAD_FOLDER.resolve()}"
    )

print("\n==============================================")
print("SOURCE FILES")
print("==============================================")
print(f"Upload folder: {UPLOAD_FOLDER.resolve()}")
print(f"CSV files found: {len(csv_files)}")

for csv_file in csv_files:
    print(f"  - {csv_file.name}")


# ==============================================================
# 3. READ AND VALIDATE FILE STRUCTURE
# ==============================================================

dataframes = []

for csv_file in csv_files:

    file_df = pd.read_csv(csv_file)

    missing_columns = REQUIRED_COLUMNS.difference(file_df.columns)

    if missing_columns:
        raise ValueError(
            f"{csv_file.name} is missing required columns: "
            + ", ".join(sorted(missing_columns))
        )

    # Preserve the originating filename so every rejected/validated
    # record can be traced back to the source submission.
    file_df["source_file"] = csv_file.name

    # Keep DoM as text until SQL validation determines whether it is a
    # valid MM/DD/YYYY date.
    file_df["DoM"] = file_df["DoM"].astype("string")

    dataframes.append(file_df)


# Combine all discovered files into the current validation dataset.
df = pd.concat(dataframes, ignore_index=True)

print(f"Total source records: {len(df)}")


# ==============================================================
# 4. CONNECT AND BEGIN TRANSACTION
# ==============================================================

con = duckdb.connect(str(DB_PATH))

con.execute("BEGIN TRANSACTION")

try:

    # ==========================================================
    # 5. CREATE RAW STAGING TABLE
    # ==========================================================

    # The raw staging table contains every submitted row.
    #
    # is_rejected and rejection_stage are populated as validation rules
    # are applied.

    con.register("csv_df", df)

    con.execute("""
        CREATE OR REPLACE TABLE mfg.stg_manufacturing_data AS
        SELECT
            *,
            FALSE AS is_rejected,
            NULL::VARCHAR AS rejection_stage,
            CURRENT_TIMESTAMP AS load_timestamp
        FROM csv_df
    """)

    # ==========================================================
    # 6. VALIDATE BATCH NAME
    # ==========================================================

    # Batch Name is the business identifier for a manufacturing batch.
    # A blank Batch Name cannot be associated with a database Batch.

    con.execute("""
        UPDATE mfg.stg_manufacturing_data
        SET
            is_rejected = TRUE,
            rejection_stage = 'batch_name'
        WHERE is_rejected = FALSE
          AND (
              "Batch Name" IS NULL
              OR TRIM("Batch Name") = ''
          )
    """)

    # ==========================================================
    # 7. VALIDATE DATE OF MANUFACTURE
    # ==========================================================

    # Validate DoM before performing comparisons against existing batches.
    #
    # TRY_STRPTIME returns NULL for an invalid date rather than terminating
    # the ETL, allowing the row to be recorded as a controlled rejection.

    con.execute("""
        UPDATE mfg.stg_manufacturing_data
        SET
            is_rejected = TRUE,
            rejection_stage = 'dom'
        WHERE is_rejected = FALSE
          AND TRY_STRPTIME(
                TRIM(DoM),
                '%m/%d/%Y'
              ) IS NULL
    """)

    # ==========================================================
    # 8. VALIDATE BATCH METADATA WITHIN THE UPLOAD
    # ==========================================================

    # A Batch Name represents one manufacturing batch.
    #
    # Therefore, multiple rows for the same Batch Name must agree on
    # Manufacturer and Date of Manufacture.
    #
    # Example:
    #
    #     BATCH001 | Manufacturer A | 08/01/2026
    #     BATCH001 | Manufacturer A | 08/02/2026
    #
    # is invalid because the same batch has conflicting metadata.

    con.execute("""
        UPDATE mfg.stg_manufacturing_data AS s
        SET
            is_rejected = TRUE,
            rejection_stage = 'batch_metadata'
        WHERE s.is_rejected = FALSE
          AND EXISTS (
              SELECT 1
              FROM mfg.stg_manufacturing_data AS x
              WHERE x."Batch Name" = s."Batch Name"
                AND x.is_rejected = FALSE
              GROUP BY x."Batch Name"
              HAVING
                  COUNT(DISTINCT TRIM(x.DoM)) > 1
                  OR COUNT(DISTINCT TRIM(x.Manufacturer)) > 1
          )
    """)

    # ==========================================================
    # 9. VALIDATE AGAINST EXISTING BATCH METADATA
    # ==========================================================

    # New Batch Names are allowed.
    #
    # Existing Batches, however, must retain their established metadata.
    # The recurring result-load process is intended to update Result values,
    # not rewrite Batch identity information.

    con.execute("""
        UPDATE mfg.stg_manufacturing_data AS s
        SET
            is_rejected = TRUE,
            rejection_stage = 'batch_metadata'
        WHERE s.is_rejected = FALSE
          AND EXISTS (
              SELECT 1
              FROM mfg.batch AS b
              JOIN mfg.manufacturer AS m
                ON m.name = s.Manufacturer
              WHERE b.batch_name = s."Batch Name"
                AND (
                    b.dom IS DISTINCT FROM
                        TRY_STRPTIME(
                            TRIM(s.DoM),
                            '%m/%d/%Y'
                        )
                    OR
                    b.manufacturer_id IS DISTINCT FROM
                        m.manufacturer_id
                )
          )
    """)

    # ==========================================================
    # 10. VALIDATE MANUFACTURER
    # ==========================================================

    # Manufacturer is reference/master data.
    #
    # The batch ingestion process does not automatically create a new
    # Manufacturer when an unknown value is submitted. The record is
    # rejected so the reference data can be reviewed and maintained through
    # an appropriate controlled process.

    con.execute("""
        UPDATE mfg.stg_manufacturing_data AS s
        SET
            is_rejected = TRUE,
            rejection_stage = 'manufacturer'
        WHERE s.is_rejected = FALSE
          AND NOT EXISTS (
              SELECT 1
              FROM mfg.manufacturer AS m
              WHERE m.name = s.Manufacturer
          )
    """)

    # ==========================================================
    # 11. VALIDATE PROCESS
    # ==========================================================

    # Process is the first level of the manufacturing process hierarchy.

    con.execute("""
        UPDATE mfg.stg_manufacturing_data AS s
        SET
            is_rejected = TRUE,
            rejection_stage = 'process'
        WHERE s.is_rejected = FALSE
          AND NOT EXISTS (
              SELECT 1
              FROM mfg.process AS p
              WHERE p.process_name = s.Process
          )
    """)

    # ==========================================================
    # 12. VALIDATE UNIT OPERATION
    # ==========================================================

    # A Unit Operation must exist under the submitted Process.
    #
    # This is intentionally validated as a relationship rather than simply
    # checking whether the Unit Operation name exists somewhere in the
    # database.

    con.execute("""
        UPDATE mfg.stg_manufacturing_data AS s
        SET
            is_rejected = TRUE,
            rejection_stage = 'unit_operation'
        WHERE s.is_rejected = FALSE
          AND NOT EXISTS (
              SELECT 1
              FROM mfg.process AS p
              JOIN mfg.unit_operation AS u
                ON p.process_id = u.process_id
              WHERE p.process_name = s.Process
                AND u.unit_operation_name = s."Unit Operation"
          )
    """)

    # ==========================================================
    # 13. VALIDATE PARAMETER
    # ==========================================================

    # Parameter validation checks the complete manufacturing hierarchy:
    #
    #     Process
    #         |
    #         +-- Unit Operation
    #                 |
    #                 +-- Parameter
    #
    # This prevents a valid Parameter name from being accepted when it
    # belongs to a different Unit Operation.

    con.execute("""
        UPDATE mfg.stg_manufacturing_data AS s
        SET
            is_rejected = TRUE,
            rejection_stage = 'parameter'
        WHERE s.is_rejected = FALSE
          AND NOT EXISTS (
              SELECT 1
              FROM mfg.process AS p
              JOIN mfg.unit_operation AS u
                ON p.process_id = u.process_id
              JOIN mfg.parameter AS pa
                ON u.unit_operation_id = pa.unit_operation_id
              WHERE p.process_name = s.Process
                AND u.unit_operation_name = s."Unit Operation"
                AND pa.parameter_name = s.Parameter
          )
    """)

    # ==========================================================
    # 14. PERSIST REJECTED RECORDS
    # ==========================================================

    # Rejected records are preserved for review rather than simply being
    # discarded.
    #
    # raw_data retains the submitted row as JSON.
    #
    # source_file identifies the original submission.
    #
    # rejection_hash provides deterministic duplicate protection.
    #
    # The source filename is included in the hash so identical invalid
    # records submitted in different files remain separately traceable.

    con.execute("""
        INSERT INTO mfg.rejected_records (
            rejection_stage,
            source_file,
            raw_data,
            rejection_hash
        )
        SELECT
            rejection_stage,
            source_file,
            row_to_json(s),
            md5(
                COALESCE(source_file, '') || '|' ||
                COALESCE(rejection_stage, '') || '|' ||
                COALESCE("Batch Name", '') || '|' ||
                COALESCE(Manufacturer, '') || '|' ||
                COALESCE(DoM, '') || '|' ||
                COALESCE(Process, '') || '|' ||
                COALESCE("Unit Operation", '') || '|' ||
                COALESCE(Parameter, '') || '|' ||
                COALESCE(CAST(Value AS VARCHAR), '')
            )
        FROM mfg.stg_manufacturing_data AS s
        WHERE is_rejected = TRUE
        ON CONFLICT DO NOTHING
    """)

    # ==========================================================
    # 15. CREATE VALIDATED STAGING SET
    # ==========================================================

    # This table is the controlled handoff from the validation process to
    # the load process.
    #
    # 04_batch_load_etl.py does not consume raw business-unit submissions.
    # It consumes only records that passed all validation rules.

    con.execute("""
        CREATE OR REPLACE TABLE mfg.stg_valid_records AS
        SELECT *
        FROM mfg.stg_manufacturing_data
        WHERE is_rejected = FALSE
    """)

    # ==========================================================
    # 16. VALIDATION SUMMARY
    # ==========================================================

    total_records = con.execute("""
        SELECT COUNT(*)
        FROM mfg.stg_manufacturing_data
    """).fetchone()[0]

    rejected_count = con.execute("""
        SELECT COUNT(*)
        FROM mfg.stg_manufacturing_data
        WHERE is_rejected = TRUE
    """).fetchone()[0]

    valid_count = con.execute("""
        SELECT COUNT(*)
        FROM mfg.stg_valid_records
    """).fetchone()[0]

    rejection_summary = con.execute("""
        SELECT
            rejection_stage,
            COUNT(*) AS rejected_count
        FROM mfg.stg_manufacturing_data
        WHERE is_rejected = TRUE
        GROUP BY rejection_stage
        ORDER BY rejection_stage
    """).fetchdf()

    print("\n==============================================")
    print("VALIDATION SUMMARY")
    print("==============================================")
    print(f"Source files processed: {len(csv_files)}")
    print(f"Total records:          {total_records}")
    print(f"Rejected records:       {rejected_count}")
    print(f"Validated records:      {valid_count}")

    if not rejection_summary.empty:
        print("\nRejections by validation stage:")
        print(rejection_summary)

    # ==========================================================
    # 17. COMMIT VALIDATION RESULTS
    # ==========================================================

    con.execute("COMMIT")

    print("\n==============================================")
    print("BATCH VALIDATION COMPLETED")
    print("==============================================")
    print("Validated records are available in:")
    print("  mfg.stg_valid_records")
    print("Rejected records are available in:")
    print("  mfg.rejected_records")
    print("==============================================")

except Exception:

    # If validation fails unexpectedly, roll back the entire transaction.
    #
    # This prevents a partially rebuilt staging dataset or partially
    # persisted rejection set from being treated as a successful ETL run.

    con.execute("ROLLBACK")

    print("\nERROR: Batch validation ETL failed.")
    print("Transaction rolled back. No validation changes were saved.")

    raise

finally:

    con.close()