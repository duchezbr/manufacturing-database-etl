# -*- coding: utf-8 -*-
"""
04_batch_load_etl.py
--------------------

Purpose
-------
Loads validated manufacturing batch records into the normalized DuckDB
database.

This is the final step in the recurring batch ETL process.

The preceding validation script, 03_batch_validation_etl.py, acts as the
data-quality gate and creates:

    mfg.stg_valid_records

This script consumes only that validated staging dataset.

Load responsibilities
---------------------
The load process supports three scenarios:

    1. New Batch
       A Batch Name does not yet exist in mfg.batch.

    2. New Result
       A Batch + Parameter combination does not yet exist in mfg.result.

    3. Updated Result
       A Batch + Parameter combination already exists, but the submitted
       Value differs from the stored Value.

Existing Batch metadata is intentionally not updated by this script.
Changes to Batch metadata are handled as validation discrepancies in the
preceding ETL stage.

Business key
------------
A Result is uniquely identified by:

    Batch + Parameter

This corresponds to:

    batch_id + parameter_id

in the normalized database.

Transaction control
-------------------
All database changes occur within a single transaction.

COMMIT_LOAD controls whether the transaction is permanently saved:

    False
        Execute the load and then ROLLBACK.

    True
        Execute the load and COMMIT the changes.

Keeping COMMIT_LOAD=False allows the complete load logic to be tested and
reviewed without permanently modifying the database.

If an unexpected exception occurs, the entire transaction is rolled back to
prevent a partially completed load.
"""

from pathlib import Path
import duckdb


# ==============================================================
# 1. CONFIGURATION
# ==============================================================

# Determine the repository/project root from the location of this script.
#
# __file__ = scripts/04_batch_load_etl.py
# .parent  = scripts/
# .parent  = manufacturing-database-etl/

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DB_PATH = PROJECT_ROOT / "manufacturing.duckdb"

# --------------------------------------------------------------
# LOAD CONTROL
# --------------------------------------------------------------
#
# False = execute the load and ROLLBACK.
# True  = execute the load and COMMIT permanently.
#
# During development/testing, leave this set to False.

COMMIT_LOAD = False


# ==============================================================
# 2. CONNECT AND BEGIN TRANSACTION
# ==============================================================

con = duckdb.connect(DB_PATH)

con.execute("BEGIN TRANSACTION")

try:

    # ==========================================================
    # 3. VERIFY VALIDATED STAGING DATA
    # ==========================================================

    # This table is populated by 03_batch_validation_etl.py.
    #
    # The load process intentionally does not perform the source-data
    # validation itself. Separating validation from loading provides a
    # clear control boundary:
    #
    #     Business-unit files
    #             |
    #             v
    #       Validation ETL
    #             |
    #             v
    #     stg_valid_records
    #             |
    #             v
    #         Load ETL
    #
    # Only validated records should reach this point.

    staging_count = con.execute("""
        SELECT COUNT(*)
        FROM mfg.stg_valid_records
    """).fetchone()[0]

    print(
        f"Validated staging records available: {staging_count}"
    )

    if staging_count == 0:
        print(
            "WARNING: No validated records are available for loading."
        )

    # ==========================================================
    # 4. CHECK DUPLICATE BATCH/PARAMETER INPUT
    # ==========================================================

    # The database permits only one Result for each:
    #
    #     Batch + Parameter
    #
    # Therefore, multiple submitted records for the same combination
    # represent an ambiguous input condition.
    #
    # Rather than allowing the outcome to depend on row ordering, the
    # load stops and requires the duplicate submission to be resolved.

    duplicate_results = con.execute("""
        SELECT
            "Batch Name",
            Process,
            "Unit Operation",
            Parameter,
            COUNT(*) AS record_count
        FROM mfg.stg_valid_records
        GROUP BY
            "Batch Name",
            Process,
            "Unit Operation",
            Parameter
        HAVING COUNT(*) > 1
        ORDER BY
            "Batch Name",
            Process,
            "Unit Operation",
            Parameter
    """).fetchdf()

    if not duplicate_results.empty:

        print(
            "\nERROR: Duplicate Batch/Parameter combinations found:"
        )

        print(duplicate_results)

        raise ValueError(
            "Duplicate Batch/Parameter combinations exist in "
            "mfg.stg_valid_records. Load stopped."
        )

    # ==========================================================
    # 5. INSERT NEW BATCHES
    # ==========================================================

    # Script 03 has already validated the Batch metadata.
    #
    # New Batch Names are inserted here.
    #
    # Existing Batch records are intentionally not updated. The purpose of
    # this recurring load is to add new batches and maintain Result values,
    # while protecting established Batch metadata.

    con.execute("""
        INSERT INTO mfg.batch (
            batch_name,
            dom,
            manufacturer_id,
            created_at,
            modified_at
        )
        SELECT DISTINCT
            s."Batch Name",
            TRY_STRPTIME(
                TRIM(s.DoM),
                '%m/%d/%Y'
            ),
            m.manufacturer_id,
            s.load_timestamp,
            s.load_timestamp
        FROM mfg.stg_valid_records AS s
        JOIN mfg.manufacturer AS m
            ON s.Manufacturer = m.name
        WHERE NOT EXISTS (
            SELECT 1
            FROM mfg.batch AS b
            WHERE b.batch_name = s."Batch Name"
        )
    """)

    # ==========================================================
    # 6. UPDATE EXISTING RESULTS
    # ==========================================================

    # Results are identified by:
    #
    #     batch_id + parameter_id
    #
    # If the incoming Value differs from the existing Value, the stored
    # Result is updated and modified_at is refreshed.
    #
    # IS DISTINCT FROM is used because it performs NULL-safe comparison.
    #
    # Examples:
    #
    #     10  -> 20     UPDATE
    #     10  -> 10     No change
    #     NULL -> 10    UPDATE
    #     10  -> NULL   UPDATE
    #     NULL -> NULL  No change

    con.execute("""
        UPDATE mfg.result AS r
        SET
            value = s.Value,
            modified_at = s.load_timestamp
        FROM mfg.stg_valid_records AS s
        JOIN mfg.batch AS b
            ON s."Batch Name" = b.batch_name
        JOIN mfg.process AS pr
            ON s.Process = pr.process_name
        JOIN mfg.unit_operation AS u
            ON u.process_id = pr.process_id
           AND u.unit_operation_name = s."Unit Operation"
        JOIN mfg.parameter AS p
            ON p.unit_operation_id = u.unit_operation_id
           AND p.parameter_name = s.Parameter
        WHERE r.batch_id = b.batch_id
          AND r.parameter_id = p.parameter_id
          AND r.value IS DISTINCT FROM s.Value
    """)

    # ==========================================================
    # 7. INSERT NEW RESULTS
    # ==========================================================

    # If a Batch + Parameter combination does not already exist, insert
    # it as a new Result.
    #
    # The joins resolve the source business identifiers to the surrogate
    # keys used by the normalized database.

    con.execute("""
        INSERT INTO mfg.result (
            batch_id,
            parameter_id,
            value,
            created_at,
            modified_at
        )
        SELECT
            b.batch_id,
            p.parameter_id,
            s.Value,
            s.load_timestamp,
            s.load_timestamp
        FROM mfg.stg_valid_records AS s
        JOIN mfg.batch AS b
            ON s."Batch Name" = b.batch_name
        JOIN mfg.process AS pr
            ON s.Process = pr.process_name
        JOIN mfg.unit_operation AS u
            ON u.process_id = pr.process_id
           AND u.unit_operation_name = s."Unit Operation"
        JOIN mfg.parameter AS p
            ON p.unit_operation_id = u.unit_operation_id
           AND p.parameter_name = s.Parameter
        WHERE NOT EXISTS (
            SELECT 1
            FROM mfg.result AS r
            WHERE r.batch_id = b.batch_id
              AND r.parameter_id = p.parameter_id
        )
    """)

    # ==========================================================
    # 8. DATABASE COUNTS
    # ==========================================================

    # These counts provide a simple post-load verification of the database
    # state before the transaction is committed or rolled back.

    batch_count = con.execute("""
        SELECT COUNT(*)
        FROM mfg.batch
    """).fetchone()[0]

    result_count = con.execute("""
        SELECT COUNT(*)
        FROM mfg.result
    """).fetchone()[0]

    print("\n==============================================")
    print("LOAD SUMMARY")
    print("==============================================")
    print(f"Total batches in database: {batch_count}")
    print(f"Total results in database: {result_count}")

    # ==========================================================
    # 9. REVIEW CURRENT VALIDATED LOAD
    # ==========================================================

    # Display results associated with the current staging load.
    #
    # This provides a simple verification view before the transaction is
    # committed.

    loaded_results = con.execute("""
        SELECT
            b.batch_name,
            b.dom,
            m.name AS manufacturer,
            pr.process_name AS process,
            u.unit_operation_name AS unit_operation,
            p.parameter_name AS parameter,
            r.value,
            r.created_at,
            r.modified_at
        FROM mfg.result AS r
        JOIN mfg.batch AS b
            ON r.batch_id = b.batch_id
        JOIN mfg.manufacturer AS m
            ON b.manufacturer_id = m.manufacturer_id
        JOIN mfg.parameter AS p
            ON r.parameter_id = p.parameter_id
        JOIN mfg.unit_operation AS u
            ON p.unit_operation_id = u.unit_operation_id
        JOIN mfg.process AS pr
            ON u.process_id = pr.process_id
        WHERE b.batch_name IN (
            SELECT DISTINCT
                "Batch Name"
            FROM mfg.stg_valid_records
        )
        ORDER BY
            b.batch_name,
            pr.process_name,
            u.unit_operation_name,
            p.parameter_name
    """).fetchdf()

    print("\nResults associated with current validated load:")
    print(loaded_results)

    # ==========================================================
    # 10. COMMIT OR ROLLBACK
    # ==========================================================

    # If COMMIT_LOAD is True, permanently save all changes.
    #
    # If COMMIT_LOAD is False, roll back all changes.
    #
    # During development/testing, leave this as False.

    if COMMIT_LOAD:

        con.execute("COMMIT")

        print()
        print("LOAD COMMITTED.")
        print("Database changes have been permanently saved.")

    else:

        con.execute("ROLLBACK")

        print()
        print("LOAD ROLLED BACK.")
        print("No database changes were saved.")

except Exception:

    # ==========================================================
    # ERROR HANDLING
    # ==========================================================

    # If any error occurs during the load, roll back the entire
    # transaction.
    #
    # This prevents a partial load where some batches or results were
    # changed before the error occurred.

    con.execute("ROLLBACK")

    print()
    print("ERROR: Load failed.")
    print(
        "Transaction rolled back. "
        "No database changes were saved."
    )

    raise

finally:

    # ==========================================================
    # CLOSE DATABASE CONNECTION
    # ==========================================================

    # Always close the DuckDB connection whether the load succeeds
    # or fails.

    con.close()