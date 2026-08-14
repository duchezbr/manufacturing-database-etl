# -*- coding: utf-8 -*-
"""
02_initial_data_load.py
-----------------------

Purpose
-------
Performs the initial migration of historical manufacturing data from a
consolidated flat file into the normalized DuckDB manufacturing database.

This script represents the initial database population step that would
typically occur before recurring incremental batch submissions are processed.

Source
------
    mock_historical_data.csv

Expected source columns
-----------------------
    Batch Name
    Manufacturer
    DoM
    Process
    Unit Operation
    Parameter
    Value

Migration approach
------------------
The historical source is intentionally represented as a flat file containing
business-friendly identifiers.

The migration separates that source into the normalized database structure:

    Manufacturer
    Process
    Unit Operation
    Parameter
    Batch
    Result

Business identifiers from the source file are used to resolve the appropriate
surrogate keys before transactional Result records are inserted.

For example:

    Process + Unit Operation
        -> unit_operation_id

    Unit Operation + Parameter
        -> parameter_id

    Batch Name
        -> batch_id

The final Result table therefore stores relationships through foreign keys
rather than repeatedly storing descriptive manufacturing metadata.

This script is designed to be safely rerunnable. Existing reference and
transactional records are not duplicated because the inserts check for
existing business keys.

A transaction is used so that a failure during migration does not leave the
database partially populated.
"""

from pathlib import Path
import duckdb
import pandas as pd


# ==============================================================
# 1. CONFIGURATION
# ==============================================================

# Determine the repository/project root from the location of this script.
#
# __file__ = scripts/02_load_historical_data.py
# .parent  = scripts/
# .parent  = manufacturing-database-etl/

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DB_PATH = PROJECT_ROOT / "manufacturing.duckdb"
SOURCE_FILE = PROJECT_ROOT / "historical_data" / "mock_historical_data.csv"

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
# 2. LOAD SOURCE DATA
# ==============================================================

if not SOURCE_FILE.exists():
    raise FileNotFoundError(
        f"Source file not found: {SOURCE_FILE.resolve()}"
    )

df = pd.read_csv(SOURCE_FILE)

missing_columns = REQUIRED_COLUMNS.difference(df.columns)

if missing_columns:
    raise ValueError(
        "Source file is missing required columns: "
        + ", ".join(sorted(missing_columns))
    )

# Keep DoM as text during extraction so that the database load controls
# conversion to DATE using the same MM/DD/YYYY format expected by the
# recurring validation/load pipeline.

df["DoM"] = df["DoM"].astype("string").str.strip()

print(f"Source file: {SOURCE_FILE.resolve()}")
print(f"Source records: {len(df)}")


# ==============================================================
# 3. CONNECT AND BEGIN TRANSACTION
# ==============================================================

con = duckdb.connect(str(DB_PATH))

con.execute("BEGIN TRANSACTION")

try:

    # ==========================================================
    # 4. LOAD MANUFACTURERS
    # ==========================================================

    # Extract the unique manufacturer values from the flat source.
    # Manufacturer is reference/master data and is therefore loaded
    # independently of individual batches or results.

    manufacturers = (
        df[["Manufacturer"]]
        .drop_duplicates()
        .rename(columns={"Manufacturer": "name"})
    )

    con.register("manufacturers_df", manufacturers)

    con.execute("""
        INSERT INTO mfg.manufacturer (
            name,
            created_at,
            modified_at
        )
        SELECT
            name,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM manufacturers_df
        WHERE NOT EXISTS (
            SELECT 1
            FROM mfg.manufacturer AS m
            WHERE m.name = manufacturers_df.name
        )
    """)

    # ==========================================================
    # 5. LOAD PROCESSES
    # ==========================================================

    # Processes are also reference/master data and are loaded before
    # Unit Operations because Unit Operations contain a Process foreign key.

    processes = (
        df[["Process"]]
        .drop_duplicates()
        .rename(columns={"Process": "process_name"})
    )

    con.register("processes_df", processes)

    con.execute("""
        INSERT INTO mfg.process (
            process_name,
            created_at,
            modified_at
        )
        SELECT
            process_name,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM processes_df
        WHERE NOT EXISTS (
            SELECT 1
            FROM mfg.process AS p
            WHERE p.process_name = processes_df.process_name
        )
    """)

    # ==========================================================
    # 6. LOAD UNIT OPERATIONS
    # ==========================================================

    # A Unit Operation belongs to a Process.
    #
    # The source therefore needs both values to resolve the appropriate
    # process_id before the Unit Operation can be inserted.

    unit_ops = (
        df[["Process", "Unit Operation"]]
        .drop_duplicates()
    )

    con.register("unit_ops_df", unit_ops)

    con.execute("""
        INSERT INTO mfg.unit_operation (
            process_id,
            unit_operation_name,
            created_at,
            modified_at
        )
        SELECT
            p.process_id,
            u."Unit Operation",
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM unit_ops_df AS u
        JOIN mfg.process AS p
            ON u.Process = p.process_name
        WHERE NOT EXISTS (
            SELECT 1
            FROM mfg.unit_operation AS existing
            WHERE existing.process_id = p.process_id
              AND existing.unit_operation_name = u."Unit Operation"
        )
    """)

    # ==========================================================
    # 7. LOAD PARAMETERS
    # ==========================================================

    # Parameters belong to Unit Operations.
    #
    # The Process and Unit Operation values from the source are therefore
    # used to resolve unit_operation_id before the Parameter is inserted.

    params = (
        df[["Process", "Unit Operation", "Parameter"]]
        .drop_duplicates()
    )

    con.register("params_df", params)

    con.execute("""
        INSERT INTO mfg.parameter (
            unit_operation_id,
            parameter_name,
            created_at,
            modified_at
        )
        SELECT
            u.unit_operation_id,
            p."Parameter",
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM params_df AS p
        JOIN mfg.process AS pr
            ON p.Process = pr.process_name
        JOIN mfg.unit_operation AS u
            ON u.process_id = pr.process_id
           AND u.unit_operation_name = p."Unit Operation"
        WHERE NOT EXISTS (
            SELECT 1
            FROM mfg.parameter AS existing
            WHERE existing.unit_operation_id = u.unit_operation_id
              AND existing.parameter_name = p."Parameter"
        )
    """)

    # ==========================================================
    # 8. LOAD BATCHES
    # ==========================================================

    # Batch Name is the business identifier for the manufacturing batch.
    #
    # Manufacturer is converted to manufacturer_id through the reference
    # table rather than storing the Manufacturer text directly in Batch.

    batches = (
        df[["Batch Name", "Manufacturer", "DoM"]]
        .drop_duplicates()
    )

    con.register("batches_df", batches)

    con.execute("""
        INSERT INTO mfg.batch (
            batch_name,
            manufacturer_id,
            dom,
            created_at,
            modified_at
        )
        SELECT
            b."Batch Name",
            m.manufacturer_id,
            TRY_STRPTIME(
                TRIM(b.DoM),
                '%m/%d/%Y'
            ),
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM batches_df AS b
        JOIN mfg.manufacturer AS m
            ON b.Manufacturer = m.name
        WHERE NOT EXISTS (
            SELECT 1
            FROM mfg.batch AS existing
            WHERE existing.batch_name = b."Batch Name"
        )
    """)

    # ==========================================================
    # 9. LOAD RESULTS
    # ==========================================================

    # Result records connect a Batch to a Parameter.
    #
    # The source contains business-friendly identifiers, so the load
    # resolves:
    #
    #     Batch Name -> batch_id
    #
    #     Process + Unit Operation + Parameter -> parameter_id
    #
    # before inserting the result.

    con.register("results_df", df)

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
            r.Value,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM results_df AS r
        JOIN mfg.batch AS b
            ON r."Batch Name" = b.batch_name
        JOIN mfg.process AS pr
            ON r.Process = pr.process_name
        JOIN mfg.unit_operation AS u
            ON u.process_id = pr.process_id
           AND u.unit_operation_name = r."Unit Operation"
        JOIN mfg.parameter AS p
            ON p.unit_operation_id = u.unit_operation_id
           AND p.parameter_name = r.Parameter
        WHERE NOT EXISTS (
            SELECT 1
            FROM mfg.result AS existing
            WHERE existing.batch_id = b.batch_id
              AND existing.parameter_id = p.parameter_id
        )
    """)

    # ==========================================================
    # 10. LOAD SUMMARY
    # ==========================================================

    manufacturer_count = con.execute(
        "SELECT COUNT(*) FROM mfg.manufacturer"
    ).fetchone()[0]

    process_count = con.execute(
        "SELECT COUNT(*) FROM mfg.process"
    ).fetchone()[0]

    unit_operation_count = con.execute(
        "SELECT COUNT(*) FROM mfg.unit_operation"
    ).fetchone()[0]

    parameter_count = con.execute(
        "SELECT COUNT(*) FROM mfg.parameter"
    ).fetchone()[0]

    batch_count = con.execute(
        "SELECT COUNT(*) FROM mfg.batch"
    ).fetchone()[0]

    result_count = con.execute(
        "SELECT COUNT(*) FROM mfg.result"
    ).fetchone()[0]

    # ==========================================================
    # 11. COMMIT INITIAL MIGRATION
    # ==========================================================

    con.execute("COMMIT")

    print("\n==============================================")
    print("INITIAL LOAD SUMMARY")
    print("==============================================")
    print(f"Manufacturers:    {manufacturer_count}")
    print(f"Processes:        {process_count}")
    print(f"Unit operations:  {unit_operation_count}")
    print(f"Parameters:       {parameter_count}")
    print(f"Batches:          {batch_count}")
    print(f"Results:          {result_count}")
    print("==============================================")
    print("Initial data migration completed successfully.")

except Exception:

    # Roll back the entire migration if any stage fails.
    #
    # This prevents a partial initial load in which some reference tables
    # have been populated while later tables remain incomplete.

    con.execute("ROLLBACK")

    print(
        "ERROR: Initial data migration failed. "
        "Transaction rolled back."
    )

    raise

finally:

    con.close()