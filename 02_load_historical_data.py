# -*- coding: utf-8 -*-
"""
Manufacturing Database ETL
--------------------------

Loads manufacturing data from a consolidated flat file into the
normalized DuckDB manufacturing database.

Source:
    mock_historical_data.csv

Destination:
    manufacturing.duckdb

The source file represents manufacturing data that has been
consolidated from multiple business units. Each business unit
maintains information related to its respective manufacturing
activities, and the resulting flat file contains the combined
dataset used by this ETL process.

The ETL process separates the flat-file data into normalized
database tables and establishes the appropriate relationships
between manufacturers, processes, unit operations, parameters,
batches, and results.

@author: duchez
"""

import pandas as pd
import duckdb


#%% ============================================================
# 1. Define Database Location
# ==============================================================
# Define the location of the DuckDB database created by the
# database setup script.
#
# This script assumes that the database structure has already
# been created and that the mfg schema and its tables exist.
# ==============================================================

DB_PATH = r".\manufacturing.duckdb"


#%% ============================================================
# 2. Load Source Data
# ==============================================================
# Read the consolidated manufacturing flat file into a pandas
# DataFrame.
#
# The source file represents data that has been combined from
# multiple business units into a single historical dataset.
#
# The DoM (Date of Manufacture) column is explicitly converted
# from text into a pandas datetime value so that it can be loaded
# into the DuckDB DATE column in mfg.batch.
# ==============================================================

df = pd.read_csv(
    r".\mock_historical_data.csv"
)

df["DoM"] = pd.to_datetime(df["DoM"])


#%% ============================================================
# 3. Populate Manufacturer Table
# ==============================================================
# Extract the unique manufacturers from the source data.
#
# The source column "Manufacturer" is renamed to "name" because
# the destination table uses "name" as its column name.
#
# drop_duplicates() ensures that each manufacturer is considered
# only once during the load.
#
# The NOT IN condition prevents a manufacturer already present in
# the database from being inserted again.
#
# This allows the ETL process to be rerun without duplicating
# existing manufacturers.
# ==============================================================

with duckdb.connect(DB_PATH) as conn:

    manufacturers = (
        df[["Manufacturer"]]
        .drop_duplicates()
        .rename(columns={"Manufacturer": "name"})
    )

    conn.register("manufacturers_df", manufacturers)

    conn.execute("""
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
        WHERE name NOT IN (
            SELECT name
            FROM mfg.manufacturer
        )
    """)


#%% ============================================================
# 4. Populate Process Table
# ==============================================================
# Extract the unique manufacturing processes from the source
# data.
#
# process_name is the corresponding destination column in
# mfg.process.
#
# The NOT IN condition prevents processes that already exist in
# the database from being inserted again.
# ==============================================================

with duckdb.connect(DB_PATH) as conn:

    processes = (
        df[["Process"]]
        .drop_duplicates()
        .rename(columns={"Process": "process_name"})
    )

    conn.register("processes_df", processes)

    conn.execute("""
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
        WHERE process_name NOT IN (
            SELECT process_name
            FROM mfg.process
        )
    """)


#%% ============================================================
# 5. Populate Unit Operation Table
# ==============================================================
# Extract unique Process / Unit Operation combinations.
#
# A unit operation belongs to a specific process, so the source
# Process value is used to locate the corresponding process_id.
#
# The process_id is then stored as a foreign key in
# mfg.unit_operation.
#
# NOT EXISTS prevents the same unit operation from being inserted
# more than once for the same process.
#
# This is important because a unit operation name by itself may
# not be globally unique. The meaningful business key is:
#
#     process + unit operation
# ==============================================================

with duckdb.connect(DB_PATH) as conn:

    unit_ops = (
        df[["Process", "Unit Operation"]]
        .drop_duplicates()
    )

    conn.register("unit_ops_df", unit_ops)

    conn.execute("""
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
        FROM unit_ops_df u

        JOIN mfg.process p
            ON u.Process = p.process_name

        WHERE NOT EXISTS (
            SELECT 1
            FROM mfg.unit_operation existing
            WHERE existing.process_id = p.process_id
              AND existing.unit_operation_name = u."Unit Operation"
        )
    """)


#%% ============================================================
# 6. Populate Parameter Table
# ==============================================================
# Extract unique Process / Unit Operation / Parameter combinations.
#
# Parameters belong to a specific unit operation, which in turn
# belongs to a process.
#
# The joins resolve the source business names into the surrogate
# primary keys used by the normalized database.
#
# The resulting unit_operation_id is stored as a foreign key in
# mfg.parameter.
#
# NOT EXISTS prevents duplicate parameters from being inserted
# for the same unit operation.
#
# The effective business key is:
#
#     unit operation + parameter
# ==============================================================

with duckdb.connect(DB_PATH) as conn:

    params = (
        df[["Process", "Unit Operation", "Parameter"]]
        .drop_duplicates()
    )

    conn.register("params_df", params)

    conn.execute("""
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

        FROM params_df p

        JOIN mfg.process pr
            ON p.Process = pr.process_name

        JOIN mfg.unit_operation u
            ON u.process_id = pr.process_id
           AND u.unit_operation_name = p."Unit Operation"

        WHERE NOT EXISTS (
            SELECT 1
            FROM mfg.parameter existing
            WHERE existing.unit_operation_id = u.unit_operation_id
              AND existing.parameter_name = p."Parameter"
        )
    """)


#%% ============================================================
# 7. Populate Batch Table
# ==============================================================
# Extract unique batches from the source data.
#
# Only Batch Name, Manufacturer, and DoM are required because
# mfg.batch does not contain a process_id.
#
# The source Manufacturer name is joined to mfg.manufacturer to
# retrieve the manufacturer's surrogate primary key.
#
# That manufacturer_id is then stored as a foreign key in
# mfg.batch.
#
# NOT EXISTS prevents an existing batch from being inserted again.
#
# The batch_name column is also defined as UNIQUE in the database,
# providing an additional database-level protection against
# duplicate batches.
# ==============================================================

with duckdb.connect(DB_PATH) as conn:

    batches = (
        df[
            [
                "Batch Name",
                "Manufacturer",
                "DoM"
            ]
        ]
        .drop_duplicates()
    )

    conn.register("batches_df", batches)

    conn.execute("""
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
            b.DoM,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP

        FROM batches_df b

        JOIN mfg.manufacturer m
            ON b.Manufacturer = m.name

        WHERE NOT EXISTS (
            SELECT 1
            FROM mfg.batch existing
            WHERE existing.batch_name = b."Batch Name"
        )
    """)


#%% ============================================================
# 8. Populate Results Table
# ==============================================================
# Load the source DataFrame into DuckDB and use the relationships
# established in the previous steps to resolve the appropriate
# surrogate keys.
#
# The source data contains business-friendly identifiers:
#
#     Batch Name
#     Process
#     Unit Operation
#     Parameter
#
# The normalized result table instead stores:
#
#     batch_id
#     parameter_id
#
# The joins progressively resolve those business identifiers:
#
#     Batch Name
#          ↓
#     batch_id
#
#     Process + Unit Operation
#          ↓
#     unit_operation_id
#
#     Unit Operation + Parameter
#          ↓
#     parameter_id
#
# NOT EXISTS prevents a result from being inserted if that
# batch/parameter combination already exists.
#
# This is consistent with the UNIQUE(batch_id, parameter_id)
# constraint defined on mfg.result.
# ==============================================================

with duckdb.connect(DB_PATH) as conn:

    conn.register("results_df", df)

    conn.execute("""
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

        FROM results_df r

        JOIN mfg.batch b
            ON r."Batch Name" = b.batch_name

        JOIN mfg.process pr
            ON r.Process = pr.process_name

        JOIN mfg.unit_operation u
            ON u.process_id = pr.process_id
           AND u.unit_operation_name = r."Unit Operation"

        JOIN mfg.parameter p
            ON p.unit_operation_id = u.unit_operation_id
           AND p.parameter_name = r.Parameter

        WHERE NOT EXISTS (
            SELECT 1
            FROM mfg.result existing
            WHERE existing.batch_id = b.batch_id
              AND existing.parameter_id = p.parameter_id
        )
    """)


#%% ============================================================
# 9. Close Database Connections
# ==============================================================
# Each ETL block uses a context manager:
#
#     with duckdb.connect(DB_PATH) as conn:
#
# The connection is therefore automatically closed when the block
# finishes.
#
# No explicit conn.close() is required for these blocks.
#
# The database remains stored on disk at DB_PATH and can be
# accessed by subsequent Python scripts, Power BI, or other tools.
# ==============================================================

print("ETL load completed successfully.")

