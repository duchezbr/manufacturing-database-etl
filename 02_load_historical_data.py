# -*- coding: utf-8 -*-
"""
Created on Thu Jun 25 15:55:56 2026

@author: duche
"""

import pandas as pd
import duckdb

DB_PATH = r".\database\manufacturing.duckdb"

#%% Load Data

df = pd.read_csv(
    r".\mock_historical_data.csv"
)

df["DoM"] = pd.to_datetime(df["DoM"])

#%%
# --------------------------------------------------
# Populate Manufacturer Table
# --------------------------------------------------

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
    
#%%
# --------------------------------------------------
# Populate Process Table
# --------------------------------------------------

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
#%%
# --------------------------------------------------
# Populate Unit Operation Table
# --------------------------------------------------

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

#%%
# --------------------------------------------------
# Populate Parameter Table
# --------------------------------------------------

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

#%%
# --------------------------------------------------
# Populate Batch Table
# --------------------------------------------------

with duckdb.connect(DB_PATH) as conn:

    batches = (
        df[
            [
                "Batch Name",
                "Manufacturer",
                "Process",
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

#%%
# --------------------------------------------------
# Populate Results Table
# --------------------------------------------------

with duckdb.connect(DB_PATH) as conn:

    conn.register("results_df", df)

    conn.execute("""
        INSERT INTO mfg.result (
            batch_id,
            parameter_id,
            value
        )
        SELECT
            b.batch_id,
            p.parameter_id,
            r.Value
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
    """)
    

