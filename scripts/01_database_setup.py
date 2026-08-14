# -*- coding: utf-8 -*-
"""
01_create_database.py
---------------------

Purpose
-------
Creates the normalized DuckDB manufacturing database used by the ETL
pipeline.

This script establishes the database structure used by the remaining
pipeline scripts. It is intended to be run first when creating or
rebuilding the demonstration database.

The database represents manufacturing information using separate
reference/master tables and transactional result data rather than storing
all information in a single flat table.

Database structure
------------------
The primary manufacturing entities are:

    Manufacturer
        |
        +---- Batch
                 |
                 +---- Result
                         |
                         +---- Parameter
                                  |
                                  +---- Unit Operation
                                           |
                                           +---- Process

The Process -> Unit Operation -> Parameter hierarchy represents the
manufacturing process structure.

The Batch + Parameter combination identifies a unique manufacturing result.

The database uses surrogate integer primary keys while retaining
business-friendly identifiers such as Batch Name, Process Name, and
Parameter Name as attributes.

Tables created
--------------
mfg.manufacturer
    Manufacturer reference/master data.

mfg.process
    Manufacturing process reference data.

mfg.unit_operation
    Unit operations associated with a Process.

mfg.parameter
    Parameters measured or recorded for a Unit Operation.

mfg.batch
    Manufacturing batch information, including Date of Manufacture and
    Manufacturer.

mfg.result
    Manufacturing parameter values associated with a Batch.

mfg.rejected_records
    Audit table used by the recurring validation ETL to preserve records
    rejected during data-quality validation.

Design considerations
---------------------
The database is intentionally normalized so that descriptive manufacturing
metadata is not repeatedly stored with every result record.

For example, a Result stores:

    batch_id
    parameter_id
    value

rather than repeating the Batch, Process, Unit Operation, and Parameter
descriptions for every observation.

Unique constraints are also used to enforce important business rules at
the database level, including:

    Manufacturer name
    Process name
    Process + Unit Operation
    Unit Operation + Parameter
    Batch name
    Batch + Parameter

These constraints provide a second layer of protection in addition to the
validation performed by the ETL process.

NOTE
----
This is a demonstration/development setup script.

RESET_DATABASE=True permanently removes an existing DuckDB database and
recreates it. Do not use that setting in a production environment without
an intentional database rebuild procedure.
"""

from pathlib import Path
import duckdb


# ==============================================================
# 1. CONFIGURATION
# ==============================================================

# Determine the repository/project root from the location of this script.
#
# __file__ = scripts/01_database_setup.py
# .parent  = scripts/
# .parent  = manufacturing-database-etl/

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DB_PATH = PROJECT_ROOT / "manufacturing.duckdb"

# Demonstration/development control.
#
# True:
# Delete the existing database and rebuild it.
#
# False:
# Preserve the existing database and raise an error if it already exists.
#
# This allows the database to be intentionally rebuilt while preventing an
# accidental overwrite when RESET_DATABASE is False.

RESET_DATABASE = True

# ==============================================================
# 2. RESET DATABASE
# ==============================================================

if DB_PATH.exists():

    if not RESET_DATABASE:
        raise FileExistsError(
            f"Database already exists: {DB_PATH.resolve()}\n"
            "Set RESET_DATABASE=True only when intentionally rebuilding it."
        )

    DB_PATH.unlink()

    print(f"Existing database removed: {DB_PATH.resolve()}")

# ==============================================================
# 3. CREATE DATABASE AND SCHEMA
# ==============================================================

con = duckdb.connect(str(DB_PATH))

try:

    con.execute("CREATE SCHEMA mfg")

    # ==========================================================
    # 4. CREATE ID SEQUENCES
    # ==========================================================

    # Surrogate integer keys are generated independently of the
    # business identifiers supplied by source systems or business users.
    #
    # Using sequences allows the database to generate stable internal
    # identifiers while business-facing values such as Batch Name and
    # Parameter Name remain available for validation and reporting.

    con.execute("""
        CREATE SEQUENCE mfg.rejected_seq START 1;
        CREATE SEQUENCE mfg.manufacturer_seq START 1;
        CREATE SEQUENCE mfg.process_seq START 1;
        CREATE SEQUENCE mfg.unit_operation_seq START 1;
        CREATE SEQUENCE mfg.parameter_seq START 1;
        CREATE SEQUENCE mfg.batch_seq START 1;
        CREATE SEQUENCE mfg.result_seq START 1;
    """)

    # ==========================================================
    # 5. CREATE AUDIT / REJECTION TABLE
    # ==========================================================

    # Rejected records are retained separately from the manufacturing
    # tables so invalid source data does not enter the normalized model.
    #
    # raw_data stores the submitted row as JSON so that the original
    # business-unit submission can be reviewed after validation.

    con.execute("""
        CREATE TABLE mfg.rejected_records (
            rejected_id INTEGER PRIMARY KEY
                DEFAULT nextval('mfg.rejected_seq'),

            rejection_stage VARCHAR NOT NULL,
            source_file VARCHAR,
            raw_data JSON,
            rejection_hash VARCHAR NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ==========================================================
    # 6. CREATE MANUFACTURER REFERENCE TABLE
    # ==========================================================

    # Manufacturer is reference/master data.
    #
    # The recurring batch ETL validates submitted Manufacturers against
    # this table rather than creating new reference records automatically.

    con.execute("""
        CREATE TABLE mfg.manufacturer (
            manufacturer_id INTEGER PRIMARY KEY
                DEFAULT nextval('mfg.manufacturer_seq'),

            name VARCHAR NOT NULL UNIQUE,
            location VARCHAR,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ==========================================================
    # 7. CREATE PROCESS REFERENCE TABLE
    # ==========================================================

    # Process represents a manufacturing process within the process
    # hierarchy:
    #
    #     Process
    #         |
    #         +-- Unit Operation
    #                 |
    #                 +-- Parameter

    con.execute("""
        CREATE TABLE mfg.process (
            process_id INTEGER PRIMARY KEY
                DEFAULT nextval('mfg.process_seq'),

            process_name VARCHAR NOT NULL UNIQUE,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ==========================================================
    # 8. CREATE UNIT OPERATION TABLE
    # ==========================================================

    # A Unit Operation belongs to a specific Process.
    #
    # Therefore, Unit Operation names are unique within a Process rather
    # than being treated as globally unique across the database.

    con.execute("""
        CREATE TABLE mfg.unit_operation (
            unit_operation_id INTEGER PRIMARY KEY
                DEFAULT nextval('mfg.unit_operation_seq'),

            process_id INTEGER NOT NULL,
            unit_operation_type VARCHAR,
            unit_operation_name VARCHAR,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (process_id)
                REFERENCES mfg.process(process_id),

            UNIQUE (process_id, unit_operation_name)
        )
    """)

    # ==========================================================
    # 9. CREATE PARAMETER TABLE
    # ==========================================================

    # Parameters belong to a specific Unit Operation.
    #
    # The combination of:
    #
    #     Unit Operation + Parameter
    #
    # therefore identifies the parameter within the manufacturing
    # process hierarchy.

    con.execute("""
        CREATE TABLE mfg.parameter (
            parameter_id INTEGER PRIMARY KEY
                DEFAULT nextval('mfg.parameter_seq'),

            unit_operation_id INTEGER NOT NULL,
            parameter_name VARCHAR NOT NULL,
            units VARCHAR,

            lower_specification_limit DOUBLE,
            upper_specification_limit DOUBLE,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (unit_operation_id)
                REFERENCES mfg.unit_operation(unit_operation_id),

            UNIQUE (unit_operation_id, parameter_name)
        )
    """)

    # ==========================================================
    # 10. CREATE BATCH TABLE
    # ==========================================================

    # Batch represents a manufacturing batch and is associated with a
    # Manufacturer through a foreign key.
    #
    # Batch Name is retained as the business identifier supplied by the
    # source data while batch_id is the database surrogate key.

    con.execute("""
        CREATE TABLE mfg.batch (
            batch_id INTEGER PRIMARY KEY
                DEFAULT nextval('mfg.batch_seq'),

            batch_name VARCHAR NOT NULL UNIQUE,
            dom DATE,
            manufacturer_id INTEGER,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (manufacturer_id)
                REFERENCES mfg.manufacturer(manufacturer_id)
        )
    """)

    # ==========================================================
    # 11. CREATE RESULT TABLE
    # ==========================================================

    # Result represents the actual manufacturing parameter value recorded
    # for a Batch.
    #
    # The business key for a result is:
    #
    #     Batch + Parameter
    #
    # This prevents multiple results from being stored for the same
    # parameter within the same batch.

    con.execute("""
        CREATE TABLE mfg.result (
            result_id INTEGER PRIMARY KEY
                DEFAULT nextval('mfg.result_seq'),

            batch_id INTEGER NOT NULL,
            parameter_id INTEGER NOT NULL,
            value VARCHAR,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (batch_id)
                REFERENCES mfg.batch(batch_id),

            FOREIGN KEY (parameter_id)
                REFERENCES mfg.parameter(parameter_id),

            UNIQUE (batch_id, parameter_id)
        )
    """)

    # ==========================================================
    # 12. VALIDATE DATABASE STRUCTURE
    # ==========================================================

    print("\nSchemas:")

    con.sql("""
        SELECT schema_name
        FROM information_schema.schemata
        ORDER BY schema_name
    """).show()

    print("Tables in mfg:")

    con.sql("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'mfg'
        ORDER BY table_name
    """).show()

    print("Database structure created successfully.")

finally:

    con.close()