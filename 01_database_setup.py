# -*- coding: utf-8 -*-
"""

@author: duchez
"""

#%% ============================================================
# 1. Import Dependencies
# ==============================================================
# DuckDB is an embedded analytical database that can be used
# directly from Python. The duckdb package provides the Python
# interface used to create, connect to, and modify the database.
# ==============================================================

import duckdb
import os


#%% ============================================================
# 2. Define Database Location
# ==============================================================
# Define the location of the DuckDB database file.
#
# Using a relative path means the database will be created in
# relation to the current working directory of the Python/Spyder
# session.
#
# Example:
#     .\manufacturing.duckdb
#
# This file will contain the entire DuckDB database.
# ==============================================================

DB_PATH = r".\manufacturing.duckdb"


#%% ============================================================
# 3. Reset Existing Database
# ==============================================================
# This project is being used as a demonstration/development
# database, so each execution should start from a clean database.
#
# If the DuckDB file already exists, remove it before connecting.
# This means the entire database—including schemas, tables,
# sequences, and data—is recreated from scratch on every run.
#
# This approach is preferable for a demo/reset script because it
# avoids errors such as:
#
#     "Schema mfg already exists"
#     "Table mfg.batch already exists"
#     "Sequence mfg.batch_seq already exists"
#
# WARNING:
# This permanently deletes the existing database and all data
# stored in it.
# ==============================================================

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print("Existing database removed.")


#%% ============================================================
# 4. Connect to Database
# ==============================================================
# Connecting to a DuckDB file creates the database if it does not
# already exist.
#
# Because the previous block removes the existing database, this
# connection will always be made to a new, empty database.
# ==============================================================

con = duckdb.connect(DB_PATH)

print("Connected to:")
print(DB_PATH)


#%% ============================================================
# 5. Create Manufacturing Schema
# ==============================================================
# A schema provides a logical namespace for database objects.
#
# The "mfg" schema will contain all tables and sequences associated
# with the manufacturing data model.
#
# Instead of placing tables in the default "main" schema, objects
# will be referenced using:
#
#     mfg.table_name
#
# For example:
#
#     mfg.batch
#     mfg.parameter
#     mfg.result
# ==============================================================

con.execute("""
    CREATE SCHEMA mfg;
""")


#%% ============================================================
# 6. Create Primary-Key Sequences
# ==============================================================
# Sequences generate unique integer identifiers for the tables.
#
# Each major entity receives its own sequence:
#
#     manufacturer_seq      -> manufacturer_id
#     process_seq           -> process_id
#     unit_operation_seq   -> unit_operation_id
#     parameter_seq         -> parameter_id
#     batch_seq             -> batch_id
#     result_seq            -> result_id
#
# The rejected_records table also receives its own sequence.
#
# These sequences are referenced later by DEFAULT nextval(...)
# in the table definitions.
#
# Example:
#
#     DEFAULT nextval('mfg.batch_seq')
#
# This automatically generates the next batch_id whenever a new
# batch is inserted without explicitly providing an ID.
# ==============================================================

con.execute("""
CREATE SEQUENCE mfg.rejected_seq START 1;
CREATE SEQUENCE mfg.manufacturer_seq START 1;
CREATE SEQUENCE mfg.process_seq START 1;
CREATE SEQUENCE mfg.unit_operation_seq START 1;
CREATE SEQUENCE mfg.parameter_seq START 1;
CREATE SEQUENCE mfg.batch_seq START 1;
CREATE SEQUENCE mfg.result_seq START 1;
""")


#%% ============================================================
# 7. Create Rejected Records Table
# ==============================================================
# This table stores records that fail validation during the ETL
# process.
#
# rejection_stage identifies the point in the validation pipeline
# where the record failed.
#
# raw_data stores the original record as JSON so that the rejected
# input can be inspected later.
#
# created_at records when the rejection occurred.
#
# This table is intentionally separate from the production tables
# because invalid records should not enter the normalized
# manufacturing data model.
# ==============================================================

con.execute("""
    CREATE TABLE mfg.rejected_records (

        rejected_id INTEGER PRIMARY KEY
            DEFAULT nextval('mfg.rejected_seq'),

        rejection_stage VARCHAR NOT NULL,

        raw_data JSON,

        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

    );
""")


#%% ============================================================
# 8. Create Manufacturer Table
# ==============================================================
# Stores manufacturer-level information.
#
# manufacturer_id:
#     Surrogate primary key generated by manufacturer_seq.
#
# name:
#     Required manufacturer name.
#
# location:
#     Optional manufacturer location.
#
# created_at / modified_at:
#     Timestamps used to track when the record was created and
#     last modified.
# ==============================================================

con.execute("""
CREATE TABLE mfg.manufacturer (

    manufacturer_id INTEGER PRIMARY KEY
        DEFAULT nextval('mfg.manufacturer_seq'),

    name VARCHAR NOT NULL,

    location VARCHAR,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);
""")


#%% ============================================================
# 9. Create Process Table
# ==============================================================
# Stores manufacturing processes.
#
# Each process receives a unique process_id generated by the
# process sequence.
#
# Example processes might represent different manufacturing
# workflows or production processes.
# ==============================================================

con.execute("""
CREATE TABLE mfg.process (

    process_id INTEGER PRIMARY KEY
        DEFAULT nextval('mfg.process_seq'),

    process_name VARCHAR NOT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);
""")


#%% ============================================================
# 10. Create Unit Operation Table
# ==============================================================
# Stores the individual unit operations that belong to a process.
#
# process_id creates the relationship between a unit operation and
# its parent process.
#
# The foreign key ensures that a unit operation cannot reference
# a process that does not exist.
#
# Relationship:
#
#     Process
#        |
#        | 1-to-many
#        v
#     Unit Operation
# ==============================================================

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
        REFERENCES mfg.process(process_id)

);
""")


#%% ============================================================
# 11. Create Parameter Table
# ==============================================================
# Stores manufacturing parameters associated with a unit operation.
#
# Each parameter belongs to one unit operation through
# unit_operation_id.
#
# lower_specification_limit and upper_specification_limit define
# the acceptable range for the parameter.
#
# Example:
#
#     Parameter: Temperature
#     Units:     °C
#     Lower:     20
#     Upper:     25
#
# Relationship:
#
#     Process
#        |
#     Unit Operation
#        |
#        v
#     Parameter
# ==============================================================

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
        REFERENCES mfg.unit_operation(unit_operation_id)

);
""")


#%% ============================================================
# 12. Create Batch Table
# ==============================================================
# Stores individual manufacturing batches.
#
# batch_name is UNIQUE, meaning the same batch cannot be entered
# into the database more than once.
#
# manufacturer_id creates a relationship between the batch and
# the manufacturer that produced it.
#
# dom represents the Date of Manufacture.
#
# Relationship:
#
#     Manufacturer
#          |
#          | 1-to-many
#          v
#        Batch
# ==============================================================

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

);
""")


#%% ============================================================
# 13. Create Result Table
# ==============================================================
# Stores the actual measured value for a parameter on a specific
# manufacturing batch.
#
# batch_id identifies the batch being tested.
#
# parameter_id identifies the parameter being measured.
#
# Together, batch_id and parameter_id form a UNIQUE constraint.
# This prevents the same parameter from being recorded more than
# once for the same batch.
#
# Relationship:
#
#     Batch ------------------+
#                              |
#                              v
#                           Result
#                              ^
#                              |
#     Parameter --------------+
#
# The result table therefore acts as the transactional/fact table
# connecting manufacturing batches with measured parameters.
# ==============================================================

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

    UNIQUE(batch_id, parameter_id)

);
""")


#%% ============================================================
# 14. Validate Database Structure
# ==============================================================
# These commands can be used during development to inspect the
# database structure.
#
# ==============================================================


# Display schemas
con.sql("""
    SELECT schema_name
    FROM information_schema.schemata
""").show()

# Display tables in the mfg schema
con.sql("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'mfg'
""").show()


#%% ============================================================
# 15. Close Database Connection
# ==============================================================
# Close the DuckDB connection after the database structure has
# been created.
#
# Closing the connection ensures that DuckDB has completed its
# database operations and releases the database connection.
# ==============================================================

con.close()

print("Database created successfully.")

