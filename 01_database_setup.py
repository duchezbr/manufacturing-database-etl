# -*- coding: utf-8 -*-
"""
Created on Thu Jun 25 19:32:57 2026

@author: duche
"""

#%%


import duckdb


#%%

# Create database
DB_PATH = r".\database\manufacturing.duckdb"


#%%


con = duckdb.connect(DB_PATH)

print("Connected to:")
print(DB_PATH)


#%% 

con.execute("""
    DROP TABLE IF EXISTS mfg.rejected_records;
    DROP TABLE IF EXISTS mfg.result;
    DROP TABLE IF EXISTS mfg.batch;
    DROP TABLE IF EXISTS mfg.parameter;
    DROP TABLE IF EXISTS mfg.unit_operation;
    DROP TABLE IF EXISTS mfg.process;
    DROP TABLE IF EXISTS mfg.manufacturer;
"""
);



#%%


con.execute("""
    CREATE SCHEMA mfg;
"""
);


#%%


con.execute("""
CREATE SEQUENCE mfg.rejected_seq START 1;
CREATE SEQUENCE mfg.manufacturer_seq START 1;
CREATE SEQUENCE mfg.process_seq START 1;
CREATE SEQUENCE mfg.unit_operation_seq START 1;
CREATE SEQUENCE mfg.parameter_seq START 1;
CREATE SEQUENCE mfg.batch_seq START 1;
CREATE SEQUENCE mfg.result_seq START 1;
"""
);


#%%


con.execute("""
    CREATE TABLE mfg.rejected_records (

    rejected_id INTEGER PRIMARY KEY
    DEFAULT nextval('mfg.rejected_seq'),

    rejection_stage VARCHAR NOT NULL,

    raw_data JSON,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    );    
""")


#%%


con.execute("""
    CREATE TABLE mfg.manufacturer (

    manufacturer_id INTEGER PRIMARY KEY
        DEFAULT nextval('mfg.manufacturer_seq'),

    name VARCHAR NOT NULL,

    location VARCHAR,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"""
)


#%%


con.execute("""
CREATE TABLE mfg.process (

    process_id INTEGER PRIMARY KEY
        DEFAULT nextval('mfg.process_seq'),

    process_name VARCHAR NOT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

    );
"""
)


#%%

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
"""
)


#%%


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
"""
)


#%%


con.execute("""
CREATE TABLE mfg.batch (

    batch_id INTEGER PRIMARY KEY
        DEFAULT nextval('mfg.batch_seq'),

    batch_name VARCHAR,

    dom DATE,

    manufacturer_id INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (manufacturer_id)
        REFERENCES mfg.manufacturer(manufacturer_id)

);
"""
)


#%%


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
"""
)


#%%


con.sql("SHOW SCHEMAS").show()
con.sql("SHOW TABLES FROM mfg").show()


#%%


con.close()

print("Database created successfully.")

