-- PostgreSQL v17 Data Types Demonstration
-- This file contains examples of all PostgreSQL data types

-- Required types for the tables below
BEGIN;
CREATE TYPE mood_enum AS ENUM ('happy', 'sad', 'neutral');
CREATE TYPE complex_type AS (x INTEGER, y TEXT);
CREATE DOMAIN positive_integer AS INTEGER CHECK (VALUE > 0);

CREATE TABLE postgres_data_types (
    -- Numeric Types
    col_smallint SMALLINT,                          -- 2-byte signed integer, range: -32768 to 32767
    col_integer INTEGER,                           -- 4-byte signed integer, range: -2147483648 to 2147483647
    col_bigint BIGINT,                             -- 8-byte signed integer, range: -9223372036854775808 to 9223372036854775807
    col_decimal DECIMAL(10,2),                     -- user-specified precision, exact
    col_numeric NUMERIC(10,2),                     -- user-specified precision, exact (same as DECIMAL)
    col_real REAL,                                 -- 4-byte floating-point number, 6 decimal digits precision
    col_double_precision DOUBLE PRECISION,         -- 8-byte floating-point number, 15 decimal digits precision
    col_smallserial SMALLSERIAL,                   -- 2-byte autoincrementing integer
    col_serial SERIAL,                             -- 4-byte autoincrementing integer
    col_bigserial BIGSERIAL,                       -- 8-byte autoincrementing integer
    
    -- Monetary Type
    col_money MONEY,                               -- currency amount, 8 bytes
    
    -- Character Types
    col_character CHARACTER(10),                   -- fixed-length character string, blank padded
    col_char CHAR(10),                             -- alias for CHARACTER
    col_character_varying CHARACTER VARYING(100),  -- variable-length character string with limit
    col_varchar VARCHAR(100),                      -- alias for CHARACTER VARYING
    col_text TEXT,                                 -- variable unlimited length
    
    -- Binary Data Types
    col_bytea BYTEA,                               -- variable-length binary string
    
    -- Date/Time Types
    col_timestamp TIMESTAMP,                       -- date and time without time zone
    col_timestamp_tz TIMESTAMP WITH TIME ZONE,     -- date and time with time zone
    col_date DATE,                                 -- date (no time of day)
    col_time TIME,                                 -- time of day (no date)
    col_time_tz TIME WITH TIME ZONE,              -- time of day with time zone
    col_interval INTERVAL,                         -- time interval
    
    -- Boolean Type
    col_boolean BOOLEAN,                           -- logical Boolean (true/false)
    
    -- Enumerated Type
    col_enum_mood mood_enum,                       -- enumerated type (requires prior CREATE TYPE mood_enum AS ENUM)
    
    -- Geometric Types
    col_point POINT,                               -- geometric point (x,y)
    col_line LINE,                                 -- infinite line
    col_lseg LSEG,                                 -- line segment
    col_box BOX,                                   -- rectangular box
    col_path PATH,                                 -- geometric path
    col_polygon POLYGON,                           -- closed geometric path
    col_circle CIRCLE,                             -- circle
    
    -- Network Address Types
    col_cidr CIDR,                                 -- IPv4 or IPv6 network address
    col_inet INET,                                 -- IPv4 or IPv6 host address
    col_macaddr MACADDR,                           -- MAC address
    col_macaddr8 MACADDR8,                         -- MAC address (EUI-64 format)
    
    -- Bit String Types
    col_bit BIT(8),                                -- fixed-length bit string
    col_bit_varying BIT VARYING(64),               -- variable-length bit string
    
    -- Text Search Types
    col_tsvector TSVECTOR,                         -- text search document
    col_tsquery TSQUERY,                           -- text search query
    
    -- UUID Type
    col_uuid UUID,                                 -- universally unique identifier
    
    -- XML Type
    col_xml XML,                                   -- XML data
    
    -- JSON Types
    col_json JSON,                                 -- textual JSON data
    col_jsonb JSONB,                               -- binary JSON data, decomposed
    col_jsonpath JSONPATH,                         -- JSON path expression
    
    -- Array Types
    col_integer_array INTEGER[],                   -- array of integers
    col_text_array TEXT[],                         -- array of text
    col_multidim_array INTEGER[][],                -- multidimensional array
    
    -- Composite Types
    col_composite complex_type,                    -- composite type (requires prior CREATE TYPE complex_type AS)
    
    -- Range Types
    col_int4range INT4RANGE,                       -- range of integers
    col_int8range INT8RANGE,                       -- range of bigints
    col_numrange NUMRANGE,                         -- range of numeric
    col_tsrange TSRANGE,                           -- range of timestamp without time zone
    col_tstzrange TSTZRANGE,                       -- range of timestamp with time zone
    col_daterange DATERANGE,                       -- range of date
    
    -- Multirange Types (PostgreSQL 14+)
    col_int4multirange INT4MULTIRANGE,             -- multirange of integers
    col_int8multirange INT8MULTIRANGE,             -- multirange of bigints
    col_nummultirange NUMMULTIRANGE,               -- multirange of numeric
    col_tsmultirange TSMULTIRANGE,                 -- multirange of timestamp without time zone
    col_tstzmultirange TSTZMULTIRANGE,             -- multirange of timestamp with time zone
    col_datemultirange DATEMULTIRANGE,             -- multirange of date
    
    -- Domain Types (example)
    col_domain_type positive_integer,              -- domain type (requires prior CREATE DOMAIN positive_integer AS)
    
    -- Object Identifier Types
    col_oid OID,                                   -- object identifier
    col_regclass REGCLASS,                         -- registered class
    col_regproc REGPROC,                           -- registered procedure
    col_regprocedure REGPROCEDURE,                 -- registered procedure with arguments
    col_regoper REGOPER,                           -- registered operator
    col_regoperator REGOPERATOR,                   -- registered operator with arguments
    col_regtype REGTYPE,                           -- registered type
    col_regrole REGROLE,                           -- registered role
    col_regnamespace REGNAMESPACE,                 -- registered namespace
    col_regconfig REGCONFIG,                       -- registered text search configuration
    col_regdictionary REGDICTIONARY,               -- registered text search dictionary
    
    -- pg_lsn Type
    col_pg_lsn PG_LSN,                             -- PostgreSQL Log Sequence Number
    
    -- Pseudo-Types (cannot be used directly as column types but shown for completeness)
    -- any, anyelement, anyarray, anynonarray, anyenum, anyrange, anymultirange, anycompatible, anycompatiblearray, anycompatiblenonarray, anycompatiblerange, anycompatiblemultirange
    
    -- New in PostgreSQL 16/17
    col_xid8 XID8,                                 -- 8-byte transaction ID
    -- col_multirange MULTIRANGE,                  -- generic multirange type (commented out as not available in this PostgreSQL version)
    
    -- Constraints for demonstration
    PRIMARY KEY (col_integer),
    UNIQUE (col_uuid),
    CHECK (col_smallint > 0)
);

-- Examples of table features

-- Example of a table with generated columns
CREATE TABLE generated_columns_example (
    id SERIAL PRIMARY KEY,
    width NUMERIC(10,2),
    height NUMERIC(10,2),
    area NUMERIC(20,4) GENERATED ALWAYS AS (width * height) STORED,
    perimeter NUMERIC(20,4) GENERATED ALWAYS AS (2 * (width + height)) STORED
);

-- Example of a partitioned table
CREATE TABLE measurement (
    city_id INT NOT NULL,
    logdate DATE NOT NULL,
    peaktemp INT,
    unitsales INT
) PARTITION BY RANGE (logdate);

CREATE TABLE measurement_y2023m01 PARTITION OF measurement
    FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');

CREATE TABLE measurement_y2023m02 PARTITION OF measurement
    FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');

-- Example of identity columns (PostgreSQL 10+)
CREATE TABLE identity_example (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_by_default INT GENERATED BY DEFAULT AS IDENTITY,
    name TEXT
);

-- Example of a table with INCLUDE index
CREATE TABLE include_index_example (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    data JSONB
);

CREATE UNIQUE INDEX idx_include_example ON include_index_example (customer_id, product_id) INCLUDE (timestamp);

-- Example of a table with NULLS NOT DISTINCT constraint (PostgreSQL 15+)
CREATE TABLE nulls_not_distinct_example (
    id SERIAL PRIMARY KEY,
    code TEXT,
    UNIQUE NULLS NOT DISTINCT (code)
);

-- Example of a table with IDENTITY sequence options
CREATE TABLE identity_options_example (
    id INT GENERATED BY DEFAULT AS IDENTITY 
       (START WITH 1000 INCREMENT BY 10 MINVALUE 1000 MAXVALUE 9999 CYCLE) PRIMARY KEY,
    name TEXT
);

-- Example of a table with GENERATED columns using expressions
CREATE TABLE expression_generated_example (
    id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    email TEXT,
    email_domain TEXT GENERATED ALWAYS AS (SUBSTRING(email FROM POSITION('@' IN email) + 1)) STORED
);

-- Example of a table with EXCLUSION constraints
-- First create the extension needed for the GiST index with range types
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE reservation (
    room_id INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    EXCLUDE USING GIST (room_id WITH =, TSRANGE(start_time, end_time) WITH &&)
);

-- Example of a table with DEFERRABLE constraints
CREATE TABLE deferrable_example (
    id SERIAL PRIMARY KEY,
    parent_id INT,
    FOREIGN KEY (parent_id) REFERENCES deferrable_example (id) DEFERRABLE INITIALLY DEFERRED
);

-- Example of a table with column collation
CREATE TABLE collation_example (
    id SERIAL PRIMARY KEY,
    name_default TEXT,
    name_en TEXT,
    name_de TEXT
);

-- Example of a table with UNLOGGED option (data not written to WAL)
CREATE UNLOGGED TABLE unlogged_example (
    id SERIAL PRIMARY KEY,
    data TEXT
);

-- Example of a table with LIKE including all options
-- This table will be created after postgres_data_types is created
CREATE TABLE like_example (
    id SERIAL PRIMARY KEY,
    data TEXT
);

-- Example of a table with column compression
CREATE TABLE compression_example (
    id SERIAL PRIMARY KEY,
    data TEXT COMPRESSION pglz,
    large_data BYTEA COMPRESSION lz4
);

-- Example of a table with TEMPORARY option
CREATE TEMPORARY TABLE temp_example (
    id SERIAL PRIMARY KEY,
    session_data JSONB
) ON COMMIT DELETE ROWS;

-- Example of a table with INHERITS
CREATE TABLE parent_table (
    id SERIAL PRIMARY KEY,
    name TEXT
);

CREATE TABLE child_table (
    extra_data TEXT
) INHERITS (parent_table);

-- Example of a table with OF type
CREATE TYPE person_type AS (
    name TEXT,
    age INTEGER,
    address TEXT
);

CREATE TABLE person_table OF person_type (
    name PRIMARY KEY,
    age CHECK (age > 0)
);

-- Example of a table with WITH options
CREATE TABLE with_options_example (
    id SERIAL PRIMARY KEY,
    data TEXT
) WITH (fillfactor=70, autovacuum_enabled=false);

-- Example of a table with typed tables
CREATE TYPE employee_type AS (
    name TEXT,
    salary NUMERIC
);

CREATE TABLE employees OF employee_type (
    name WITH OPTIONS PRIMARY KEY,
    salary WITH OPTIONS DEFAULT 1000.00,
    CONSTRAINT salary_check CHECK (salary > 0)
);

-- Example of a table with TABLESPACE option
-- CREATE TABLESPACE example_space LOCATION '/custom/location';
-- CREATE TABLE tablespace_example (
--     id SERIAL PRIMARY KEY,
--     data TEXT
-- ) TABLESPACE example_space;

-- Example of a table with USING method
CREATE TABLE using_method_example (
    id SERIAL PRIMARY KEY,
    data TEXT
) USING heap;

-- Example of a table with FOREIGN option (requires foreign data wrapper setup)
-- CREATE FOREIGN TABLE foreign_example (
--     id INTEGER,
--     data TEXT
-- ) SERVER foreign_server OPTIONS (schema 'remote_schema', table 'remote_table');

-- Example of a table with all PostgreSQL v17 features
CREATE TABLE comprehensive_example (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data JSONB DEFAULT '{}'::jsonb,
    tags TEXT[] DEFAULT '{}',
    status TEXT DEFAULT 'active',
    numeric_data NUMERIC(20,6) DEFAULT 0,
    is_valid BOOLEAN DEFAULT TRUE,
    valid_range TSTZRANGE,
    search_vector TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', COALESCE(status, ''))) STORED,
    CONSTRAINT valid_status CHECK (status IN ('active', 'inactive', 'pending')),
    CONSTRAINT valid_range_check CHECK (valid_range IS NULL OR (LOWER(valid_range) < UPPER(valid_range)))
) WITH (fillfactor=90);

CREATE INDEX idx_comprehensive_search ON comprehensive_example USING GIN (search_vector);
CREATE INDEX idx_comprehensive_data ON comprehensive_example USING GIN (data);
CREATE INDEX idx_comprehensive_range ON comprehensive_example USING GIST (valid_range);

-- Add triggers for updated_at
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_timestamp
BEFORE UPDATE ON comprehensive_example
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

-- Comments
COMMENT ON TABLE comprehensive_example IS 'A comprehensive example table demonstrating PostgreSQL v17 features';
COMMENT ON COLUMN comprehensive_example.id IS 'Primary key with identity';
COMMENT ON COLUMN comprehensive_example.search_vector IS 'Generated column for full-text search';
COMMIT;
