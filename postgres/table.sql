-- PostgreSQL v17 Data Types Demonstration
-- This file contains examples of all PostgreSQL data types

BEGIN;
CREATE TYPE mood_enum AS ENUM ('happy', 'sad', 'neutral');
CREATE TYPE complex_type AS (x INTEGER, y TEXT);
CREATE DOMAIN positive_integer AS INTEGER CHECK (VALUE > 0);

CREATE TABLE postgres_data_types (
    id SERIAL PRIMARY KEY,
    type_name TEXT NOT NULL,

    -- Numeric Types
    smallint_val SMALLINT,
    integer_val INTEGER,
    bigint_val BIGINT,
    decimal_val DECIMAL(10,2),
    numeric_val NUMERIC(10,2),
    real_val REAL,
    double_precision_val DOUBLE PRECISION,
    smallserial_val SMALLSERIAL,
    serial_val SERIAL,
    bigserial_val BIGSERIAL,

    -- Monetary Type
    money_val MONEY,

    -- Character Types
    char_val CHAR(10),
    varchar_val VARCHAR(100),
    text_val TEXT,

    -- Binary Data Types
    bytea_val BYTEA,

    -- Date/Time Types
    timestamp_val TIMESTAMP,
    timestamptz_val TIMESTAMP WITH TIME ZONE,
    date_val DATE,
    time_val TIME,
    timetz_val TIME WITH TIME ZONE,
    interval_val INTERVAL,

    -- Boolean Type
    boolean_val BOOLEAN,

    -- Enumerated Type
    enum_mood_val mood_enum,

    -- Geometric Types
    point_val POINT,
    line_val LINE,
    lseg_val LSEG,
    box_val BOX,
    path_val PATH,
    polygon_val POLYGON,
    circle_val CIRCLE,

    -- Network Address Types
    cidr_val CIDR,
    inet_val INET,
    macaddr_val MACADDR,
    macaddr8_val MACADDR8,

    -- Bit String Types
    bit_val BIT(8),
    varbit_val BIT VARYING(64),

    -- Text Search Types
    tsvector_val TSVECTOR,
    tsquery_val TSQUERY,

    -- UUID Type
    uuid_val UUID,

    -- XML Type
    xml_val XML,

    -- JSON Types
    json_val JSON,
    jsonb_val JSONB,
    jsonpath_val JSONPATH,

    -- Array Types
    integer_array_val INTEGER[],
    text_array_val TEXT[],
    multidim_array_val INTEGER[][],

    -- Composite Types
    composite_val complex_type,

    -- Range Types
    int4range_val INT4RANGE,
    int8range_val INT8RANGE,
    numrange_val NUMRANGE,
    tsrange_val TSRANGE,
    tstzrange_val TSTZRANGE,
    daterange_val DATERANGE,

    -- Multirange Types
    int4multirange_val INT4MULTIRANGE,
    int8multirange_val INT8MULTIRANGE,
    nummultirange_val NUMMULTIRANGE,
    tsmultirange_val TSMULTIRANGE,
    tstzmultirange_val TSTZMULTIRANGE,
    datemultirange_val DATEMULTIRANGE,

    -- Domain Types
    domain_val positive_integer,

    -- Object Identifier Types
    oid_val OID,
    regclass_val REGCLASS,
    regproc_val REGPROC,
    regprocedure_val REGPROCEDURE,
    regoper_val REGOPER,
    regoperator_val REGOPERATOR,
    regtype_val REGTYPE,
    regrole_val REGROLE,
    regnamespace_val REGNAMESPACE,
    regconfig_val REGCONFIG,
    regdictionary_val REGDICTIONARY,

    -- pg_lsn Type
    pg_lsn_val PG_LSN,

    -- New in PostgreSQL 16/17
    xid8_val XID8
);

COMMIT;
