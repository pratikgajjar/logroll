-- mini_postgres_data_types.sql
BEGIN;

-- Create the minimal table with most commonly used data types
CREATE TABLE mini_postgres_data_types (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    
    -- Most commonly used data types
    integer_val INTEGER,
    bigint_val BIGINT,
    numeric_val NUMERIC(12, 2),
    text_val TEXT,
    varchar_val VARCHAR(255),
    boolean_val BOOLEAN,
    date_val DATE,
    timestamp_val TIMESTAMP WITH TIME ZONE,
    jsonb_val JSONB,
    integer_array INTEGER[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add a comment
COMMENT ON TABLE mini_postgres_data_types IS 'Minimal version of postgres_data_types with most commonly used data types';

-- Create an index on the name column
CREATE INDEX idx_mini_postgres_data_types_name ON mini_postgres_data_types(name);

-- Add a trigger to update the updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_mini_postgres_data_types_updated_at
BEFORE UPDATE ON mini_postgres_data_types
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMIT;