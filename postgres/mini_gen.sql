-- mini_gen.sql
-- Insert sample data into mini_postgres_data_types using CTE for batch generation

WITH batch_insert AS (
    SELECT 
        'Item ' || n AS name,
        'Auto-generated test item ' || n AS description,
        (random() * 1000)::INTEGER AS integer_val,
        (random() * 1000000000000000)::BIGINT AS bigint_val,
        (random() * 10000)::NUMERIC(12,2) AS numeric_val,
        'This is auto-generated content for item ' || n AS text_val,
        'item-' || n || '-' || substr(md5(random()::text), 1, 8) AS varchar_val,
        (random() > 0.5) AS boolean_val,
        CURRENT_DATE - (n * 2)::INTEGER AS date_val,
        NOW() - (n * INTERVAL '1 day') AS timestamp_val,
        jsonb_build_object(
            'id', n,
            'status', (ARRAY['active','inactive','pending'])[floor(random() * 3 + 1)],
            'score', (random() * 10)::NUMERIC(2,1),
            'tags', (SELECT array_agg('tag' || s) FROM generate_series(1, (random() * 5)::INT) AS s)
        ) AS jsonb_val,
        (SELECT array_agg(floor(random() * 100)::INTEGER) 
         FROM generate_series(1, (random() * 10 + 1)::INT)) AS integer_array,
        NOW() - (n * INTERVAL '1 hour') AS created_at,
        NOW() - (n * INTERVAL '30 minute') AS updated_at
    FROM generate_series(1, 20) AS n  -- Generate 20 rows by default
)
INSERT INTO mini_postgres_data_types (
    name, 
    description,
    integer_val,
    bigint_val,
    numeric_val,
    text_val,
    varchar_val,
    boolean_val,
    date_val,
    timestamp_val,
    jsonb_val,
    integer_array,
    created_at,
    updated_at
)
SELECT 
    name, 
    description,
    integer_val,
    bigint_val,
    numeric_val,
    text_val,
    varchar_val,
    boolean_val,
    date_val,
    timestamp_val,
    jsonb_val,
    integer_array,
    created_at,
    updated_at
FROM batch_insert;

-- Insert some fixed test data as well
INSERT INTO mini_postgres_data_types (
    name, 
    description,
    integer_val,
    bigint_val,
    numeric_val,
    text_val,
    varchar_val,
    boolean_val,
    date_val,
    timestamp_val,
    jsonb_val,
    integer_array
) VALUES 
    ('Sample Product', 'A test product entry', 
     42, 
     9223372036854775807, 
     1234.56, 
     'This is a sample text field', 
     'sample-product', 
     true, 
     '2025-05-18', 
     '2025-05-18 15:30:00+05:30',
     '{"key": "value", "nested": {"a": 1, "b": 2}, "tags": ["tag1", "tag2"]}',
     ARRAY[1, 2, 3, 4, 5]
    ),
    ('Another Item', 'Another test entry with different values', 
     100, 
     123456789012345, 
     789.10, 
     'Another text field with more content', 
     'another-item', 
     false, 
     '2025-05-17', 
     '2025-05-17 10:15:00+05:30',
     '{"status": "active", "metadata": {"views": 150, "rating": 4.5}, "categories": ["electronics", "gadgets"]}',
     ARRAY[10, 20, 30, 40, 50]
    ),
    ('Test Record', 'A third test record', 
     -500, 
     -9223372036854775808, 
     -99.99, 
     'Negative values test', 
     'negative-test', 
     true, 
     '2025-01-01', 
     '2025-01-01 00:00:00+00:00',
     '{"active": true, "count": 0, "data": null}',
     ARRAY[-1, 0, 1]
    );