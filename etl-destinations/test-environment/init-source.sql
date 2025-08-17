-- Initialize source database for integration testing

-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;

-- Create test schema
CREATE SCHEMA IF NOT EXISTS test_schema;

-- Create various test tables with different data types
CREATE TABLE test_schema.users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email TEXT UNIQUE,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB,
    score NUMERIC(10, 2),
    tags TEXT[]
);

CREATE TABLE test_schema.products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    quantity INTEGER DEFAULT 0,
    category VARCHAR(100),
    created_date DATE DEFAULT CURRENT_DATE,
    last_modified TIME WITH TIME ZONE,
    attributes JSONB,
    image_data BYTEA
);

CREATE TABLE test_schema.events (
    event_id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_data JSON,
    occurred_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT false,
    processing_time INTERVAL,
    source_ip INET,
    mac_address MACADDR,
    location POINT
);

-- Create publication for logical replication
CREATE PUBLICATION etl_test_publication FOR ALL TABLES;

-- Create replication slot
SELECT pg_create_logical_replication_slot('etl_test_slot', 'pgoutput');

-- Insert sample data
INSERT INTO test_schema.users (username, email, metadata, score, tags) VALUES
    ('john_doe', 'john@example.com', '{"role": "admin", "department": "IT"}', 95.5, ARRAY['admin', 'developer']),
    ('jane_smith', 'jane@example.com', '{"role": "user", "department": "Sales"}', 87.3, ARRAY['sales', 'manager']),
    ('bob_wilson', 'bob@example.com', NULL, 92.0, ARRAY['support']);

INSERT INTO test_schema.products (name, description, price, quantity, category, attributes) VALUES
    ('Laptop', 'High-performance laptop', 1299.99, 50, 'Electronics', '{"brand": "TechCorp", "warranty": "2 years"}'),
    ('Mouse', 'Wireless mouse', 29.99, 200, 'Accessories', '{"color": "black", "connectivity": "bluetooth"}'),
    ('Keyboard', 'Mechanical keyboard', 89.99, 75, 'Accessories', '{"switches": "blue", "backlight": true}');

INSERT INTO test_schema.events (event_type, event_data, source_ip, mac_address) VALUES
    ('user_login', '{"user_id": 1, "session": "abc123"}', '192.168.1.100', '00:1B:44:11:3A:B7'),
    ('page_view', '{"page": "/products", "duration": 45}', '10.0.0.50', '00:1B:44:11:3A:B8'),
    ('purchase', '{"product_id": 1, "quantity": 2}', '172.16.0.25', '00:1B:44:11:3A:B9');

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON SCHEMA test_schema TO replication_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA test_schema TO replication_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA test_schema TO replication_user;
GRANT REPLICATION TO replication_user;