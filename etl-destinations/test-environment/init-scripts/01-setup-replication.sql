-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;

-- Create replication user
CREATE USER replicator WITH REPLICATION LOGIN PASSWORD 'repl_password';

-- Grant necessary permissions
GRANT USAGE ON SCHEMA public TO replicator;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO replicator;

-- Create test tables
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    age INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product_name TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    price NUMERIC(10, 2) NOT NULL,
    order_date DATE DEFAULT CURRENT_DATE,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create publication for CDC
CREATE PUBLICATION etl_publication FOR ALL TABLES;

-- Insert initial test data
INSERT INTO users (name, email, age) VALUES
    ('Alice Johnson', 'alice@example.com', 28),
    ('Bob Smith', 'bob@example.com', 35),
    ('Charlie Brown', 'charlie@example.com', 42);

INSERT INTO orders (user_id, product_name, quantity, price) VALUES
    (1, 'Laptop', 1, 1299.99),
    (1, 'Mouse', 2, 29.99),
    (2, 'Keyboard', 1, 89.99),
    (3, 'Monitor', 2, 399.99);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for users table
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();