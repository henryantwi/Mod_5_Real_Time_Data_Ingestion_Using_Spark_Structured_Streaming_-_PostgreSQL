-- PostgreSQL Setup Script for E-Commerce Events Pipeline
-- This script runs automatically when the PostgreSQL container starts

-- Create the events table
CREATE TABLE IF NOT EXISTS user_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(50) UNIQUE NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    product_category VARCHAR(100),
    product_price DECIMAL(10, 2),
    quantity INTEGER DEFAULT 1,
    event_timestamp TIMESTAMP NOT NULL,
    session_id VARCHAR(50),
    device_type VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_user_events_user_id ON user_events(user_id);
CREATE INDEX IF NOT EXISTS idx_user_events_event_type ON user_events(event_type);
CREATE INDEX IF NOT EXISTS idx_user_events_timestamp ON user_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_user_events_product_id ON user_events(product_id);

-- Create a view for quick analytics
CREATE OR REPLACE VIEW event_summary AS
SELECT 
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT product_id) as unique_products,
    SUM(product_price * quantity) as total_value
FROM user_events
GROUP BY event_type;

-- Grant permissions (if needed for different users)
GRANT ALL PRIVILEGES ON TABLE user_events TO lab_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO lab_user;

-- Display confirmation message
DO $$
BEGIN
    RAISE NOTICE 'Database setup completed successfully!';
    RAISE NOTICE 'Table "user_events" is ready to receive streaming data.';
END $$;
