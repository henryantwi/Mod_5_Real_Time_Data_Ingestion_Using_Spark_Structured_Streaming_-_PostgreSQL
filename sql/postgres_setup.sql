-- PostgreSQL Setup Script for E-Commerce Events Pipeline
-- This script runs automatically when the PostgreSQL container starts

-- Enable UUID extension (required for UUID datatype)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create the events table
CREATE TABLE IF NOT EXISTS user_events (
    id SERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    user_id UUID NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    product_category VARCHAR(100),
    product_price DECIMAL(10, 2),
    quantity INTEGER DEFAULT 1,
    event_timestamp TIMESTAMP NOT NULL,
    session_id UUID,
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

-- ============================================
-- Hourly Aggregations Table (for Watermarking Demo)
-- ============================================
-- Stores windowed aggregations computed by Spark Streaming
-- Uses composite unique constraint for upsert support

CREATE TABLE IF NOT EXISTS event_hourly_summary (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 0,
    total_revenue DECIMAL(12, 2) DEFAULT 0,
    unique_users INTEGER DEFAULT 0,
    unique_products INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(window_start, window_end, event_type)
);

-- Index for efficient time-range queries
CREATE INDEX IF NOT EXISTS idx_hourly_summary_window ON event_hourly_summary(window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_hourly_summary_event_type ON event_hourly_summary(event_type);

-- Grant permissions for aggregation table
GRANT ALL PRIVILEGES ON TABLE event_hourly_summary TO lab_user;

-- Display confirmation message
DO $$
BEGIN
    RAISE NOTICE 'Database setup completed successfully!';
    RAISE NOTICE 'Table "user_events" is ready to receive streaming data.';
    RAISE NOTICE 'UUID columns: event_id, user_id, session_id';
END $$;
