-- Create DimUsers Table (Dimension)
CREATE TABLE dim_users (
    user_id BIGINT PRIMARY KEY,
    phone_number VARCHAR(15),
    joined_date DATE,
    is_active BOOLEAN,
    is_admin BOOLEAN
);

-- Create DimDates Table (Dimension)
CREATE TABLE dim_dates (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    year INT,
    month INT,
    day INT,
    day_of_week VARCHAR(10)
);

-- Populate DimDates (Optional Helper)
INSERT INTO dim_dates (full_date, year, month, day, day_of_week)
SELECT DISTINCT
    created_at::DATE AS full_date,
    EXTRACT(YEAR FROM created_at) AS year,
    EXTRACT(MONTH FROM created_at) AS month,
    EXTRACT(DAY FROM created_at) AS day,
    TO_CHAR(created_at, 'Day') AS day_of_week
FROM transactions;

-- Create DimTransactionTypes Table (Dimension)
CREATE TABLE dim_transaction_types (
    transaction_type_id SERIAL PRIMARY KEY,
    transaction_type VARCHAR(20) UNIQUE
);

-- Populate DimTransactionTypes (Optional Helper)
INSERT INTO dim_transaction_types (transaction_type)
SELECT DISTINCT transaction_type
FROM transactions;

-- Create FactTransactions Table (Fact)
CREATE TABLE fact_transactions (
    transaction_id BIGINT PRIMARY KEY,
    transaction_type_id INT REFERENCES dim_transaction_types(transaction_type_id),
    user_id BIGINT REFERENCES dim_users(user_id),
    date_id INT REFERENCES dim_dates(date_id),
    amount NUMERIC(10, 2),
    description TEXT
);
