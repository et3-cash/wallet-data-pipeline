-- Create Users Table
CREATE TABLE users (
    user_id BIGSERIAL PRIMARY KEY,
    phone_number VARCHAR(15) NOT NULL,
    joined_date TIMESTAMPTZ NOT NULL,
    last_login TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL,
    is_admin BOOLEAN NOT NULL
);

-- Create Accounts Table
CREATE TABLE accounts (
    account_id BIGSERIAL PRIMARY KEY,
    balance NUMERIC(10, 2) NOT NULL,
    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE
);

-- Create Transactions Table
CREATE TABLE transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    transaction_type VARCHAR(20) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    description TEXT NOT NULL,
    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE
);

-- Insert Sample Users (Manually Generated)
INSERT INTO users (phone_number, joined_date, last_login, is_active, is_admin)
VALUES
    ('+11234567890', '2022-01-15 08:45:30', '2023-05-10 12:10:45', TRUE, FALSE),
    ('+11239876543', '2021-09-20 14:23:00', '2023-01-12 09:30:00', FALSE, FALSE),
    ('+11237651234', '2023-04-01 10:00:00', NULL, TRUE, TRUE),
    ('+11238765432', '2020-12-31 23:59:59', '2022-11-15 18:45:00', TRUE, FALSE);

-- Insert Sample Accounts (Linked to Users Above)
INSERT INTO accounts (balance, user_id)
VALUES
    (500.75, 1),
    (2000.00, 2),
    (-150.50, 3),
    (0.00, 4);

-- Insert Sample Transactions (Manually Generated)
INSERT INTO transactions (transaction_type, amount, created_at, description, user_id)
VALUES
    ('Deposit', 500.75, '2023-06-01 10:15:00', 'Initial deposit', 1),
    ('Withdrawal', -100.00, '2023-07-10 15:45:00', 'ATM withdrawal', 2),
    ('Transfer', -50.50, '2023-08-15 09:30:00', 'Transfer to savings', 3),
    ('Deposit', 200.00, '2023-09-20 12:00:00', 'Online deposit', 4);
