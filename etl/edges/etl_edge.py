from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json

# Function to extract data from OLTP database (Postgres)
def extract_data_from_oltp():
    # Establish connection to OLTP database
    hook = PostgresHook(postgres_conn_id='oltp_db_conn')
    
    # Extract data from 'users', 'accounts', 'transactions' tables
    sql = "SELECT * FROM users;"
    users_df = hook.get_pandas_df(sql)
    
    sql = "SELECT * FROM accounts;"
    accounts_df = hook.get_pandas_df(sql)
    
    sql = "SELECT * FROM transactions;"
    transactions_df = hook.get_pandas_df(sql)
    
    return {
        'users': users_df,
        'accounts': accounts_df,
        'transactions': transactions_df
    }

# Function to extract data from CSV and JSON files
def extract_data_from_json_csv():
    # Extract CSV and JSON data from files
    with open('/opt/airflow/dags/data/accounts.json') as f:  # Path updated to mounted folder in the container
        accounts_json = json.load(f)
    accounts_json_df = pd.DataFrame(accounts_json)
    
    transactions_csv = pd.read_csv('/opt/airflow/dags/data/transactions.csv')  # Path updated
    
    return {
        'accounts_json': accounts_json_df,
        'transactions_csv': transactions_csv
    }

# Function to transform and clean the data
def transform_data(**context):
    users_df = context['ti'].xcom_pull(task_ids='extract_oltp_data')['users']
    accounts_df = context['ti'].xcom_pull(task_ids='extract_oltp_data')['accounts']
    transactions_df = context['ti'].xcom_pull(task_ids='extract_oltp_data')['transactions']
    
    # Example transformations
    accounts_df = accounts_df[accounts_df['balance'] >= 0]  # Remove negative balances
    users_df.rename(columns={'phone_numer': 'phone_number'}, inplace=True)  # Fix column name typo
    
    # Merge the data
    merged_df = pd.merge(transactions_df, accounts_df, on='account_id', how='inner')
    merged_df = pd.merge(merged_df, users_df, on='user_id', how='inner')
    
    # Extract unique transaction types for separate loading
    unique_transaction_types = merged_df['transaction_type'].unique()
    
    return merged_df, unique_transaction_types

# Function to load the data into the Data Warehouse
def load_to_dw(**context):
    merged_df, unique_transaction_types = context['ti'].xcom_pull(task_ids='transform_data')
    
    # Load merged data to dw-db (Postgres)
    hook = PostgresHook(postgres_conn_id='dw_db_conn')
    
    # Insert merged transaction data into fact_transactions table
    for index, row in merged_df.iterrows():
        hook.run("""
            INSERT INTO fact_transactions (transaction_id, user_id, account_id, amount, transaction_type)
            VALUES (%s, %s, %s, %s, %s)
        """, parameters=(row['transaction_id'], row['user_id'], row['account_id'], row['amount'], row['transaction_type']))
    
    # Insert unique transaction types into dim_transaction_types table
    for transaction_type in unique_transaction_types:
        hook.run("""
            INSERT INTO dim_transaction_types (transaction_type)
            VALUES (%s)
        """, parameters=(transaction_type,))
