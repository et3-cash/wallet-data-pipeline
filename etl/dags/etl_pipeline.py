from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json

def extract_data_from_oltp(postgres_conn_id='oltp_db_conn'):
    """
    Extract data from the OLTP database (Postgres).
    
    Args:
        postgres_conn_id (str): Airflow connection ID for the Postgres database.

    Returns:
        dict: A dictionary with DataFrames for 'users', 'accounts', and 'transactions'.
    """
    try:
        # Establish connection to OLTP database
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = hook.get_conn()
        
        # Queries to extract data
        queries = {
            'users': "SELECT * FROM users;",
            'accounts': "SELECT * FROM accounts;",
            'transactions': "SELECT * FROM transactions;"
        }

        data_frames = {key: pd.read_sql_query(sql, conn) for key, sql in queries.items()}
        
        return data_frames

    except Exception as e:
        print(f"Error extracting data from OLTP: {e}")
        raise

    finally:
        # Ensure the connection is closed
        if 'conn' in locals():
            conn.close()

def extract_data_from_json_csv():
    with open('/opt/airflow/dags/data/accounts.json') as f:  
        accounts_json = json.load(f)
    accounts_json_df = pd.DataFrame(accounts_json)
    
    transactions_csv = pd.read_csv('/opt/airflow/dags/data/transactions.csv')  
    
    return {
        'accounts_json': accounts_json_df,
        'transactions_csv': transactions_csv
    }

def transform_data(**context):
    import pandas as pd
    
    # Retrieve OLTP data from XCom (from task 'extract_oltp_data')
    users_df = context['ti'].xcom_pull(task_ids='extract_oltp_data')['users']
    accounts_df = context['ti'].xcom_pull(task_ids='extract_oltp_data')['accounts']
    transactions_df = context['ti'].xcom_pull(task_ids='extract_oltp_data')['transactions']
    
    # Retrieve JSON and CSV data from XCom (from task 'extract_json_csv_data')
    accounts_json_df = context['ti'].xcom_pull(task_ids='extract_json_csv_data')['accounts_json']
    transactions_csv_df = context['ti'].xcom_pull(task_ids='extract_json_csv_data')['transactions_csv']
    
    # Combine the data (merging OLTP data with the extracted JSON and CSV data)
    accounts_df = pd.concat([accounts_df, accounts_json_df], ignore_index=True)
    transactions_df = pd.concat([transactions_df, transactions_csv_df], ignore_index=True)

    # Drop duplicates
    accounts_df = accounts_df.drop_duplicates(subset=['account_id'])
    transactions_df = transactions_df.drop_duplicates(subset=['transaction_id', 'user_id'])
    
    # Ensure that the 'balance' column is numeric before comparison
    accounts_df['balance'] = pd.to_numeric(accounts_df['balance'], errors='coerce')
    accounts_df = accounts_df[accounts_df['balance'] >= 0]  # Remove negative balances
    
    # Fix column name typo
    users_df.rename(columns={'phone_numer': 'phone_number'}, inplace=True)
    
    # Merge the data (transactions + accounts + users)
    merged_df = pd.merge(transactions_df, accounts_df, on='user_id', how='inner')
    merged_df = pd.merge(merged_df, users_df, on='user_id', how='inner')
    
    # convert datetime columns to string to be serializable
    merged_df['created_at'] = merged_df['created_at'].astype(str)
    merged_df['joined_date'] = merged_df['joined_date'].astype(str)
    merged_df['last_login'] = merged_df['last_login'].astype(str)

    
    # Handle potential NaN values by converting them to None (so they are serializable)
    merged_df = merged_df.where(pd.notnull(merged_df), None)
    
    # Extract unique transaction types for separate loading
    unique_transaction_types = merged_df['transaction_type'].unique()
    
    # Serialize the data
    serialized_data = {
        "merged_data": merged_df.to_dict(orient="records"),  # Convert DataFrame to list of dictionaries
        "unique_transaction_types": list(unique_transaction_types)  # Convert NumPy array to list
    }
    
    return serialized_data



def load_to_dw(**context):
    # Pull serialized data
    transformed_data = context['ti'].xcom_pull(task_ids='transform_data')
    merged_data = transformed_data["merged_data"]
    unique_transaction_types = transformed_data["unique_transaction_types"]
    
    # Load merged data to dw-db (Postgres)
    hook = PostgresHook(postgres_conn_id='dw_db_conn')

    # First, insert unique transaction types into dim_transaction_types table
    for transaction_type in unique_transaction_types:
        hook.run("""
            INSERT INTO dim_transaction_types (transaction_type)
            VALUES (%s)
            ON CONFLICT (transaction_type) DO NOTHING
        """, parameters=(transaction_type,))
    
    # Now insert unique user IDs into dim_users table
    unique_user_ids = {row['user_id'] for row in merged_data}  # Set of unique user IDs
    for user_id in unique_user_ids:
        hook.run("""
            INSERT INTO dim_users (user_id)
            VALUES (%s)
            ON CONFLICT (user_id) DO NOTHING
        """, parameters=(user_id,))
    
    # Now insert merged transaction data into fact_transactions table
    for row in merged_data:
        hook.run("""
            INSERT INTO fact_transactions (transaction_id, user_id, amount, transaction_type)
            VALUES (%s, %s, %s, %s)
        """, parameters=(row['transaction_id'], row['user_id'], row['amount'], row['transaction_type']))






# Define the DAG
dag = DAG(
    "etl_pipeline",
    description="ETL Pipeline for Data Warehouse",
    schedule_interval=None,  # No automatic scheduling, run manually or trigger
    start_date=days_ago(1),
    catchup=False,
)

# Task 1: Extract data from OLTP (Postgres DB), CSV, and JSON files
extract_oltp_data = PythonOperator(
    task_id='extract_oltp_data',
    python_callable=extract_data_from_oltp,
    dag=dag,
)

extract_json_csv_data = PythonOperator(
    task_id='extract_json_csv_data',
    python_callable=extract_data_from_json_csv,
    dag=dag,
)

# Task 2: Transform data
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Task 3: Load data into Data Warehouse (dw-db)
load_to_dw_task = PythonOperator(
    task_id='load_to_dw',
    python_callable=load_to_dw,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
[extract_oltp_data, extract_json_csv_data] >> transform_data_task >> load_to_dw_task
