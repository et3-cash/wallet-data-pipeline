from airflow import DAG
from airflow.providers.apache.airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from edges.etl_edge import extract_data_from_oltp, extract_data_from_json_csv, transform_data, load_to_dw

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

# Set task dependencies
extract_oltp_data >> extract_json_csv_data >> transform_data_task >> load_to_dw_task
