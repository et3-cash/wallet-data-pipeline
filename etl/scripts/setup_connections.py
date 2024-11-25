from airflow import settings
from airflow.models import Connection
from airflow.utils.session import create_session

def create_or_update_connection(conn_id, conn_type, host, schema, login, password, port):
    session = settings.Session()
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if existing_conn:
        # Update the existing connection with new parameters
        existing_conn.conn_type = conn_type
        existing_conn.host = host
        existing_conn.schema = schema
        existing_conn.login = login
        existing_conn.password = password
        existing_conn.port = port
        session.commit()
        print(f"Updated existing connection: {conn_id}")
    else:
        # Create a new connection if it doesn't exist
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            schema=schema,
            login=login,
            password=password,
            port=port
        )
        session.add(new_conn)
        session.commit()
        print(f"Created new connection: {conn_id}")

# Use this function to create or update the connections
create_or_update_connection('oltp_db_conn', 'postgres', 'oltp-db', 'oltp_db', 'admin', 'password', 5432)
create_or_update_connection('dw_db_conn', 'postgres', 'dw-db', 'dw_db', 'admin', 'password', 5432)
