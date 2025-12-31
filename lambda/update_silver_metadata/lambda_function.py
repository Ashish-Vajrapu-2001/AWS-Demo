import json
import os
import boto3
import psycopg2
from datetime import datetime

def get_db_connection():
    secret_name = os.environ['DB_SECRET_NAME']
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(get_secret_value_response['SecretString'])
    
    conn = psycopg2.connect(
        host=secret['host'],
        database=secret['dbname'],
        user=secret['username'],
        password=secret['password']
    )
    return conn

def lambda_handler(event, context):
    """
    Updates control.table_metadata_silver and logs execution
    Input: { "table_id": 1, "status": "SUCCESS", "pipeline_run_id": "exec-123" }
    """
    table_id = event['table_id']
    status = event['status']
    run_id = event['pipeline_run_id']
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Update Metadata
        update_sql = """
            UPDATE control.table_metadata_silver
            SET last_load_status = %s,
                last_load_timestamp = %s,
                silver_initial_load_completed = '1'
            WHERE table_id = %s
        """
        cur.execute(update_sql, (status, datetime.now(), table_id))
        
        # Insert Log
        log_sql = """
            INSERT INTO control.silver_execution_log 
            (pipeline_run_id, table_id, end_time, status)
            VALUES (%s, %s, %s, %s)
        """
        cur.execute(log_sql, (run_id, table_id, datetime.now(), status))
        
        conn.commit()
        return {"statusCode": 200, "body": "Metadata updated"}
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()