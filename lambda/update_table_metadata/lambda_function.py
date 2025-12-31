import json
import boto3
import psycopg2
import os

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def lambda_handler(event, context):
    """
    Updates control.table_metadata and logs execution.
    CRITICAL: Sets initial_load_completed = True if applicable.
    """
    db_secret_arn = os.environ['DB_SECRET_ARN']
    
    # Extract parameters
    table_id = event.get('table_id')
    status = event.get('status') # 'SUCCESS' or 'FAILED'
    execution_id = event.get('execution_id')
    records_loaded = event.get('records_loaded', 0)
    mark_initial_complete = event.get('mark_initial_complete', False)
    new_sync_version = event.get('last_sync_version') # Optional
    error_message = event.get('error_message')

    if not table_id or not status:
        raise ValueError("Missing table_id or status")

    try:
        creds = get_secret(db_secret_arn)
        conn = psycopg2.connect(
            host=creds['host'],
            dbname=creds['dbname'],
            user=creds['username'],
            password=creds['password'],
            port=5432
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # 1. Update Metadata
        update_sql = """
            UPDATE control.table_metadata
            SET last_load_status = %s,
                last_load_timestamp = CURRENT_TIMESTAMP,
                last_execution_id = %s,
                records_loaded = %s,
                modified_date = CURRENT_TIMESTAMP
            WHERE table_id = %s
        """
        params = [status, execution_id, records_loaded, table_id]
        cursor.execute(update_sql, params)

        # 2. Handle Success Logic
        if status == 'SUCCESS':
            # Update Sync Version if provided
            if new_sync_version is not None:
                cursor.execute("""
                    UPDATE control.table_metadata
                    SET last_sync_version = %s
                    WHERE table_id = %s
                """, (new_sync_version, table_id))

            # CRITICAL: Flip Initial Load Flag
            if mark_initial_complete:
                print(f"Marking initial load complete for Table ID: {table_id}")
                cursor.execute("""
                    UPDATE control.table_metadata
                    SET initial_load_completed = TRUE
                    WHERE table_id = %s
                """, (table_id,))

        # 3. Log Execution
        log_sql = """
            INSERT INTO control.pipeline_execution_log 
            (execution_id, table_id, execution_status, records_processed, end_time, error_message)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
        """
        cursor.execute(log_sql, (execution_id, table_id, status, records_loaded, error_message))

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Metadata updated successfully'})
        }

    except Exception as e:
        print(f"Error updating metadata: {str(e)}")
        raise e
    finally:
        if 'conn' in locals() and conn:
            conn.close()