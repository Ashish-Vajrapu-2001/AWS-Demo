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
    Returns list of active tables to process.
    Currently returns flat list sorted by priority.
    """
    db_secret_arn = os.environ['DB_SECRET_ARN']
    
    try:
        creds = get_secret(db_secret_arn)
        conn = psycopg2.connect(
            host=creds['host'],
            dbname=creds['dbname'],
            user=creds['username'],
            password=creds['password'],
            port=5432
        )
        cursor = conn.cursor()

        # Join with source systems to get connection details (if needed later)
        # Order by load_priority (ASC means process earlier)
        query = """
            SELECT 
                t.table_id,
                t.schema_name,
                t.table_name,
                t.primary_key_columns,
                t.initial_load_completed,
                t.last_sync_version,
                s.source_system_name as source_system,
                s.connection_name
            FROM control.table_metadata t
            JOIN control.source_systems s ON t.source_system_id = s.source_system_id
            WHERE t.is_active = TRUE AND s.is_active = TRUE
            ORDER BY t.load_priority ASC, t.table_id ASC
        """
        
        cursor.execute(query)
        
        tables = []
        for row in cursor.fetchall():
            tables.append({
                'table_id': row[0],
                'schema_name': row[1],
                'table_name': row[2],
                'primary_key_columns': row[3],
                'initial_load_completed': row[4],
                'last_sync_version': row[5],
                'source_system': row[6],
                'connection_name': row[7]
            })

        print(f"Found {len(tables)} active tables to process.")
        return tables

    except Exception as e:
        print(f"Error fetching table list: {str(e)}")
        raise e
    finally:
        if 'conn' in locals() and conn:
            conn.close()