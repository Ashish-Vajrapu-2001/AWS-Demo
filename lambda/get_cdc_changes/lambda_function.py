import json
import boto3
import psycopg2
import os
from datetime import datetime

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def lambda_handler(event, context):
    """
    Retrieves CDC changes from Source DB (PostgreSQL trigger-based or Logical Decoding).
    Note: For MSSQL Sources, this would use pyodbc, but we stick to Postgres requirement.
    """
    db_secret_arn = os.environ['DB_SECRET_ARN']
    
    # Extract parameters
    schema_name = event.get('schema_name')
    table_name = event.get('table_name')
    last_sync_version = event.get('last_sync_version', 0)
    
    if not schema_name or not table_name:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing schema or table name')
        }

    try:
        # Get Credentials
        creds = get_secret(db_secret_arn)
        
        conn = psycopg2.connect(
            host=creds['host'],
            dbname=creds['dbname'],
            user=creds['username'],
            password=creds['password'],
            port=5432
        )
        
        cursor = conn.cursor()
        
        # Determine Source CDC Table (Assumption: created via pg_logical or audit table)
        # Using a pattern where changes are logged to {table}_cdc
        cdc_query = f"""
            SELECT * 
            FROM {schema_name}.{table_name}_cdc 
            WHERE lsn > %s 
            ORDER BY lsn ASC
        """
        
        cursor.execute(cdc_query, (last_sync_version,))
        
        columns = [desc[0] for desc in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
            
        # Serialize datetime objects
        def default_serializer(obj):
            if isinstance(obj, (datetime)):
                return obj.isoformat()
            return str(obj)
            
        return {
            'statusCode': 200,
            'changes': json.loads(json.dumps(results, default=default_serializer)),
            'count': len(results)
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }
    finally:
        if 'conn' in locals() and conn:
            conn.close()