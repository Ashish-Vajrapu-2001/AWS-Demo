import boto3
import json
import os
import urllib.parse

s3 = boto3.client('s3')
sns = boto3.client('sns')

def lambda_handler(event, context):
    """
    Triggered by S3 PutObject in the DLQ prefix.
    Sends an alert via SNS.
    """
    topic_arn = os.environ.get('SNS_TOPIC_ARN')
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        # Expected path: dlq/bronze/{table_name}/{execution_id}/...
        try:
            parts = key.split('/')
            layer = parts[1] if len(parts) > 1 else "unknown"
            table_name = parts[2] if len(parts) > 2 else "unknown"
            execution_id = parts[3] if len(parts) > 3 else "unknown"
            
            message = f"""
CRITICAL: Data Quality Failure Detected

Layer: {layer}
Table: {table_name}
Execution ID: {execution_id}
DLQ Location: s3://{bucket}/{key}

Action Required: Review failed records in S3 and check pipeline logs.
            """
            
            print(f"Publishing alert for {key}")
            
            if topic_arn:
                sns.publish(
                    TopicArn=topic_arn,
                    Subject=f'Data Quality Failure - {layer}.{table_name}',
                    Message=message
                )
            else:
                print("SNS_TOPIC_ARN not set. Skipping alert.")
                
        except Exception as e:
            print(f"Error processing record {key}: {str(e)}")
            continue

    return {'statusCode': 200}