import sys
import boto3
import psycopg2
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'db_secret_arn', 'bucket_name'])
job.init(args['JOB_NAME'], args)

def validate_rds_connectivity(secret_arn):
    print(f"Testing connectivity to Control DB via Secret: {secret_arn}")
    try:
        sm = boto3.client('secretsmanager')
        secret = sm.get_secret_value(SecretId=secret_arn)
        creds = eval(secret['SecretString']) # using eval for json-like dict string
        
        conn = psycopg2.connect(
            host=creds['host'],
            dbname=creds['dbname'],
            user=creds['username'],
            password=creds['password'],
            port=5432,
            connect_timeout=5
        )
        print("PASS: Control DB Connectivity")
        conn.close()
        return True
    except Exception as e:
        print(f"FAIL: Control DB Connectivity - {str(e)}")
        return False

def validate_s3_access(bucket_name):
    print(f"Testing write access to S3 Bucket: {bucket_name}")
    try:
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key='validation_test.txt', Body='test')
        s3.delete_object(Bucket=bucket_name, Key='validation_test.txt')
        print("PASS: S3 Write Access")
        return True
    except Exception as e:
        print(f"FAIL: S3 Write Access - {str(e)}")
        return False

def validate_delta_libs():
    try:
        from delta.tables import DeltaTable
        print("PASS: Delta Lake Libraries Installed")
        return True
    except ImportError:
        print("FAIL: Delta Lake Libraries NOT Found")
        return False

# Run Validations
print("--- STARTING ENVIRONMENT VALIDATION ---")
results = []
results.append(validate_rds_connectivity(args['db_secret_arn']))
results.append(validate_s3_access(args['bucket_name']))
results.append(validate_delta_libs())

if all(results):
    print("--- ALL CHECKS PASSED ---")
    job.commit()
else:
    print("--- VALIDATION FAILED ---")
    sys.exit(1)