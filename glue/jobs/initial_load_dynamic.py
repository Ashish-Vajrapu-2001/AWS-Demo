import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, current_timestamp
from delta.tables import *

# Initialize Glue Context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'execution_id',
    'table_id',
    'schema_name',
    'table_name',
    'source_system',
    'db_secret_arn',
    'bucket_name',
    'job-bookmark-option'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Configuration ---
execution_id = args['execution_id']
table_id = args['table_id']
schema_name = args['schema_name']
table_name = args['table_name']
source_system = args['source_system']
bucket_name = args['bucket_name']
db_secret_arn = args['db_secret_arn']

# Get Credentials
secrets_client = boto3.client('secretsmanager')
secret_response = secrets_client.get_secret_value(SecretId=db_secret_arn)
secret = json.loads(secret_response['SecretString'])
jdbc_url = f"jdbc:sqlserver://{secret['host']}:1433;databaseName={secret['dbname']}"
jdbc_user = secret['username']
jdbc_password = secret['password']

print(f"Starting Initial Load for {schema_name}.{table_name} (Run: {execution_id})")

try:
    # --- 1. Read from Source (JDBC) with Bookmarks ---
    # Note: transformation_ctx enables bookmarks
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="sqlserver",
        connection_options={
            "url": jdbc_url,
            "user": jdbc_user,
            "password": jdbc_password,
            "dbtable": f"{schema_name}.{table_name}"
        },
        transformation_ctx=f"bronze_{schema_name}_{table_name}_initial"
    )

    if datasource.count() == 0:
        print("No records found in source table.")
    else:
        # Convert to DataFrame
        df = datasource.toDF()

        # --- 2. Add Metadata Columns ---
        df_transformed = df \
            .withColumn("_cdc_operation", lit("F")) \
            .withColumn("_pipeline_run_id", lit(execution_id)) \
            .withColumn("_ingest_timestamp", current_timestamp())

        # --- 3. Write to Staging (Parquet) - Optional for debugging/audit ---
        staging_path = f"s3://{bucket_name}/bronze/staging/{source_system}/{schema_name}/{table_name}/initial/{execution_id}/"
        df_transformed.write.mode("overwrite").parquet(staging_path)
        print(f"Wrote staging data to {staging_path}")

        # --- 4. Write to Delta Lake (Target) ---
        delta_path = f"s3://{bucket_name}/bronze/{source_system}/{schema_name}/{table_name}/"
        df_transformed.write.format("delta").mode("overwrite").save(delta_path)
        print(f"Wrote Delta table to {delta_path}")

    job.commit()

except Exception as e:
    print(f"Error in initial load: {str(e)}")
    # The Step Function will catch this failure
    raise e