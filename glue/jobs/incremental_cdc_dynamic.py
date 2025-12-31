import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, current_timestamp, col, row_number
from pyspark.sql.window import Window
from delta.tables import *

# Initialize Glue
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'execution_id',
    'table_id',
    'schema_name',
    'table_name',
    'primary_key_columns',
    'source_system',
    'db_secret_arn',
    'bucket_name',
    'last_sync_version',
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
pk_columns = args['primary_key_columns'].split(',')
source_system = args['source_system']
bucket_name = args['bucket_name']
last_sync_version = int(args['last_sync_version'])
db_secret_arn = args['db_secret_arn']

# Get Credentials
secrets_client = boto3.client('secretsmanager')
secret = json.loads(secrets_client.get_secret_value(SecretId=db_secret_arn)['SecretString'])
jdbc_url = f"jdbc:sqlserver://{secret['host']}:1433;databaseName={secret['dbname']}"

print(f"Starting Incremental Load for {schema_name}.{table_name} from LSN {last_sync_version}")

try:
    # --- 1. Fetch CDC Changes ---
    # Construct query to call SQL Server native CDC function
    # Note: Requires sysadmin to have enabled CDC on source
    cdc_query = f"""
        SELECT 
            sys.fn_cdc_map_lsn_to_time(__$start_lsn) as _change_timestamp,
            __$operation as _cdc_operation_code,
            __$start_lsn as _cdc_version,
            *
        FROM cdc.fn_cdc_get_all_changes_{schema_name}_{table_name}(
            sys.fn_cdc_map_time_to_lsn('smallest greater than', DATEADD(day, -3, GETDATE())), -- Safety window
            sys.fn_cdc_map_time_to_lsn('largest less than or equal', GETDATE()),
            'all'
        )
        WHERE __$start_lsn > {last_sync_version}
    """
    
    # Note: For strict LSN based query, we usually bind parameters, but DynamicFrame JDBC string requires embedded value
    # Better approach in production: Use a stored procedure or explicit LSN bounds passed in args
    
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="sqlserver",
        connection_options={
            "url": jdbc_url,
            "user": secret['username'],
            "password": secret['password'],
            "query": cdc_query
        },
        transformation_ctx=f"bronze_{schema_name}_{table_name}_cdc"
    )

    if datasource.count() == 0:
        print("No new changes detected.")
        job.commit()
        sys.exit(0)

    # --- 2. Transform & Deduplicate ---
    df = datasource.toDF()
    
    # Map SQL Server CDC operations: 1=Delete, 2=Insert, 3=Before Update, 4=After Update
    # Simplifying to I, U, D
    df_clean = df.withColumn("_cdc_operation", 
        when(col("_cdc_operation_code") == 1, "DELETE")
        .when(col("_cdc_operation_code") == 2, "INSERT")
        .when(col("_cdc_operation_code") == 3, "UPDATE_BEFORE")
        .when(col("_cdc_operation_code") == 4, "UPDATE")
        .otherwise("UNKNOWN")
    ).filter("_cdc_operation != 'UPDATE_BEFORE'") # We only need the 'after' image for updates

    # Deduplicate: If multiple changes for same PK in batch, take latest LSN
    window_spec = Window.partitionBy(*pk_columns).orderBy(col("_cdc_version").desc())
    df_deduped = df_clean.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") == 1).drop("rank") \
        .withColumn("_pipeline_run_id", lit(execution_id)) \
        .withColumn("_ingest_timestamp", current_timestamp())

    # --- 3. Write Staging (Incremental) ---
    staging_path = f"s3://{bucket_name}/bronze/staging/{source_system}/{schema_name}/{table_name}/incremental/{execution_id}/"
    df_deduped.write.mode("overwrite").parquet(staging_path)

    # --- 4. MERGE into Delta Lake ---
    delta_path = f"s3://{bucket_name}/bronze/{source_system}/{schema_name}/{table_name}/"
    target_table = DeltaTable.forPath(spark, delta_path)

    # Construct Dynamic Merge Condition
    merge_condition = " AND ".join([f"target.{pk.strip()} = source.{pk.strip()}" for pk in pk_columns])
    
    # Get all columns except metadata for update/insert
    data_columns = [c for c in df_deduped.columns if not c.startswith("_") and c not in ['__$start_lsn', '__$seqval', '__$operation', '__$update_mask']]
    
    update_mapping = {c: f"source.{c}" for c in data_columns}
    update_mapping["_ingest_timestamp"] = "current_timestamp()"
    update_mapping["_pipeline_run_id"] = f"'{execution_id}'"
    update_mapping["_cdc_operation"] = "source._cdc_operation"

    print(f"Executing MERGE on {delta_path} with condition: {merge_condition}")
    
    target_table.alias("target").merge(
        df_deduped.alias("source"),
        merge_condition
    ).whenMatchedDelete(
        condition="source._cdc_operation = 'DELETE'"
    ).whenMatchedUpdate(
        condition="source._cdc_operation != 'DELETE'",
        set=update_mapping
    ).whenNotMatchedInsert(
        condition="source._cdc_operation != 'DELETE'",
        values=update_mapping
    ).execute()

    # --- 5. Optimize ---
    spark.sql(f"OPTIMIZE delta.`{delta_path}` ZORDER BY ({','.join(pk_columns)})")
    
    job.commit()

except Exception as e:
    print(f"Error in CDC merge: {str(e)}")
    raise e