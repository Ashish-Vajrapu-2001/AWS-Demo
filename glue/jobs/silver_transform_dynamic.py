import sys
import boto3
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Argument Parsing
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'table_id', 'pipeline_run_id', 
    'db_secret_name', 'bronze_bucket'
])

# Initialize Spark & Glue with Delta support
spark = SparkSession.builder \
    .appName(f"Silver_Transform_{args['table_id']}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def get_db_connection(secret_name):
    """Retrieve DB credentials from Secrets Manager and return connection props"""
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name='us-east-1')
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(get_secret_value_response['SecretString'])
    
    url = f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}"
    properties = {
        "user": secret['username'],
        "password": secret['password'],
        "driver": "org.postgresql.Driver"
    }
    return url, properties

def fetch_metadata(table_id, url, props):
    """Fetch table config and DQ rules"""
    # Table Config
    query_config = f"SELECT * FROM control.table_metadata_silver WHERE table_id = {table_id}"
    df_config = spark.read.jdbc(url=url, table=f"({query_config}) as tmp", properties=props)
    config = df_config.collect()[0].asDict()
    
    # DQ Rules
    query_rules = f"SELECT * FROM control.data_quality_rules WHERE table_id = {table_id} AND is_active = '1'"
    df_rules = spark.read.jdbc(url=url, table=f"({query_rules}) as tmp", properties=props)
    rules = [row.asDict() for row in df_rules.collect()]
    
    return config, rules

def apply_dq_rules(df, rules, dlq_path, pipeline_run_id):
    """Apply DQ rules, filter data, and write exceptions to DLQ"""
    if not rules:
        return df
    
    valid_condition = lit(True)
    
    # Construct validation logic
    for rule in rules:
        rule_expr = expr(rule['rule_expression'])
        if rule['severity'] == 'ERROR':
            # Fail fast logic could go here, for now we treat as filter
            valid_condition = valid_condition & rule_expr
        elif rule['severity'] == 'SKIP_ROW':
            valid_condition = valid_condition & rule_expr
        # WARNINGS are logged but not filtered (implied)

    # Split Data
    df_valid = df.filter(valid_condition)
    df_invalid = df.filter(~valid_condition)
    
    invalid_count = df_invalid.count()
    
    if invalid_count > 0:
        logger.warning(f"Found {invalid_count} invalid records. Writing to DLQ: {dlq_path}")
        # Add metadata to DLQ records
        df_invalid = df_invalid.withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
                               .withColumn("_rejection_timestamp", current_timestamp())
        
        # Write to S3 JSON DLQ
        dlq_target = f"{dlq_path}/{pipeline_run_id}/"
        df_invalid.write.mode("append").json(dlq_target)

    return df_valid

def process_scd1(source_df, target_path, pk_cols):
    """Handle SCD Type 1 (Upsert)"""
    if not DeltaTable.isDeltaTable(spark, target_path):
        source_df.write.format("delta").mode("overwrite").save(target_path)
        return

    delta_target = DeltaTable.forPath(spark, target_path)
    merge_condition = " AND ".join([f"t.{col} = s.{col}" for col in pk_cols])
    
    delta_target.alias("t").merge(
        source_df.alias("s"),
        merge_condition
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

def process_scd2(source_df, target_path, pk_cols, track_cols):
    """Handle SCD Type 2 (History)"""
    # Add Hash for tracked columns
    hash_col = sha2(concat_ws("||", *[col(c) for c in track_cols]), 256)
    source_df = source_df.withColumn("_hash_key", hash_col)
    
    if not DeltaTable.isDeltaTable(spark, target_path):
        # Initial Load
        init_df = source_df \
            .withColumn("_is_current", lit(True)) \
            .withColumn("_valid_from", current_timestamp()) \
            .withColumn("_valid_to", lit(None).cast(TimestampType()))
        init_df.write.format("delta").mode("overwrite").save(target_path)
        return

    target_table = DeltaTable.forPath(spark, target_path)
    target_df = target_table.toDF()

    # Determine Updates (Rows where PK matches but Hash differs)
    # Join source and target to find changes
    join_cond = [source_df[c] == target_df[c] for c in pk_cols]
    
    # Records requiring update (Expire old row)
    updates_df = target_df.alias("t").join(source_df.alias("s"), join_cond) \
        .filter("t._is_current = true AND t._hash_key <> s._hash_key") \
        .selectExpr("NULL as mergeKey", "s.*") \
        .withColumn("_is_current", lit(True)) \
        .withColumn("_valid_from", current_timestamp()) \
        .withColumn("_valid_to", lit(None).cast(TimestampType()))

    # Union with original source for processing
    staged_updates = updates_df.selectExpr("mergeKey", "*") \
        .unionByName(source_df.withColumn("mergeKey", concat_ws("_", *pk_cols)) \
                     .withColumn("_is_current", lit(True)) \
                     .withColumn("_valid_from", current_timestamp()) \
                     .withColumn("_valid_to", lit(None).cast(TimestampType())) \
                     .withColumn("_hash_key", hash_col), allowMissingColumns=True)

    # Complex Merge Logic for SCD2
    # 1. If key matches and hash differs -> Update old record (set valid_to)
    # 2. If key matches and hash differs -> Insert new record (handled by UNION above)
    # 3. If New -> Insert
    
    # Note: A pure SQL MERGE for SCD2 is complex. 
    # Standard pattern: Write new rows to temp, Merge Update old rows, Append new rows.
    # Simplified Implementation:
    
    # 1. Identify Changed Keys
    changed_keys = target_df.join(source_df, pk_cols) \
        .filter(target_df._is_current == True) \
        .filter(target_df._hash_key != source_df._hash_key) \
        .select([target_df[c] for c in pk_cols]).distinct()
        
    # 2. Expire Old Rows in Delta
    if changed_keys.count() > 0:
        merge_cond = " AND ".join([f"t.{c} = s.{c}" for c in pk_cols])
        target_table.alias("t").merge(
            changed_keys.alias("s"),
            f"{merge_cond} AND t._is_current = true"
        ).whenMatchedUpdate(
            set = { "_is_current": "false", "_valid_to": "current_timestamp()" }
        ).execute()

    # 3. Append New Versions and New Rows
    # New Versions
    new_versions = source_df.join(changed_keys, pk_cols) \
        .withColumn("_is_current", lit(True)) \
        .withColumn("_valid_from", current_timestamp()) \
        .withColumn("_valid_to", lit(None).cast(TimestampType()))
        
    # Brand New Rows (Not in Target)
    new_rows = source_df.join(target_df.filter("_is_current=true"), pk_cols, "left_anti") \
        .withColumn("_is_current", lit(True)) \
        .withColumn("_valid_from", current_timestamp()) \
        .withColumn("_valid_to", lit(None).cast(TimestampType()))
        
    final_insert = new_versions.unionByName(new_rows)
    final_insert.write.format("delta").mode("append").save(target_path)


def main():
    url, props = get_db_connection(args['db_secret_name'])
    config, rules = fetch_metadata(args['table_id'], url, props)
    
    bronze_path = f"s3://{args['bronze_bucket']}/bronze/{config['bronze_table_name']}/"
    silver_path = config['silver_s3_path']
    pk_cols = config['primary_key_columns'].split(',')
    
    # Read Bronze
    # Optimally, we use CDC versions. For simplicity, reading delta directly.
    # In production, use .option("startingVersion", config['last_sync_version'])
    source_df = spark.read.format("delta").load(bronze_path)
    
    # Standard Transformations (Trim strings, cast dates)
    for field in source_df.schema.fields:
        if isinstance(field.dataType, StringType):
            source_df = source_df.withColumn(field.name, trim(col(field.name)))
            source_df = source_df.withColumn(field.name, when(col(field.name) == "", None).otherwise(col(field.name)))
            
    # Apply Data Quality
    dlq_path = silver_path.replace("/silver/", "/dlq/") 
    valid_df = apply_dq_rules(source_df, rules, dlq_path, args['pipeline_run_id'])
    
    records_written = valid_df.count()
    
    if records_written > 0:
        # Add Metadata Columns
        valid_df = valid_df.withColumn("_pipeline_run_id", lit(args['pipeline_run_id'])) \
                           .withColumn("_load_timestamp", current_timestamp())
        
        # Process Storage
        if config['scd_type'] == 2:
            track_cols = config['scd_tracked_columns'].split(',')
            process_scd2(valid_df, silver_path, pk_cols, track_cols)
        else:
            process_scd1(valid_df, silver_path, pk_cols)
            
        # Optimization
        if config['z_order_columns']:
            spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY ({config['z_order_columns']})")
    
    logger.info(f"Processed {records_written} records for {config['silver_table_name']}")

    # Finalize Job (The Lambda will handle DB stats update based on logs/step function)
    job.commit()

if __name__ == '__main__':
    main()