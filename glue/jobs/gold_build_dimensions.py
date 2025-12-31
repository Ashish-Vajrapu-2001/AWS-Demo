import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import *

# Initialize Spark Session with Delta support
spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'pipeline_run_id', 'silver_db', 'gold_bucket'])
pipeline_run_id = args['pipeline_run_id']
silver_db = args['silver_db']
gold_path_base = f"s3://{args['gold_bucket']}/dimensions"

# -------------------------------------------------------------------------
# Helper: Generate Surrogate Keys
# -------------------------------------------------------------------------
def generate_sk(df, order_col, sk_col_name, offset=0):
    w = Window.orderBy(order_col)
    return df.withColumn(sk_col_name, row_number().over(w) + lit(offset).cast("long"))

# -------------------------------------------------------------------------
# 1. PROCESS DIM_DATE (Static Generation)
# -------------------------------------------------------------------------
def process_dim_date():
    print("Processing Dim Date...")
    start_date = '2020-01-01'
    end_date = '2030-12-31'
    
    date_df = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as full_date
    """)
    
    dim_date = date_df.select(
        (date_format(col("full_date"), "yyyyMMdd").cast("int")).alias("dim_date_key"),
        col("full_date"),
        year("full_date").alias("year"),
        quarter("full_date").alias("quarter"),
        month("full_date").alias("month_number"),
        date_format("full_date", "MMMM").alias("month_name"),
        weekofyear("full_date").alias("week_of_year"),
        dayofweek("full_date").alias("day_of_week"),
        date_format("full_date", "EEEE").alias("day_name"),
        when(dayofweek("full_date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
        lit(False).alias("is_holiday") # Placeholder for holiday logic
    )
    
    # Overwrite full dim_date (Small table)
    dim_date.write.format("delta").mode("overwrite").save(f"{gold_path_base}/dim_date")

# -------------------------------------------------------------------------
# 2. PROCESS DIM_PRODUCT (SCD Type 1 - Overwrite/Upsert)
# -------------------------------------------------------------------------
def process_dim_product():
    print("Processing Dim Product...")
    # Read Silver Sources
    products = spark.table(f"{silver_db}.products")
    brands = spark.table(f"{silver_db}.brands")
    categories = spark.table(f"{silver_db}.categories")

    # Join to create flattened view
    df_product = products.alias("p") \
        .join(brands.alias("b"), "brand_id", "left") \
        .join(categories.alias("c"), "category_id", "left") \
        .select(
            col("p.product_id"),
            col("p.sku"),
            col("p.item_name").alias("product_name"),
            col("p.list_price"),
            col("b.brand_name"),
            col("b.brand_tier"),
            col("c.category_name"),
            lit("Level 1").alias("category_level_1"), # Placeholder logic
            lit("Level 2").alias("category_level_2"),
            current_timestamp().alias("_created_date"),
            lit(pipeline_run_id).alias("_pipeline_run_id")
        )

    target_path = f"{gold_path_base}/dim_product"
    
    # Check if target exists for Merge
    if DeltaTable.isDeltaTable(spark, target_path):
        deltaTable = DeltaTable.forPath(spark, target_path)
        
        # Simple Merge for SCD 1
        deltaTable.alias("tgt").merge(
            df_product.alias("src"),
            "tgt.product_id = src.product_id"
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsert(
            values = {
                "dim_product_key": "src.product_id", # Simplified SK generation: using ID if unique or hash
                "product_id": "src.product_id",
                "sku": "src.sku",
                "product_name": "src.product_name",
                "list_price": "src.list_price",
                "brand_name": "src.brand_name",
                "brand_tier": "src.brand_tier",
                "category_name": "src.category_name",
                "category_level_1": "src.category_level_1",
                "category_level_2": "src.category_level_2",
                "_created_date": "src._created_date",
                "_pipeline_run_id": "src._pipeline_run_id"
            }
        ).execute()
    else:
        # Initial Load
        df_final = df_product.withColumn("dim_product_key", col("product_id")) # Or use row_number logic
        df_final.write.format("delta").mode("overwrite").save(target_path)

# -------------------------------------------------------------------------
# 3. PROCESS DIM_CUSTOMER (SCD Type 2)
# -------------------------------------------------------------------------
def process_dim_customer():
    print("Processing Dim Customer (SCD2)...")
    
    df_silver = spark.table(f"{silver_db}.customers").select(
        "customer_id", "email", "first_name", "last_name", "gender", 
        "date_of_birth", "registration_date", "customer_type", "status"
    )
    
    target_path = f"{gold_path_base}/dim_customer"
    
    if not DeltaTable.isDeltaTable(spark, target_path):
        # Initial Load
        df_final = df_silver \
            .withColumn("dim_customer_key", row_number().over(Window.orderBy("customer_id"))) \
            .withColumn("_valid_from", lit(current_timestamp())) \
            .withColumn("_valid_to", lit(None).cast("timestamp")) \
            .withColumn("_is_current", lit(True)) \
            .withColumn("_version", lit(1)) \
            .withColumn("_created_date", current_timestamp()) \
            .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
            .withColumn("rfm_segment", lit("New")) 
            
        df_final.write.format("delta").mode("overwrite").save(target_path)
        
        # Add Unknown Member (-1)
        unknown_schema = spark.read.format("delta").load(target_path).limit(0).schema
        unknown_row = spark.createDataFrame([(-1, -1, 'UNKNOWN', 'Unknown', 'Customer', 'U', None, None, 'Standard', 'Active', 'Unknown', None, None, True, 1, None, None)], schema=unknown_schema)
        unknown_row.write.format("delta").mode("append").save(target_path)
        return

    # SCD Type 2 Logic
    deltaTable = DeltaTable.forPath(spark, target_path)
    target_df = deltaTable.toDF().filter(col("_is_current") == True)
    
    # Identify changes
    join_cond = [df_silver.customer_id == target_df.customer_id]
    
    # 1. New Records
    new_records = df_silver.join(target_df, df_silver.customer_id == target_df.customer_id, "left_anti") \
        .withColumn("action", lit("INSERT"))
        
    # 2. Changed Records (Compare Hash of SCD2 attributes)
    # Attributes to track history for: customer_type, status
    updates = df_silver.alias("src").join(target_df.alias("tgt"), "customer_id") \
        .filter(
            (col("src.customer_type") != col("tgt.customer_type")) | 
            (col("src.status") != col("tgt.status"))
        ).select("src.*").withColumn("action", lit("UPDATE"))

    if new_records.count() == 0 and updates.count() == 0:
        print("No changes for Dim Customer.")
        return

    # Prepare Staging for Merge
    # For updates: We need to Close the old record AND Insert the new record
    
    # Determine Max Surrogate Key to increment from
    max_sk = spark.read.format("delta").load(target_path).agg(max("dim_customer_key")).collect()[0][0]
    if max_sk is None: max_sk = 0

    # Process New Inserts
    df_inserts = new_records.union(updates) \
        .withColumn("dim_customer_key", row_number().over(Window.orderBy("customer_id")) + lit(max_sk)) \
        .withColumn("_valid_from", current_timestamp()) \
        .withColumn("_valid_to", lit(None).cast("timestamp")) \
        .withColumn("_is_current", lit(True)) \
        .withColumn("_version", lit(1)) # Ideally increment version from previous
        
    # Logic to handle the MERGE for Type 2 is complex in pure PySpark API. 
    # Standard Pattern: 
    # 1. Update old records in Gold setting _is_current = False, _valid_to = Now
    # 2. Append new records from df_inserts
    
    # Step 1: Close old records
    ids_to_update = [row.customer_id for row in updates.collect()]
    if ids_to_update:
        deltaTable.update(
            condition = col("customer_id").isin(ids_to_update) & (col("_is_current") == True),
            set = { 
                "_is_current": lit(False),
                "_valid_to": current_timestamp()
            }
        )
    
    # Step 2: Append new/updated records
    cols = deltaTable.toDF().columns
    # Fill missing columns for insert (rfm_segment, audit)
    df_inserts = df_inserts \
        .withColumn("rfm_segment", lit("Recalculate")) \
        .withColumn("_created_date", current_timestamp()) \
        .withColumn("_pipeline_run_id", lit(pipeline_run_id))
    
    df_inserts.select(*cols).write.format("delta").mode("append").save(target_path)

# Execute
process_dim_date()
process_dim_product()
process_dim_customer()
# Note: process_dim_location would follow similar SCD1 pattern as product

job.commit()