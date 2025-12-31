import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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
gold_base = f"s3://{args['gold_bucket']}"

def process_fact_orders():
    print("Processing Fact Orders...")
    
    # 1. Read Sources
    silver_orders = spark.table(f"{silver_db}.orders") \
        .filter(col("order_status") != "Draft")
    
    # 2. Read Dimensions (Current versions only)
    dim_cust = spark.read.format("delta").load(f"{gold_base}/dimensions/dim_customer/") \
        .filter(col("_is_current") == True) \
        .select("customer_id", "dim_customer_key")
        
    dim_loc = spark.read.format("delta").load(f"{gold_base}/dimensions/dim_location/") \
        .select("address_id", "dim_location_key")
        
    # 3. Join and Calculate Keys
    # Handle Late Arriving Dimensions / Unknowns by coalescing to -1
    fact_df = silver_orders.alias("o") \
        .join(dim_cust, silver_orders.customer_id == dim_cust.customer_id, "left") \
        .join(dim_loc, silver_orders.shipping_address_id == dim_loc.address_id, "left") \
        .select(
            # Date Key
            date_format(col("o.order_date"), "yyyyMMdd").cast("int").alias("dim_date_key"),
            
            # Lookup Keys with Null Handling (Referential Integrity)
            coalesce(col("dim_customer_key"), lit(-1)).alias("dim_customer_key"),
            coalesce(col("dim_location_key"), lit(-1)).alias("dim_location_key"),
            lit(-1).alias("dim_campaign_key"), # Logic to derive from campaign source
            
            # Degenerate Dims
            col("o.order_number"),
            col("o.order_status"),
            col("o.payment_method"),
            
            # Measures
            col("o.total_amount").cast("decimal(18,2)"),
            col("o.subtotal_amount").cast("decimal(18,2)"),
            col("o.discount_amount").cast("decimal(18,2)"),
            col("o.tax_amount").cast("decimal(18,2)"),
            col("o.shipping_amount").cast("decimal(18,2)"),
            when(col("o.order_status") == "Delivered", 1).otherwise(0).alias("is_delivered"),
            
            # Audit
            current_timestamp().alias("_created_date"),
            lit(pipeline_run_id).alias("_pipeline_run_id")
        )

    # 4. Write to Gold (Incremental Merge based on Order Number)
    target_path = f"{gold_base}/facts/fact_orders"
    
    if DeltaTable.isDeltaTable(spark, target_path):
        deltaTable = DeltaTable.forPath(spark, target_path)
        deltaTable.alias("tgt").merge(
            fact_df.alias("src"),
            "tgt.order_number = src.order_number"
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()
    else:
        fact_df.write.format("delta") \
            .partitionBy("dim_date_key") \
            .mode("overwrite").save(target_path)

def process_fact_order_lines():
    print("Processing Fact Order Lines...")
    
    # Read Sources
    silver_lines = spark.table(f"{silver_db}.order_lines")
    silver_orders = spark.table(f"{silver_db}.orders").select("order_number", "order_date", "customer_id")
    
    # Dims
    dim_prod = spark.read.format("delta").load(f"{gold_base}/dimensions/dim_product/") \
        .select("product_id", "dim_product_key")
    dim_cust = spark.read.format("delta").load(f"{gold_base}/dimensions/dim_customer/") \
        .filter(col("_is_current") == True).select("customer_id", "dim_customer_key")
        
    # Enrich Lines with Order Context (Date, Customer)
    lines_enriched = silver_lines.join(silver_orders, "order_number")
    
    fact_lines = lines_enriched.alias("l") \
        .join(dim_prod, col("l.product_id") == dim_prod.product_id, "left") \
        .join(dim_cust, col("l.customer_id") == dim_cust.customer_id, "left") \
        .select(
            date_format(col("l.order_date"), "yyyyMMdd").cast("int").alias("dim_date_key"),
            coalesce(col("dim_product_key"), lit(-1)).alias("dim_product_key"),
            coalesce(col("dim_customer_key"), lit(-1)).alias("dim_customer_key"),
            lit(None).cast("long").alias("dim_order_key"), # Optional
            
            col("l.order_number"),
            col("l.line_number"), # Assuming generated in Silver
            lit("Closed").alias("line_status"),
            
            col("l.quantity").cast("int"),
            col("l.line_amount").cast("decimal(18,2)"),
            col("l.unit_price").cast("decimal(18,2)"),
            
            current_timestamp().alias("_created_date"),
            lit(pipeline_run_id).alias("_pipeline_run_id")
        )
        
    target_path = f"{gold_base}/facts/fact_order_lines"
    
    # Full Overwrite for simplicity in this example, or Merge on (order_number, product_id)
    fact_lines.write.format("delta") \
        .partitionBy("dim_date_key") \
        .mode("overwrite") \
        .option("replaceWhere", f"dim_date_key >= {20200101}") \
        .save(target_path)

process_fact_orders()
process_fact_order_lines()

job.commit()