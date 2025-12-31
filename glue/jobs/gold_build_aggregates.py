import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'pipeline_run_id', 'gold_bucket'])
gold_base = f"s3://{args['gold_bucket']}"

def process_agg_customer_snapshot():
    print("Building Customer Monthly Snapshot...")
    
    # Read Fact Orders
    fact_orders = spark.read.format("delta").load(f"{gold_base}/facts/fact_orders")
    
    # Read Dim Date to get Year-Month
    dim_date = spark.read.format("delta").load(f"{gold_base}/dimensions/dim_date") \
        .select("dim_date_key", "full_date")
        
    # Join and Aggregate
    base_agg = fact_orders.join(dim_date, "dim_date_key") \
        .withColumn("month_key", date_format("full_date", "yyyyMM").cast("int")) \
        .groupBy("dim_customer_key", "month_key") \
        .agg(
            sum("total_amount").alias("monthly_revenue"),
            countDistinct("order_number").alias("monthly_orders"),
            sum("discount_amount").alias("monthly_discount"),
            max("full_date").alias("last_order_date_in_month")
        )
        
    # Window functions for Lifetime calculations
    w_lifetime = Window.partitionBy("dim_customer_key").orderBy("month_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    final_agg = base_agg \
        .withColumn("lifetime_revenue", sum("monthly_revenue").over(w_lifetime)) \
        .withColumn("lifetime_orders", sum("monthly_orders").over(w_lifetime)) \
        .withColumn("recency_days", datediff(current_date(), col("last_order_date_in_month"))) \
        .withColumn("aov_lifetime", col("lifetime_revenue") / col("lifetime_orders")) \
        .drop("last_order_date_in_month") \
        .withColumn("_created_date", current_timestamp()) \
        .withColumn("_pipeline_run_id", lit(args['pipeline_run_id']))
        
    target_path = f"{gold_base}/aggregates/agg_customer_monthly_snapshot"
    
    final_agg.write.format("delta") \
        .partitionBy("month_key") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(target_path)

process_agg_customer_snapshot()

job.commit()