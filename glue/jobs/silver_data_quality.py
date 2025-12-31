import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# This job runs POST-LOAD to check for Referential Integrity or Complex Business Logic
# that requires querying the full target dataset.

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'table_id', 'db_secret_name'])

spark = SparkSession.builder \
    .appName("Silver_DQ_Auditor") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def main():
    # 1. Fetch RI Rules from DB (Logic similar to main job)
    # 2. Iterate Rules
    
    # Example Logic for Referential Integrity: Orders -> Customers
    # This checks if there are orders with customer_ids that don't exist in the Customer silver table
    
    silver_orders = spark.read.format("delta").load("s3://myntra-clv-silver/erp/orders/")
    silver_customers = spark.read.format("delta").load("s3://myntra-clv-silver/crm/customers/")
    
    # Find Orphans
    orphans = silver_orders.join(silver_customers, silver_orders.customer_id == silver_customers.customer_id, "left_anti")
    
    orphan_count = orphans.count()
    if orphan_count > 0:
        print(f"FAILED INTEGRITY CHECK: Found {orphan_count} orders with invalid customer_id")
        # Write to DQ Log Table via JDBC or S3 DLQ
        orphans.select("order_id", "customer_id", lit("DQ-R-001").alias("rule_id")) \
            .write.mode("append").json("s3://myntra-clv-silver/dlq/audit_failures/")
    
    print("DQ Audit Complete")

if __name__ == '__main__':
    main()