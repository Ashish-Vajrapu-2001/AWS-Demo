# Silver Layer Deployment Guide

## Prerequisites
1. **Infrastructure**: AWS Account with S3, Glue, Lambda, Step Functions enabled.
2. **Database**: Postgres RDS instance running with `control` schema.
3. **IAM Roles**:
   - `GlueSilverRole`: Access to S3 (Bronze/Silver), SecretsManager, CloudWatch.
   - `LambdaExecutionRole`: VPCAccess (to reach RDS), SecretsManager.

## Deployment Steps

### 1. Database Setup
Execute the SQL scripts in `sql/silver_control_tables/` via a SQL client (dBeaver/pgAdmin) connected to the RDS instance.
1. `01_create_silver_tables.sql`
2. `02_populate_silver_config.sql`

### 2. S3 Bucket Configuration
Ensure `s3://myntra-clv-silver/` exists.
Create folders:
- `crm/`
- `erp/`
- `marketing/`
- `dlq/`

### 3. Glue Jobs
Upload scripts to `s3://aws-glue-scripts-{region}-{account}/myntra/`:
1. Create Job `silver_transform_dynamic`:
   - Type: Spark
   - Python Version: 3
   - Worker Type: G.1X
   - Script Path: `glue/jobs/silver_transform_dynamic.py`
   - **Important**: Enable "Delta Lake" in Job Details -> Job Parameters: `--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`

### 4. Lambda Functions
1. Create `update_silver_metadata` function.
2. Zip `lambda_function.py` and installed dependencies (`pip install -r requirements.txt -t .`).
3. Set Environment Variable `DB_SECRET_NAME`.

### 5. Orchestration
1. Create State Machine in AWS Step Functions using `stepfunctions/silver_orchestrator.json`.
2. Update the ARN resources in the JSON to match your deployed Lambda/Glue ARNs.

## Execution
Trigger the Step Function. It will:
1. Fetch active tables from DB.
2. Launch parallel Glue jobs.
3. Perform SCD processing and write Delta tables.
4. Update status in Postgres.