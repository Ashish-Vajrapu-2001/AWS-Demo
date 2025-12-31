# Gold Layer Deployment Guide

## Prerequisites
1. **Infrastructure**: AWS S3 Bucket `myntra-clv-gold` created.
2. **Access**: Glue Role `GoldETLRole` with S3 R/W and Glue permissions.
3. **Data**: Silver layer (Delta tables) populated in `silver` database.

## Step-by-Step Deployment

### 1. Database & Schema Setup
Execute the SQL scripts in Athena to register the Delta tables:
1. `sql/gold_tables/01_create_gold_schema.sql`
2. `sql/gold_tables/02_create_dimensions.sql`
3. `sql/gold_tables/03_create_facts.sql`

### 2. Deploy Glue Jobs
Upload the following scripts to S3 (e.g., `s3://aws-glue-scripts/`):
- `glue/jobs/gold_build_dimensions.py`
- `glue/jobs/gold_build_facts.py`
- `glue/jobs/gold_build_aggregates.py`

Create Glue Jobs (Type: Spark) referencing these scripts. Ensure "Delta Lake" JARs are enabled:
`--datalake-formats delta`

### 3. Deploy Orchestration
Create an AWS Step Functions State Machine using the definition in `stepfunctions/gold_orchestrator.json`.

### 4. Execute
Trigger the Step Function with input: