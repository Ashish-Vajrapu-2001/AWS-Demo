# AWS Gold Layer - CLV Analytics

This repository contains the code to build the Dimensional Model (Gold Layer) for the Myntra CLV platform.

## Folder Structure
- `sql/`: DDL for Athena/Spark SQL.
- `glue/`: PySpark ETL scripts using Delta Lake.
- `stepfunctions/`: Orchestration logic.
- `docs/`: Design and deployment documentation.

## Key Features
- **Delta Lake**: ACID transactions, schema enforcement, time travel.
- **SCD Type 2**: Handled in `dim_customer` to track user status changes.
- **Performance**: Facts partitioned by Date; Z-Ordering applied on Join Keys.
- **Resilience**: Unknown member handling (-1) for referential integrity.