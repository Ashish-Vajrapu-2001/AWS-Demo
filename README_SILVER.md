# AWS Silver Layer - Myntra CLV

This repository contains the ETL code for the Silver Layer of the CLV platform.
It transforms raw Bronze data into conformed, cleansed Delta Lake tables.

## Components
- **Control DB**: Postgres metadata driving the pipeline.
- **Glue**: PySpark jobs using Delta Lake for ACID transactions and SCD handling.
- **Step Functions**: Orchestrates the parallel execution of table loads.

## Key Features
- **Metadata Driven**: New tables can be onboarded by adding rows to `control.table_metadata_silver`.
- **SCD Type 2**: Automatic history tracking for Customers and Addresses.
- **Data Quality**: Row-level validation with Dead Letter Queue (DLQ) support.
- **Delta Lake**: Supports time travel and schema evolution.

## How to Run
Trigger the `SilverOrchestrator` Step Function from the AWS Console or EventBridge.