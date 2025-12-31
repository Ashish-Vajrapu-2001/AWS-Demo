# Troubleshooting Guide

## Common Issues

### 1. "Initial Load Runs Every Time"
*   **Symptom**: Step Function always takes the "InitialLoadJob" path.
*   **Cause**: `initial_load_completed` flag in `control.table_metadata` is False.
*   **Fix**: Check `update_table_metadata` Lambda logs. Ensure it is receiving `mark_initial_complete=True` and status `SUCCESS`. Verify `initial_load_dynamic.py` calls the Lambda correctly.

### 2. "Glue Job Connection Timeout"
*   **Symptom**: Glue job fails after 10-15 minutes with connection error.
*   **Cause**: Security Group or Subnet misconfiguration.
*   **Fix**: Ensure Glue Connection uses a private subnet with a NAT Gateway (for internet) or valid VPN routing to the Source DB. Check SG allows outbound traffic on port 1433/5432.

### 3. "Delta Merge Fails"
*   **Symptom**: `AnalysisException: ... matches multiple rows in target`.
*   **Cause**: Source data contains duplicates in the batch that match the same target row.
*   **Fix**: The `incremental_cdc_dynamic.py` script includes a deduplication step (`Window.partitionBy(pk)`). Ensure the PK definition in `control.table_metadata` matches the actual source PK.

### 4. "Lambda Missing Module psycopg2"
*   **Symptom**: `ImportError: No module named psycopg2`.
*   **Fix**: `psycopg2-binary` must be installed in the deployment package (zipped folder). Alternatively, use an AWS Lambda Layer containing `psycopg2`.

## Resetting a Table
To force a full reload for a specific table: