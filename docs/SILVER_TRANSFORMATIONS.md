# Silver Layer Transformation Logic

## Standard Transformations
Applied automatically to all tables:
1. **String Trimming**: All string columns have leading/trailing whitespace removed.
2. **Empty Strings**: `""` converted to `NULL`.
3. **Timestamps**: Cast to TimestampType.

## Data Quality Rules
Defined in `control.data_quality_rules`.

### Severity Levels
- **ERROR**: Validated in logic, usually stops processing (configurable).
- **SKIP_ROW**: Row is removed from the Valid batch and written to DLQ S3.
- **WARNING**: Logged, but data continues.

### Specific Entity Rules

#### Customers
- `EMAIL IS NOT NULL` (Error)
- `EMAIL` Regex Check (Skip Row)

#### Orders
- `TOTAL_AMOUNT > 0` (Skip Row)
- `CUSTOMER_ID` Referential Integrity check (Skip Row)

## SCD Strategy
- **Type 1 (Upsert)**: Orders, OrderLines, Products. Latest record wins based on PK.
- **Type 2 (History)**: Customers, Addresses.
  - Adds `_valid_from`, `_valid_to`, `_is_current`.
  - Triggered by hash change in: `status`, `email`, `city`, etc.