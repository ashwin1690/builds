# DQ Results Atlan Enrichment Script

This script processes Data Quality (DQ) results from a CSV file and enriches corresponding Snowflake column assets in Atlan with custom metadata.

## Features

- **Bulk Asset Retrieval**: Uses Atlan's metadata lakehouse for optimized read performance
- **Batch Processing**: Processes records in configurable batches for memory efficiency
- **Comprehensive Logging**: Detailed logs with timestamps and progress tracking
- **Error Handling**: Graceful error handling with detailed statistics
- **CSV Validation**: Validates CSV structure before processing

## Prerequisites

1. **Python 3.8+** installed
2. **Atlan API credentials**:
   - API Key
   - Base URL (e.g., `https://your-tenant.atlan.com`)
3. **Custom Metadata Setup in Atlan**:
   - Custom metadata set named "DQ" must exist with GUID: `faf3353d-86c2-4214-b4fc-f3fccf1991dd`
   - Attributes within "DQ" custom metadata:
     - `DQ_NULL_COUNT`: String or Number type
     - `DQ_STRINGLENGTH`: String or Number type

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

Or install pyatlan directly:
```bash
pip install pyatlan>=2.0.0
```

## Configuration

### Environment Variables

Set the following environment variables:

```bash
export ATLAN_API_KEY="your-api-key-here"
export ATLAN_BASE_URL="https://your-tenant.atlan.com"
```

### Script Constants

The script uses these predefined constants (modify in `dq_enrichment.py` if needed):

- `SNOWFLAKE_ACCOUNT`: `qia75894` (from qia75894.snowflakecomputing.com)
- `CONNECTION_NAME`: `snowflake_connection_60`
- `DQ_CUSTOM_METADATA_NAME`: `DQ`
- `DQ_CUSTOM_METADATA_GUID`: `faf3353d-86c2-4214-b4fc-f3fccf1991dd`

## CSV File Format

The input CSV file must contain the following columns:

| Column | Description | Example |
|--------|-------------|---------|
| DATABASE | Snowflake database name | MDLH_GOLD_RKO |
| SCHEMA | Snowflake schema name | PUBLIC |
| TABLE | Snowflake table name | CUSTOMERS |
| COLUMN | Snowflake column name | EMAIL |
| DQ_NULL_COUNT | Count of null values | 5 |
| DQ_STRINGLENGTH | String length metric | 50 |

### Sample CSV

See `sample_dq_results.csv` for an example:

```csv
DATABASE,SCHEMA,TABLE,COLUMN,DQ_NULL_COUNT,DQ_STRINGLENGTH
MDLH_GOLD_RKO,PUBLIC,CUSTOMERS,CUSTOMER_ID,0,10
MDLH_GOLD_RKO,PUBLIC,CUSTOMERS,EMAIL,5,50
MDLH_GOLD_RKO,PUBLIC,ORDERS,ORDER_ID,0,12
```

## Usage

### Basic Usage

```bash
python dq_enrichment.py --csv-file path/to/dq_results.csv
```

### With Custom Batch Size

```bash
python dq_enrichment.py --csv-file dq_results.csv --batch-size 100
```

### Passing Credentials as Arguments

```bash
python dq_enrichment.py \
  --csv-file dq_results.csv \
  --api-key "your-api-key" \
  --base-url "https://your-tenant.atlan.com"
```

### Command Line Options

```
--csv-file       Path to the CSV file containing DQ results (required)
--batch-size     Number of records to process in each batch (default: 50)
--api-key        Atlan API key (alternative to ATLAN_API_KEY env variable)
--base-url       Atlan base URL (alternative to ATLAN_BASE_URL env variable)
```

## How It Works

### 1. CSV Processing
- Reads and validates the CSV file structure
- Parses each row into a `DQRecord` object
- Normalizes database object names to uppercase

### 2. Qualified Name Generation
For each row, generates Atlan qualified names in the format:
```
default/snowflake/{ACCOUNT}/{DATABASE}/{SCHEMA}/{TABLE}/{COLUMN}
```

Example:
```
default/snowflake/qia75894/MDLH_GOLD_RKO/PUBLIC/CUSTOMERS/EMAIL
```

### 3. Bulk Asset Retrieval (Metadata Lakehouse Optimization)
- Uses Atlan's search API to fetch multiple assets in a single request
- Implements efficient query with `Terms` for OR matching on qualified names
- Filters for active Column assets only
- Processes up to 100 assets per search request

### 4. Custom Metadata Update
For each asset found:
- Constructs a `CustomMetadataDict` with DQ attributes
- Updates the asset using `client.asset.save()`
- Logs success or failure for each update

### 5. Statistics & Reporting
Tracks and reports:
- Total records processed
- Assets found vs. not found
- Successful updates
- Errors encountered
- Success rate percentage

## Output

### Console Output
```
2024-01-15 10:30:00 - INFO - Starting DQ Enrichment Process
2024-01-15 10:30:01 - INFO - Reading CSV file: dq_results.csv
2024-01-15 10:30:02 - INFO - Processing 150 records in 3 batches (batch_size=50)
2024-01-15 10:30:05 - INFO - ✓ Updated custom metadata for MDLH_GOLD_RKO.PUBLIC.CUSTOMERS.EMAIL
...
2024-01-15 10:30:30 - INFO - DQ Enrichment Complete
2024-01-15 10:30:30 - INFO - Assets updated successfully: 145
2024-01-15 10:30:30 - INFO - Success rate: 96.7%
```

### Log File
A detailed log file is created with timestamp:
```
dq_enrichment_20240115_103000.log
```

## Performance Optimization

### Metadata Lakehouse Benefits
The script leverages Atlan's metadata lakehouse architecture:

1. **Bulk Queries**: Single search API call retrieves multiple assets
2. **Indexed Search**: Uses indexed fields (`qualifiedName`, `__typeName`, `__state`)
3. **Filtered Results**: Reduces payload by filtering for active Column assets only
4. **Batch Processing**: Configurable batch size prevents memory issues

### Recommended Batch Sizes
- **Small datasets (<1000 records)**: 50-100
- **Medium datasets (1000-10000)**: 100-200
- **Large datasets (>10000)**: 200-500

## Error Handling

The script handles various error scenarios:

- **Missing CSV**: Clear error message if file not found
- **Invalid CSV structure**: Validates required columns
- **Asset not found**: Logs warning and continues processing
- **Update failures**: Logs error and continues with next asset
- **API errors**: Catches and logs exceptions with full traceback
- **Keyboard interrupt**: Graceful shutdown on Ctrl+C

## Troubleshooting

### Asset Not Found
If assets are not found in Atlan:
1. Verify the Snowflake connection name: `snowflake_connection_60`
2. Check the account identifier: `qia75894`
3. Ensure database/schema/table/column names match exactly (case-sensitive)
4. Verify assets exist and are in ACTIVE state in Atlan

### Custom Metadata Not Updated
1. Confirm the DQ custom metadata set exists in Atlan
2. Verify the GUID matches: `faf3353d-86c2-4214-b4fc-f3fccf1991dd`
3. Ensure attribute names are: `DQ_NULL_COUNT`, `DQ_STRINGLENGTH`
4. Check API key has permission to update custom metadata

### Authentication Errors
1. Verify `ATLAN_API_KEY` is set correctly
2. Confirm `ATLAN_BASE_URL` includes `https://` and has no trailing slash
3. Test API key with: `curl -H "Authorization: Bearer $ATLAN_API_KEY" $ATLAN_BASE_URL/api/meta/types/typedefs`

## Example Workflow

```bash
# 1. Set environment variables
export ATLAN_API_KEY="your-api-key"
export ATLAN_BASE_URL="https://your-tenant.atlan.com"

# 2. Prepare your CSV file
# DATABASE,SCHEMA,TABLE,COLUMN,DQ_NULL_COUNT,DQ_STRINGLENGTH
# ...

# 3. Run the script
python dq_enrichment.py --csv-file my_dq_results.csv --batch-size 100

# 4. Check the log file for details
cat dq_enrichment_*.log
```

## Integration with Snowflake

The script is configured for the Snowflake instance:
- **URL**: https://qia75894.snowflakecomputing.com/
- **Database**: MDLH_GOLD_RKO (and others as specified in CSV)
- **Connection**: Must be configured in Atlan as `snowflake_connection_60`

Ensure the Snowflake connection in Atlan:
1. Is named exactly `snowflake_connection_60`
2. Points to account `qia75894`
3. Has crawled the databases/schemas/tables/columns in your CSV
4. Assets are in ACTIVE state

## Future Enhancements

Potential improvements:
- Support for table-level DQ metrics aggregation
- Multiple custom metadata sets
- Configurable qualified name patterns
- Delta processing (only update changed records)
- Integration with DQ monitoring tools
- Support for other data sources (BigQuery, Redshift, etc.)

## Support

For issues or questions:
1. Check the log file for detailed error messages
2. Verify CSV format matches the expected structure
3. Confirm Atlan connection and custom metadata configuration
4. Review the qualified name format in logs

## License

Internal use only.
