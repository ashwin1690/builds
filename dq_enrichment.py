#!/usr/bin/env python3
"""
DQ Results Atlan Enrichment Script

This script processes DQ (Data Quality) results from a CSV file and enriches
corresponding assets in Atlan with custom metadata. It uses the metadata lakehouse
for optimized read performance.

Usage:
    python dq_enrichment.py --csv-file path/to/dq_results.csv [--batch-size 50]

Environment Variables:
    ATLAN_API_KEY: Your Atlan API key (required)
    ATLAN_BASE_URL: Your Atlan instance URL (required, e.g., https://your-tenant.atlan.com)
"""

import csv
import os
import sys
import logging
import argparse
from typing import List, Dict, Optional, Set
from dataclasses import dataclass
from datetime import datetime

try:
    from pyatlan.client.atlan import AtlanClient
    from pyatlan.model.assets import Column, Asset
    from pyatlan.model.enums import AtlanConnectorType
    from pyatlan.cache.role_cache import RoleCache
    from pyatlan.model.search import DSL, Term, Bool
    from pyatlan.model.response import AssetMutationResponse
    from pyatlan.model.custom_metadata import CustomMetadataDict
except ImportError:
    print("ERROR: pyatlan package not found. Please install it with: pip install pyatlan")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'dq_enrichment_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants
SNOWFLAKE_ACCOUNT = "qia75894"
CONNECTION_NAME = "snowflake_connection_60"
DQ_CUSTOM_METADATA_NAME = "DQ"
DQ_CUSTOM_METADATA_GUID = "faf3353d-86c2-4214-b4fc-f3fccf1991dd"


@dataclass
class DQRecord:
    """Represents a DQ result record from CSV"""
    database: str
    schema: str
    table: str
    column: str
    dq_null_count: Optional[str]
    dq_stringlength: Optional[str]

    @property
    def qualified_name(self) -> str:
        """Generate Atlan qualified name for the column asset"""
        return f"default/snowflake/{SNOWFLAKE_ACCOUNT}/{self.database}/{self.schema}/{self.table}/{self.column}"

    def __str__(self):
        return f"{self.database}.{self.schema}.{self.table}.{self.column}"


class DQEnrichmentProcessor:
    """Processes DQ results and enriches Atlan assets"""

    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        """Initialize the processor with Atlan client"""
        self.api_key = api_key or os.environ.get("ATLAN_API_KEY")
        self.base_url = base_url or os.environ.get("ATLAN_BASE_URL")

        if not self.api_key:
            raise ValueError("ATLAN_API_KEY environment variable or api_key parameter is required")
        if not self.base_url:
            raise ValueError("ATLAN_BASE_URL environment variable or base_url parameter is required")

        logger.info(f"Initializing Atlan client for {self.base_url}")
        self.client = AtlanClient(api_key=self.api_key, base_url=self.base_url)

        # Statistics
        self.stats = {
            'total_records': 0,
            'assets_found': 0,
            'assets_updated': 0,
            'assets_not_found': 0,
            'errors': 0
        }

    def read_csv(self, csv_file: str) -> List[DQRecord]:
        """Read DQ results from CSV file"""
        logger.info(f"Reading CSV file: {csv_file}")
        records = []

        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                # Validate required columns
                required_columns = {'DATABASE', 'SCHEMA', 'TABLE', 'COLUMN', 'DQ_NULL_COUNT', 'DQ_STRINGLENGTH'}
                if not required_columns.issubset(set(reader.fieldnames)):
                    raise ValueError(f"CSV must contain columns: {required_columns}")

                for row_num, row in enumerate(reader, start=2):  # Start at 2 (1 is header)
                    try:
                        record = DQRecord(
                            database=row['DATABASE'].strip().upper(),
                            schema=row['SCHEMA'].strip().upper(),
                            table=row['TABLE'].strip().upper(),
                            column=row['COLUMN'].strip().upper(),
                            dq_null_count=row.get('DQ_NULL_COUNT', '').strip() or None,
                            dq_stringlength=row.get('DQ_STRINGLENGTH', '').strip() or None
                        )
                        records.append(record)
                    except Exception as e:
                        logger.warning(f"Skipping row {row_num} due to error: {e}")
                        continue

            logger.info(f"Successfully read {len(records)} records from CSV")
            self.stats['total_records'] = len(records)
            return records

        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_file}")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise

    def fetch_assets_bulk(self, qualified_names: List[str]) -> Dict[str, Column]:
        """
        Fetch multiple assets efficiently using metadata lakehouse.

        This uses Atlan's search API with bulk qualified name lookups,
        which is optimized through the metadata lakehouse architecture.
        """
        logger.info(f"Fetching {len(qualified_names)} assets from Atlan (bulk operation)...")

        assets_map = {}

        try:
            # Build search query for bulk retrieval
            # Use Terms query for efficient OR matching on qualified names
            query = Bool(
                filter=[
                    Term(field="__typeName", value="Column"),
                    Term(field="__state", value="ACTIVE")
                ],
                should=[
                    Term(field="qualifiedName", value=qn) for qn in qualified_names
                ],
                minimum_should_match=1
            )

            dsl = DSL(query=query)

            # Execute search with appropriate page size
            search_request = (
                self.client.asset.search(dsl=dsl)
                .page_size(min(len(qualified_names), 100))
            )

            # Iterate through results
            for asset in search_request:
                if isinstance(asset, Column) and asset.qualified_name:
                    assets_map[asset.qualified_name] = asset
                    logger.debug(f"Found asset: {asset.qualified_name}")

            logger.info(f"Successfully fetched {len(assets_map)} assets from Atlan")

        except Exception as e:
            logger.error(f"Error during bulk asset fetch: {e}")
            raise

        return assets_map

    def update_custom_metadata(self, asset: Column, record: DQRecord) -> bool:
        """
        Update custom metadata on a single asset

        Args:
            asset: The Column asset to update
            record: The DQ record containing the values to set

        Returns:
            bool: True if update successful, False otherwise
        """
        try:
            # Build custom metadata dictionary
            custom_metadata = {}

            if record.dq_null_count is not None:
                custom_metadata['DQ_NULL_COUNT'] = record.dq_null_count

            if record.dq_stringlength is not None:
                custom_metadata['DQ_STRINGLENGTH'] = record.dq_stringlength

            if not custom_metadata:
                logger.warning(f"No DQ values to update for {record}")
                return False

            # Create updated asset with custom metadata
            updated_column = Column()
            updated_column.guid = asset.guid
            updated_column.qualified_name = asset.qualified_name

            # Set custom metadata using the CustomMetadataDict
            cm_dict = CustomMetadataDict()
            cm_dict[DQ_CUSTOM_METADATA_NAME] = custom_metadata
            updated_column.custom_metadata_sets = cm_dict

            # Update the asset
            response: AssetMutationResponse = self.client.asset.save(updated_column)

            if response and response.assets_updated(asset_type=Column):
                logger.info(f"✓ Updated custom metadata for {record}")
                return True
            else:
                logger.warning(f"Update response empty for {record}")
                return False

        except Exception as e:
            logger.error(f"Error updating custom metadata for {record}: {e}")
            return False

    def process_batch(self, records: List[DQRecord]) -> None:
        """Process a batch of DQ records"""
        logger.info(f"Processing batch of {len(records)} records...")

        # Extract qualified names for bulk fetch
        qualified_names = [record.qualified_name for record in records]

        # Fetch all assets in one bulk operation (metadata lakehouse optimization)
        assets_map = self.fetch_assets_bulk(qualified_names)

        # Update each asset with its DQ metadata
        for record in records:
            asset = assets_map.get(record.qualified_name)

            if asset:
                self.stats['assets_found'] += 1
                success = self.update_custom_metadata(asset, record)
                if success:
                    self.stats['assets_updated'] += 1
                else:
                    self.stats['errors'] += 1
            else:
                self.stats['assets_not_found'] += 1
                logger.warning(f"✗ Asset not found in Atlan: {record}")

    def process_csv(self, csv_file: str, batch_size: int = 50) -> None:
        """
        Process the entire CSV file in batches

        Args:
            csv_file: Path to the CSV file
            batch_size: Number of records to process in each batch (for memory efficiency)
        """
        start_time = datetime.now()
        logger.info("=" * 80)
        logger.info("Starting DQ Enrichment Process")
        logger.info("=" * 80)

        try:
            # Read all records from CSV
            records = self.read_csv(csv_file)

            if not records:
                logger.warning("No records to process")
                return

            # Process in batches
            total_batches = (len(records) + batch_size - 1) // batch_size
            logger.info(f"Processing {len(records)} records in {total_batches} batches (batch_size={batch_size})")

            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                logger.info(f"\n--- Batch {batch_num}/{total_batches} ---")
                self.process_batch(batch)

            # Print summary
            duration = datetime.now() - start_time
            logger.info("\n" + "=" * 80)
            logger.info("DQ Enrichment Complete")
            logger.info("=" * 80)
            logger.info(f"Duration: {duration}")
            logger.info(f"Total records processed: {self.stats['total_records']}")
            logger.info(f"Assets found: {self.stats['assets_found']}")
            logger.info(f"Assets updated successfully: {self.stats['assets_updated']}")
            logger.info(f"Assets not found: {self.stats['assets_not_found']}")
            logger.info(f"Errors: {self.stats['errors']}")

            success_rate = (self.stats['assets_updated'] / self.stats['total_records'] * 100) if self.stats['total_records'] > 0 else 0
            logger.info(f"Success rate: {success_rate:.1f}%")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Fatal error during processing: {e}", exc_info=True)
            raise


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Enrich Atlan assets with DQ results from CSV',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables:
  ATLAN_API_KEY      Your Atlan API key (required)
  ATLAN_BASE_URL     Your Atlan instance URL (required)

Example:
  export ATLAN_API_KEY="your-api-key"
  export ATLAN_BASE_URL="https://your-tenant.atlan.com"
  python dq_enrichment.py --csv-file dq_results.csv --batch-size 50
        """
    )

    parser.add_argument(
        '--csv-file',
        required=True,
        help='Path to the CSV file containing DQ results'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=50,
        help='Number of records to process in each batch (default: 50)'
    )

    parser.add_argument(
        '--api-key',
        help='Atlan API key (alternatively set ATLAN_API_KEY env variable)'
    )

    parser.add_argument(
        '--base-url',
        help='Atlan base URL (alternatively set ATLAN_BASE_URL env variable)'
    )

    args = parser.parse_args()

    try:
        processor = DQEnrichmentProcessor(
            api_key=args.api_key,
            base_url=args.base_url
        )
        processor.process_csv(args.csv_file, args.batch_size)

    except KeyboardInterrupt:
        logger.info("\nProcess interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Process failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
