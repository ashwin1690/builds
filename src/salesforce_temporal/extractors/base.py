"""
Base extractor class for Salesforce temporal data extraction.

This module provides the foundation for all specific extractors with common
functionality for Salesforce connectivity, pagination, error handling, and
event emission.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Optional

from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from salesforce_temporal.config.settings import Settings, get_settings
from salesforce_temporal.models.events import MicroDecisionEvent

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """
    Abstract base class for all Salesforce temporal data extractors.

    Provides common functionality for:
    - Salesforce API authentication
    - SOQL query execution with pagination
    - Bulk API 2.0 support for large data volumes
    - Error handling and retries
    - Event emission and standardization
    """

    def __init__(self, settings: Optional[Settings] = None):
        """
        Initialize the extractor.

        Args:
            settings: Configuration settings (uses global settings if not provided)
        """
        self.settings = settings or get_settings()
        self.sf: Optional[Salesforce] = None
        self.extracted_count = 0
        self.error_count = 0
        self.version = "0.1.0"

    @retry(
        retry=retry_if_exception_type((ConnectionError, SalesforceError)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(3),
    )
    def connect(self) -> Salesforce:
        """
        Establish connection to Salesforce with retry logic.

        Returns:
            Salesforce client instance

        Raises:
            Exception: If connection fails after retries
        """
        if self.sf is not None:
            return self.sf

        try:
            auth_config = self.settings.get_salesforce_auth_config()
            logger.info(f"Connecting to Salesforce ({self.settings.salesforce_domain})...")

            self.sf = Salesforce(
                username=auth_config["username"],
                password=auth_config.get("password"),
                security_token=auth_config.get("security_token", ""),
                domain=auth_config["domain"],
                version=self.settings.salesforce_api_version,
            )

            logger.info("✓ Connected to Salesforce")
            return self.sf

        except Exception as e:
            logger.error(f"Failed to connect to Salesforce: {e}")
            raise

    def execute_query(
        self,
        soql: str,
        use_bulk: bool = False,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Execute a SOQL query with automatic pagination.

        Args:
            soql: SOQL query string
            use_bulk: Whether to use Bulk API (for large result sets)

        Yields:
            Dictionary records from the query results
        """
        sf = self.connect()

        try:
            if use_bulk:
                # Use Bulk API 2.0 for large queries
                logger.info(f"Executing bulk query: {soql[:100]}...")
                # Note: simple-salesforce bulk support is limited
                # For production, consider using salesforce-bulk library
                results = sf.query_all(soql)
            else:
                # Use REST API with pagination
                logger.info(f"Executing query: {soql[:100]}...")
                results = sf.query_all(soql)

            # Yield records
            for record in results.get("records", []):
                # Remove attributes metadata
                record.pop("attributes", None)
                yield record

        except SalesforceError as e:
            logger.error(f"Query failed: {e}")
            self.error_count += 1
            raise

    def build_incremental_query(
        self,
        base_query: str,
        date_field: str = "CreatedDate",
        lookback_days: Optional[int] = None,
    ) -> str:
        """
        Build a SOQL query with incremental extraction logic.

        Args:
            base_query: Base SOQL query
            date_field: Field to filter on for incremental extraction
            lookback_days: Days to look back (uses setting if not provided)

        Returns:
            Modified SOQL query with date filter
        """
        days = lookback_days or self.settings.incremental_lookback_days
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        cutoff_str = cutoff_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Add WHERE clause or append to existing WHERE
        if "WHERE" in base_query.upper():
            query = f"{base_query} AND {date_field} >= {cutoff_str}"
        else:
            # Insert WHERE before ORDER BY if present
            if "ORDER BY" in base_query.upper():
                parts = base_query.split("ORDER BY")
                query = f"{parts[0]} WHERE {date_field} >= {cutoff_str} ORDER BY {parts[1]}"
            else:
                query = f"{base_query} WHERE {date_field} >= {cutoff_str}"

        return query

    @abstractmethod
    def extract_events(
        self,
        incremental: bool = True,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Generator[MicroDecisionEvent, None, None]:
        """
        Extract events from Salesforce and emit as MicroDecisionEvent records.

        This method must be implemented by each specific extractor.

        Args:
            incremental: Whether to do incremental extraction (vs full refresh)
            start_date: Start date for extraction (optional)
            end_date: End date for extraction (optional)

        Yields:
            MicroDecisionEvent instances
        """
        pass

    def get_stats(self) -> Dict[str, Any]:
        """
        Get extraction statistics.

        Returns:
            Dictionary with extraction metrics
        """
        return {
            "extracted_count": self.extracted_count,
            "error_count": self.error_count,
            "extractor_class": self.__class__.__name__,
            "version": self.version,
        }

    def log_stats(self):
        """Log extraction statistics."""
        stats = self.get_stats()
        logger.info(f"Extraction complete: {stats}")

    def reset_stats(self):
        """Reset extraction counters."""
        self.extracted_count = 0
        self.error_count = 0
