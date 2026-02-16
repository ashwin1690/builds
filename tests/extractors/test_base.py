"""Tests for base extractor."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from salesforce_temporal.extractors.base import BaseExtractor
from salesforce_temporal.models.events import MicroDecisionEvent


class ConcreteExtractor(BaseExtractor):
    """Concrete implementation for testing."""

    def extract_events(self, incremental=True, start_date=None, end_date=None):
        """Dummy implementation."""
        return iter([])


class TestBaseExtractor:
    """Tests for BaseExtractor class."""

    def test_initialization(self):
        """Test extractor initialization."""
        extractor = ConcreteExtractor()

        assert extractor.sf is None
        assert extractor.extracted_count == 0
        assert extractor.error_count == 0
        assert extractor.version == "0.1.0"

    def test_build_incremental_query_with_where(self):
        """Test building incremental query when WHERE clause exists."""
        extractor = ConcreteExtractor()

        base_query = "SELECT Id FROM Task WHERE OwnerId = '005xx'"
        result = extractor.build_incremental_query(base_query, "CreatedDate")

        assert "WHERE" in result
        assert "CreatedDate >=" in result
        assert "OwnerId = '005xx'" in result

    def test_build_incremental_query_without_where(self):
        """Test building incremental query without WHERE clause."""
        extractor = ConcreteExtractor()

        base_query = "SELECT Id FROM Task ORDER BY CreatedDate"
        result = extractor.build_incremental_query(base_query, "CreatedDate")

        assert "WHERE CreatedDate >=" in result
        assert "ORDER BY CreatedDate" in result

    def test_build_incremental_query_simple(self):
        """Test building incremental query for simple query."""
        extractor = ConcreteExtractor()

        base_query = "SELECT Id FROM Task"
        result = extractor.build_incremental_query(base_query, "CreatedDate")

        assert "WHERE CreatedDate >=" in result

    def test_get_stats(self):
        """Test getting extraction statistics."""
        extractor = ConcreteExtractor()
        extractor.extracted_count = 100
        extractor.error_count = 5

        stats = extractor.get_stats()

        assert stats["extracted_count"] == 100
        assert stats["error_count"] == 5
        assert stats["extractor_class"] == "ConcreteExtractor"
        assert stats["version"] == "0.1.0"

    def test_reset_stats(self):
        """Test resetting statistics."""
        extractor = ConcreteExtractor()
        extractor.extracted_count = 100
        extractor.error_count = 5

        extractor.reset_stats()

        assert extractor.extracted_count == 0
        assert extractor.error_count == 0

    @patch("salesforce_temporal.extractors.base.Salesforce")
    def test_connect_success(self, mock_sf_class):
        """Test successful Salesforce connection."""
        extractor = ConcreteExtractor()
        mock_sf_instance = MagicMock()
        mock_sf_class.return_value = mock_sf_instance

        result = extractor.connect()

        assert result == mock_sf_instance
        assert extractor.sf == mock_sf_instance

    def test_execute_query_not_connected(self):
        """Test that execute_query connects if not already connected."""
        extractor = ConcreteExtractor()

        # Mock the connect method
        with patch.object(extractor, "connect") as mock_connect:
            mock_sf = MagicMock()
            mock_sf.query_all.return_value = {"records": [{"Id": "123"}]}
            mock_connect.return_value = mock_sf

            results = list(extractor.execute_query("SELECT Id FROM Task"))

            mock_connect.assert_called_once()
            assert len(results) == 1
            assert results[0]["Id"] == "123"
