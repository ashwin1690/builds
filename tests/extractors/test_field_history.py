"""Tests for field history extractor."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from salesforce_temporal.extractors.field_history import (
    FieldHistoryExtractor,
    create_opportunity_field_history_extractor,
)
from salesforce_temporal.models.events import ActorType, EventType


class TestFieldHistoryExtractor:
    """Tests for FieldHistoryExtractor class."""

    def test_initialization(self):
        """Test extractor initialization."""
        extractor = FieldHistoryExtractor("OpportunityFieldHistory", "Opportunity")

        assert extractor.history_object_name == "OpportunityFieldHistory"
        assert extractor.parent_object_name == "Opportunity"
        assert extractor.parent_id_field == "OpportunityId"

    def test_initialization_with_custom_object(self):
        """Test initialization with custom object."""
        extractor = FieldHistoryExtractor("CustomObject__History", "CustomObject__c")

        assert extractor.history_object_name == "CustomObject__History"
        assert extractor.parent_object_name == "CustomObject__c"
        assert extractor.parent_id_field == "CustomObject__cId"

    def test_convert_to_event(self):
        """Test converting field history record to event."""
        extractor = FieldHistoryExtractor("OpportunityFieldHistory", "Opportunity")

        record = {
            "Id": "017xx000001X8Uz",
            "OpportunityId": "006xx000001X8Uz",
            "CreatedDate": "2024-01-15T10:30:00.000Z",
            "CreatedById": "005xx000001X8Uz",
            "Field": "Amount",
            "OldValue": "10000",
            "NewValue": "15000",
            "DataType": "Currency",
        }

        event = extractor._convert_to_event(record)

        assert event is not None
        assert event.event_type == EventType.FIELD_CHANGE
        assert event.record_type == "Opportunity"
        assert event.record_id == "006xx000001X8Uz"
        assert event.field_name == "Amount"
        assert event.old_value == "10000"
        assert event.new_value == "15000"
        assert event.actor_type == ActorType.USER
        assert event.context.source_object == "OpportunityFieldHistory"

    def test_convert_to_event_owner_change(self):
        """Test converting owner change to event."""
        extractor = FieldHistoryExtractor("AccountHistory", "Account")

        record = {
            "Id": "017xx000001X8Uz",
            "AccountId": "001xx000001X8Uz",
            "CreatedDate": "2024-01-15T10:30:00.000Z",
            "CreatedById": "005xx000001X8Uz",
            "Field": "OwnerId",
            "OldValue": "005xx000001X8Uy",
            "NewValue": "005xx000001X8Uz",
            "DataType": "EntityId",
        }

        event = extractor._convert_to_event(record)

        assert event is not None
        assert event.event_type == EventType.OWNER_CHANGE

    def test_determine_event_type_field_change(self):
        """Test determining event type for regular field change."""
        extractor = FieldHistoryExtractor("CaseHistory", "Case")

        event_type = extractor._determine_event_type("Status", "New", "In Progress")

        assert event_type == EventType.FIELD_CHANGE

    def test_determine_event_type_owner_change(self):
        """Test determining event type for owner change."""
        extractor = FieldHistoryExtractor("LeadHistory", "Lead")

        event_type = extractor._determine_event_type(
            "OwnerId", "005xx000001X8Uy", "005xx000001X8Uz"
        )

        assert event_type == EventType.OWNER_CHANGE

    def test_factory_functions(self):
        """Test convenience factory functions."""
        opp_extractor = create_opportunity_field_history_extractor()
        assert opp_extractor.history_object_name == "OpportunityFieldHistory"
        assert opp_extractor.parent_object_name == "Opportunity"

    @patch.object(FieldHistoryExtractor, "execute_query")
    def test_get_tracked_fields(self, mock_execute):
        """Test getting list of tracked fields."""
        extractor = FieldHistoryExtractor("OpportunityFieldHistory", "Opportunity")

        mock_execute.return_value = iter(
            [{"Field": "Amount"}, {"Field": "StageName"}, {"Field": "CloseDate"}]
        )

        fields = extractor.get_tracked_fields()

        assert len(fields) == 3
        assert "Amount" in fields
        assert "StageName" in fields
        assert "CloseDate" in fields
