"""Tests for event models."""

from datetime import datetime

import pytest

from salesforce_temporal.models.events import (
    ActorType,
    EventContext,
    EventType,
    MicroDecisionEvent,
)


class TestEventContext:
    """Tests for EventContext model."""

    def test_create_basic_context(self):
        """Test creating a basic event context."""
        context = EventContext(
            source_object="OpportunityHistory",
            source_record_id="001xx000001X8Uz",
        )

        assert context.source_object == "OpportunityHistory"
        assert context.source_record_id == "001xx000001X8Uz"
        assert context.related_records == {}
        assert context.metadata == {}

    def test_create_context_with_stage_info(self):
        """Test creating context with stage information."""
        context = EventContext(
            source_object="OpportunityHistory",
            source_record_id="001xx000001X8Uz",
            previous_stage="Prospecting",
            stage_duration_days=5.2,
        )

        assert context.previous_stage == "Prospecting"
        assert context.stage_duration_days == 5.2

    def test_context_allows_extra_fields(self):
        """Test that context allows extra fields."""
        context = EventContext(
            source_object="Task",
            custom_field="custom_value",
        )

        assert context.source_object == "Task"


class TestMicroDecisionEvent:
    """Tests for MicroDecisionEvent model."""

    def test_create_basic_event(self):
        """Test creating a basic micro-decision event."""
        now = datetime.utcnow()

        event = MicroDecisionEvent(
            event_type=EventType.FIELD_CHANGE,
            timestamp_occurred=now,
            timestamp_recorded=now,
            actor_id="005xx000001X8Uz",
            actor_type=ActorType.USER,
            record_type="Opportunity",
            record_id="006xx000001X8Uz",
            field_name="Amount",
            old_value=10000,
            new_value=15000,
            context=EventContext(source_object="OpportunityFieldHistory"),
        )

        assert event.event_type == EventType.FIELD_CHANGE
        assert event.actor_type == ActorType.USER
        assert event.record_type == "Opportunity"
        assert event.field_name == "Amount"
        assert event.old_value == 10000
        assert event.new_value == 15000

    def test_create_stage_change_event(self):
        """Test creating a stage change event."""
        now = datetime.utcnow()

        context = EventContext(
            source_object="OpportunityHistory",
            previous_stage="Prospecting",
            stage_duration_days=5.0,
        )

        event = MicroDecisionEvent(
            event_type=EventType.STAGE_CHANGE,
            timestamp_occurred=now,
            timestamp_recorded=now,
            actor_id="005xx000001X8Uz",
            actor_type=ActorType.USER,
            record_type="Opportunity",
            record_id="006xx000001X8Uz",
            field_name="StageName",
            old_value="Prospecting",
            new_value="Qualification",
            context=context,
        )

        assert event.event_type == EventType.STAGE_CHANGE
        assert event.context.previous_stage == "Prospecting"
        assert event.context.stage_duration_days == 5.0

    def test_parse_datetime_from_string(self):
        """Test parsing datetime from ISO string."""
        event = MicroDecisionEvent(
            event_type=EventType.FIELD_CHANGE,
            timestamp_occurred="2024-01-15T10:30:00Z",
            timestamp_recorded="2024-01-15T10:30:00Z",
            actor_id="005xx000001X8Uz",
            actor_type=ActorType.USER,
            record_type="Account",
            record_id="001xx000001X8Uz",
            context=EventContext(source_object="AccountHistory"),
        )

        assert isinstance(event.timestamp_occurred, datetime)
        assert event.timestamp_occurred.year == 2024
        assert event.timestamp_occurred.month == 1
        assert event.timestamp_occurred.day == 15

    def test_to_dict(self):
        """Test converting event to dictionary."""
        now = datetime.utcnow()

        event = MicroDecisionEvent(
            event_type=EventType.ACTIVITY,
            timestamp_occurred=now,
            timestamp_recorded=now,
            actor_id="005xx000001X8Uz",
            actor_type=ActorType.USER,
            record_type="Account",
            record_id="001xx000001X8Uz",
            context=EventContext(
                source_object="Task",
                activity_type="Task",
                activity_subject="Follow up call",
            ),
        )

        event_dict = event.to_dict()

        assert isinstance(event_dict, dict)
        assert event_dict["event_type"] == "activity"
        assert event_dict["actor_type"] == "user"
        assert event_dict["context"]["activity_subject"] == "Follow up call"

    def test_to_json(self):
        """Test converting event to JSON string."""
        now = datetime.utcnow()

        event = MicroDecisionEvent(
            event_type=EventType.APPROVAL_DECISION,
            timestamp_occurred=now,
            timestamp_recorded=now,
            actor_id="005xx000001X8Uz",
            actor_type=ActorType.USER,
            record_type="Opportunity",
            record_id="006xx000001X8Uz",
            field_name="ApprovalStatus",
            old_value="Pending",
            new_value="Approved",
            context=EventContext(
                source_object="ProcessInstanceStep",
                approval_comments="Looks good to me",
            ),
        )

        json_str = event.to_json()

        assert isinstance(json_str, str)
        assert "approval_decision" in json_str
        assert "Looks good to me" in json_str

    def test_event_has_extracted_at(self):
        """Test that event automatically sets extracted_at timestamp."""
        now = datetime.utcnow()

        event = MicroDecisionEvent(
            event_type=EventType.FIELD_CHANGE,
            timestamp_occurred=now,
            timestamp_recorded=now,
            actor_id="005xx000001X8Uz",
            actor_type=ActorType.USER,
            record_type="Case",
            record_id="500xx000001X8Uz",
            context=EventContext(source_object="CaseHistory"),
        )

        assert hasattr(event, "extracted_at")
        assert isinstance(event.extracted_at, datetime)

    def test_event_has_version(self):
        """Test that event has extractor version."""
        now = datetime.utcnow()

        event = MicroDecisionEvent(
            event_type=EventType.SETUP_CHANGE,
            timestamp_occurred=now,
            timestamp_recorded=now,
            actor_id="005xx000001X8Uz",
            actor_type=ActorType.USER,
            record_type="Configuration",
            record_id="Users",
            context=EventContext(source_object="SetupAuditTrail"),
        )

        assert hasattr(event, "extractor_version")
        assert event.extractor_version == "0.1.0"
