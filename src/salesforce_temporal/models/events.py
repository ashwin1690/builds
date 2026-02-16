"""
Core event schema models for temporal data extraction.

This module defines the MicroDecisionEvent schema that standardizes all temporal
data from Salesforce into a unified format for downstream processing and Atlan integration.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    """Types of micro-decision events."""

    FIELD_CHANGE = "field_change"
    STAGE_CHANGE = "stage_change"
    APPROVAL_DECISION = "approval_decision"
    ACTIVITY = "activity"
    SETUP_CHANGE = "setup_change"
    RECORD_CREATE = "record_create"
    RECORD_DELETE = "record_delete"
    OWNER_CHANGE = "owner_change"


class ActorType(str, Enum):
    """Types of actors who can trigger events."""

    USER = "user"
    SYSTEM = "system"
    AUTOMATION = "automation"
    INTEGRATION = "integration"
    UNKNOWN = "unknown"


class EventContext(BaseModel):
    """
    Additional contextual information about the event.

    This captures metadata that helps understand the circumstances
    around the micro-decision.
    """

    source_object: str = Field(
        ..., description="Salesforce object that generated this event (e.g., 'OpportunityHistory')"
    )
    source_record_id: Optional[str] = Field(
        None, description="ID of the source history/audit record"
    )
    org_id: Optional[str] = Field(None, description="Salesforce org ID")
    org_instance: Optional[str] = Field(None, description="Salesforce instance (e.g., 'na135')")
    related_records: Optional[Dict[str, str]] = Field(
        default_factory=dict,
        description="Related record IDs (e.g., AccountId, ContactId)",
    )
    stage_duration_days: Optional[float] = Field(
        None, description="Duration in current stage (for opportunity history)"
    )
    previous_stage: Optional[str] = Field(
        None, description="Previous stage value (for stage changes)"
    )
    approval_comments: Optional[str] = Field(None, description="Comments from approval process")
    activity_type: Optional[str] = Field(None, description="Type of activity (Task, Event)")
    activity_subject: Optional[str] = Field(None, description="Subject line of activity")
    metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Additional metadata"
    )

    model_config = {"extra": "allow"}


class MicroDecisionEvent(BaseModel):
    """
    Standardized event representing a micro-decision or temporal data point.

    This schema captures any change, activity, or decision point in Salesforce
    that represents a business process micro-decision. All extractors emit
    records in this format.

    Example:
        >>> event = MicroDecisionEvent(
        ...     event_type=EventType.STAGE_CHANGE,
        ...     timestamp_occurred=datetime.now(),
        ...     timestamp_recorded=datetime.now(),
        ...     actor_id="005xx000001X8Uz",
        ...     actor_type=ActorType.USER,
        ...     record_type="Opportunity",
        ...     record_id="006xx000001X8Uz",
        ...     field_name="StageName",
        ...     old_value="Prospecting",
        ...     new_value="Qualification",
        ...     context=EventContext(
        ...         source_object="OpportunityHistory",
        ...         stage_duration_days=5.2
        ...     )
        ... )
    """

    event_type: EventType = Field(
        ..., description="Type of event (field_change, stage_change, approval, etc.)"
    )

    timestamp_occurred: datetime = Field(
        ...,
        description="When the event actually occurred in the business process",
    )

    timestamp_recorded: datetime = Field(
        ...,
        description="When the event was recorded in Salesforce (usually via CreatedDate)",
    )

    actor_id: Optional[str] = Field(
        None,
        description="Salesforce ID of the user/system that triggered the event",
    )

    actor_type: ActorType = Field(
        ..., description="Type of actor (user, system, automation, etc.)"
    )

    record_type: str = Field(
        ...,
        description="Type of primary record affected (Opportunity, Account, Case, etc.)",
    )

    record_id: str = Field(
        ...,
        description="Salesforce ID of the primary record affected",
    )

    field_name: Optional[str] = Field(
        None,
        description="Name of the field that changed (null for non-field-change events)",
    )

    old_value: Optional[Any] = Field(
        None,
        description="Previous value of the field (null for creates)",
    )

    new_value: Optional[Any] = Field(
        None,
        description="New value of the field (null for deletes)",
    )

    context: EventContext = Field(
        ...,
        description="Additional contextual information about the event",
    )

    # Internal tracking fields
    extracted_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When this event was extracted from Salesforce",
    )

    extractor_version: str = Field(
        default="0.1.0",
        description="Version of the extractor that produced this event",
    )

    @field_validator("timestamp_occurred", "timestamp_recorded", mode="before")
    @classmethod
    def parse_datetime(cls, v: Any) -> datetime:
        """Parse datetime from various formats."""
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            # Handle ISO format and common Salesforce formats
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError:
                # Try common formats
                for fmt in ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]:
                    try:
                        return datetime.strptime(v, fmt)
                    except ValueError:
                        continue
                raise ValueError(f"Could not parse datetime: {v}")
        raise ValueError(f"Invalid datetime type: {type(v)}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary with JSON-serializable values."""
        return self.model_dump(mode="json")

    def to_json(self) -> str:
        """Convert event to JSON string."""
        return self.model_dump_json()

    model_config = {"use_enum_values": True, "json_encoders": {datetime: lambda v: v.isoformat()}}
