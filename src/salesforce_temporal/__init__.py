"""
Salesforce Temporal Data Extraction Pipeline

This package extracts temporal/historical data from Salesforce and processes it
into standardized MicroDecisionEvent records for integration with Atlan.
"""

__version__ = "0.1.0"

from salesforce_temporal.extractors.activity import ActivityExtractor
from salesforce_temporal.extractors.approval_history import ApprovalHistoryExtractor
from salesforce_temporal.extractors.base import BaseExtractor
from salesforce_temporal.extractors.field_history import (
    FieldHistoryExtractor,
    create_account_history_extractor,
    create_case_history_extractor,
    create_contact_history_extractor,
    create_lead_history_extractor,
    create_opportunity_field_history_extractor,
)
from salesforce_temporal.extractors.opportunity_history import OpportunityHistoryExtractor
from salesforce_temporal.extractors.setup_audit_trail import SetupAuditTrailExtractor
from salesforce_temporal.models.events import (
    ActorType,
    EventContext,
    EventType,
    MicroDecisionEvent,
)

__all__ = [
    # Version
    "__version__",
    # Event Models
    "MicroDecisionEvent",
    "EventContext",
    "EventType",
    "ActorType",
    # Extractors
    "BaseExtractor",
    "OpportunityHistoryExtractor",
    "FieldHistoryExtractor",
    "ApprovalHistoryExtractor",
    "ActivityExtractor",
    "SetupAuditTrailExtractor",
    # Factory Functions
    "create_opportunity_field_history_extractor",
    "create_account_history_extractor",
    "create_case_history_extractor",
    "create_lead_history_extractor",
    "create_contact_history_extractor",
]
