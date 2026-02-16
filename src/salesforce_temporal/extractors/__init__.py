"""
Extractors for Salesforce historical data.

This module contains specialized extractors for different types of Salesforce
temporal data sources.
"""

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

__all__ = [
    "BaseExtractor",
    "OpportunityHistoryExtractor",
    "FieldHistoryExtractor",
    "ApprovalHistoryExtractor",
    "ActivityExtractor",
    "SetupAuditTrailExtractor",
    "create_opportunity_field_history_extractor",
    "create_account_history_extractor",
    "create_case_history_extractor",
    "create_lead_history_extractor",
    "create_contact_history_extractor",
]
