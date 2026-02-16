"""
Generic FieldHistoryExtractor for any Salesforce field history tracking object.

This extractor works with any FieldHistory object:
- OpportunityFieldHistory
- AccountHistory (which is actually AccountFieldHistory in practice)
- CaseHistory
- LeadHistory
- ContactHistory
- CustomObject__History

Field history tracking must be enabled for the specific fields in Salesforce Setup.
"""

import logging
from datetime import datetime
from typing import Dict, Generator, Optional

from salesforce_temporal.extractors.base import BaseExtractor
from salesforce_temporal.models.events import (
    ActorType,
    EventContext,
    EventType,
    MicroDecisionEvent,
)

logger = logging.getLogger(__name__)


class FieldHistoryExtractor(BaseExtractor):
    """
    Generic extractor for any Salesforce FieldHistory object.

    This extractor is parameterized by object name and can extract field
    history from any standard or custom object that has field history tracking
    enabled.

    Field history objects have a standard structure:
    - Id: History record ID
    - ParentId: ID of the parent record (Opportunity, Account, etc.)
    - CreatedById: User who made the change
    - CreatedDate: When the change occurred
    - Field: Name of the field that changed
    - OldValue: Previous value
    - NewValue: New value
    - DataType: Type of the field (e.g., 'string', 'number')

    Usage:
        # Extract OpportunityFieldHistory
        extractor = FieldHistoryExtractor("OpportunityFieldHistory", "Opportunity")
        for event in extractor.extract_events():
            process(event)

        # Extract AccountHistory
        extractor = FieldHistoryExtractor("AccountHistory", "Account")
        for event in extractor.extract_events():
            process(event)
    """

    # Mapping of history object names to their parent record field
    PARENT_FIELD_MAP = {
        "OpportunityFieldHistory": "OpportunityId",
        "AccountHistory": "AccountId",
        "CaseHistory": "CaseId",
        "LeadHistory": "LeadId",
        "ContactHistory": "ContactId",
    }

    def __init__(
        self,
        history_object_name: str,
        parent_object_name: str,
        *args,
        **kwargs,
    ):
        """
        Initialize the FieldHistory extractor.

        Args:
            history_object_name: Name of the history object (e.g., 'OpportunityFieldHistory')
            parent_object_name: Name of the parent object (e.g., 'Opportunity')
        """
        super().__init__(*args, **kwargs)
        self.history_object_name = history_object_name
        self.parent_object_name = parent_object_name

        # Determine the parent ID field name
        self.parent_id_field = self.PARENT_FIELD_MAP.get(
            history_object_name,
            f"{parent_object_name}Id",
        )

    def extract_events(
        self,
        incremental: bool = True,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        use_bulk_api: bool = False,
    ) -> Generator[MicroDecisionEvent, None, None]:
        """
        Extract field history records as MicroDecisionEvents.

        Args:
            incremental: If True, only extract recent records
            start_date: Optional start date for extraction
            end_date: Optional end date for extraction
            use_bulk_api: Use Bulk API 2.0 with PK Chunking for large volumes

        Yields:
            MicroDecisionEvent instances for each field change
        """
        logger.info(f"Starting {self.history_object_name} extraction...")

        # Build the SOQL query
        fields = [
            "Id",
            self.parent_id_field,
            "CreatedDate",
            "CreatedById",
            "Field",
            "OldValue",
            "NewValue",
            "DataType",
        ]

        # Some history objects might have IsDeleted
        base_query = f"SELECT {', '.join(fields)} FROM {self.history_object_name}"

        # Add date filters
        if start_date and end_date:
            start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            query = f"{base_query} WHERE CreatedDate >= {start_str} AND CreatedDate <= {end_str}"
        elif incremental:
            query = self.build_incremental_query(base_query, "CreatedDate")
        else:
            query = base_query

        # Add ordering for consistent processing
        query += f" ORDER BY {self.parent_id_field}, CreatedDate ASC"

        # Execute query and process records
        try:
            for record in self.execute_query(query, use_bulk=use_bulk_api):
                event = self._convert_to_event(record)
                if event:
                    yield event
                    self.extracted_count += 1

                    # Log progress
                    if self.extracted_count % 1000 == 0:
                        logger.info(
                            f"Processed {self.extracted_count} {self.history_object_name} records"
                        )

        except Exception as e:
            logger.error(f"Error during {self.history_object_name} extraction: {e}")
            self.error_count += 1
            raise

        finally:
            self.log_stats()

    def _convert_to_event(self, record: Dict) -> Optional[MicroDecisionEvent]:
        """
        Convert a field history record to a MicroDecisionEvent.

        Args:
            record: Field history record from Salesforce

        Returns:
            MicroDecisionEvent or None if conversion fails
        """
        try:
            parent_id = record.get(self.parent_id_field)
            if not parent_id:
                logger.warning(f"Missing parent ID in record: {record.get('Id')}")
                return None

            timestamp = datetime.fromisoformat(record["CreatedDate"].replace("Z", "+00:00"))
            actor_id = record.get("CreatedById")
            field_name = record.get("Field")
            old_value = record.get("OldValue")
            new_value = record.get("NewValue")
            data_type = record.get("DataType")

            # Create event context
            context = EventContext(
                source_object=self.history_object_name,
                source_record_id=record["Id"],
                related_records={self.parent_id_field: parent_id},
                metadata={"data_type": data_type} if data_type else {},
            )

            # Determine event type based on field change
            event_type = self._determine_event_type(field_name, old_value, new_value)

            # Create the event
            event = MicroDecisionEvent(
                event_type=event_type,
                timestamp_occurred=timestamp,
                timestamp_recorded=timestamp,
                actor_id=actor_id,
                actor_type=ActorType.USER if actor_id else ActorType.SYSTEM,
                record_type=self.parent_object_name,
                record_id=parent_id,
                field_name=field_name,
                old_value=old_value,
                new_value=new_value,
                context=context,
                extractor_version=self.version,
            )

            return event

        except Exception as e:
            logger.error(f"Failed to convert record to event: {e}")
            self.error_count += 1
            return None

    def _determine_event_type(
        self,
        field_name: Optional[str],
        old_value: any,
        new_value: any,
    ) -> EventType:
        """
        Determine the appropriate event type based on the field change.

        Args:
            field_name: Name of the field that changed
            old_value: Previous value
            new_value: New value

        Returns:
            EventType enum value
        """
        if not field_name:
            return EventType.FIELD_CHANGE

        # Special handling for owner changes
        if "owner" in field_name.lower():
            return EventType.OWNER_CHANGE

        # Check if this is a record creation (no old value)
        if old_value is None and new_value is not None:
            return EventType.FIELD_CHANGE

        # Default to field change
        return EventType.FIELD_CHANGE

    def extract_by_field_name(
        self,
        field_name: str,
        incremental: bool = True,
    ) -> Generator[MicroDecisionEvent, None, None]:
        """
        Extract field history for a specific field only.

        This is useful when you want to focus on changes to a particular field,
        e.g., only Status changes or only Amount changes.

        Args:
            field_name: Name of the field to extract (e.g., 'Status', 'Amount')
            incremental: If True, only extract recent records

        Yields:
            MicroDecisionEvent instances for the specified field
        """
        logger.info(f"Extracting {self.history_object_name} for field: {field_name}")

        fields = [
            "Id",
            self.parent_id_field,
            "CreatedDate",
            "CreatedById",
            "Field",
            "OldValue",
            "NewValue",
            "DataType",
        ]

        base_query = f"SELECT {', '.join(fields)} FROM {self.history_object_name} WHERE Field = '{field_name}'"

        if incremental:
            query = self.build_incremental_query(base_query, "CreatedDate")
        else:
            query = base_query

        query += f" ORDER BY {self.parent_id_field}, CreatedDate ASC"

        try:
            for record in self.execute_query(query):
                event = self._convert_to_event(record)
                if event:
                    yield event
                    self.extracted_count += 1

        except Exception as e:
            logger.error(f"Error extracting field {field_name}: {e}")
            self.error_count += 1
            raise

    def get_tracked_fields(self) -> list:
        """
        Get list of fields currently being tracked in field history.

        Returns:
            List of field names
        """
        logger.info(f"Getting tracked fields for {self.history_object_name}")

        query = f"SELECT Field FROM {self.history_object_name} GROUP BY Field"

        tracked_fields = []
        try:
            for record in self.execute_query(query):
                field_name = record.get("Field")
                if field_name:
                    tracked_fields.append(field_name)

        except Exception as e:
            logger.error(f"Failed to get tracked fields: {e}")

        logger.info(f"Found {len(tracked_fields)} tracked fields")
        return tracked_fields


# Convenience factory functions for common history objects


def create_opportunity_field_history_extractor(**kwargs) -> FieldHistoryExtractor:
    """Create an OpportunityFieldHistory extractor."""
    return FieldHistoryExtractor("OpportunityFieldHistory", "Opportunity", **kwargs)


def create_account_history_extractor(**kwargs) -> FieldHistoryExtractor:
    """Create an AccountHistory extractor."""
    return FieldHistoryExtractor("AccountHistory", "Account", **kwargs)


def create_case_history_extractor(**kwargs) -> FieldHistoryExtractor:
    """Create a CaseHistory extractor."""
    return FieldHistoryExtractor("CaseHistory", "Case", **kwargs)


def create_lead_history_extractor(**kwargs) -> FieldHistoryExtractor:
    """Create a LeadHistory extractor."""
    return FieldHistoryExtractor("LeadHistory", "Lead", **kwargs)


def create_contact_history_extractor(**kwargs) -> FieldHistoryExtractor:
    """Create a ContactHistory extractor."""
    return FieldHistoryExtractor("ContactHistory", "Contact", **kwargs)
