"""
ActivityExtractor for Task and Event records.

Task and Event objects in Salesforce represent activities associated with records:
- Task: To-do items, calls, emails (has due date, completion status)
- Event: Calendar events, meetings (has start/end time)

Both relate to records via:
- WhoId: Person (Lead, Contact)
- WhatId: Object (Account, Opportunity, Case, Custom Objects)

These activities represent temporal engagement patterns and can reveal:
- Sales cadence and follow-up patterns
- Customer service responsiveness
- Deal progression indicators
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


class ActivityExtractor(BaseExtractor):
    """
    Extract Task and Event activity records as MicroDecisionEvents.

    Activities in Salesforce capture user engagement with records:

    Task fields:
    - Subject: Description of the task
    - Status: New, In Progress, Completed, Deferred, etc.
    - Priority: Normal, High, Low
    - ActivityDate: Due date
    - WhoId: Related person (Lead/Contact)
    - WhatId: Related record (Account/Opportunity/etc)
    - OwnerId: Task owner
    - IsClosed: Whether task is completed

    Event fields:
    - Subject: Event description
    - StartDateTime: When event starts
    - EndDateTime: When event ends
    - WhoId: Related person
    - WhatId: Related record
    - OwnerId: Event owner
    - EventSubtype: Type (Call, Email, Meeting, etc.)

    Both Task and Event completion represent micro-decisions about
    customer engagement and resource allocation.
    """

    def extract_events(
        self,
        incremental: bool = True,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        extract_tasks: bool = True,
        extract_events: bool = True,
    ) -> Generator[MicroDecisionEvent, None, None]:
        """
        Extract Task and Event activities as MicroDecisionEvents.

        Args:
            incremental: If True, only extract recent records
            start_date: Optional start date for extraction
            end_date: Optional end date for extraction
            extract_tasks: Include Task records
            extract_events: Include Event records

        Yields:
            MicroDecisionEvent instances for each activity
        """
        logger.info("Starting Activity extraction...")

        if extract_tasks:
            logger.info("Extracting Task records...")
            yield from self._extract_tasks(incremental, start_date, end_date)

        if extract_events:
            logger.info("Extracting Event records...")
            yield from self._extract_events(incremental, start_date, end_date)

        self.log_stats()

    def _extract_tasks(
        self,
        incremental: bool,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> Generator[MicroDecisionEvent, None, None]:
        """
        Extract Task records.

        Args:
            incremental: If True, only extract recent records
            start_date: Optional start date
            end_date: Optional end date

        Yields:
            MicroDecisionEvent instances
        """
        fields = [
            "Id",
            "Subject",
            "Status",
            "Priority",
            "ActivityDate",
            "CreatedDate",
            "CompletedDateTime",
            "WhoId",
            "WhatId",
            "OwnerId",
            "IsClosed",
            "TaskSubtype",
            "CallType",
            "CallDisposition",
            "Description",
        ]

        query = f"SELECT {', '.join(fields)} FROM Task"

        # Add date filters
        if start_date and end_date:
            start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            query += f" WHERE CreatedDate >= {start_str} AND CreatedDate <= {end_str}"
        elif incremental:
            query = self.build_incremental_query(query, "CreatedDate")

        query += " ORDER BY CreatedDate ASC"

        try:
            for record in self.execute_query(query):
                event = self._convert_task_to_event(record)
                if event:
                    yield event
                    self.extracted_count += 1

                    if self.extracted_count % 1000 == 0:
                        logger.info(f"Processed {self.extracted_count} Task records")

        except Exception as e:
            logger.error(f"Error extracting Tasks: {e}")
            self.error_count += 1
            raise

    def _convert_task_to_event(self, record: Dict) -> Optional[MicroDecisionEvent]:
        """
        Convert a Task record to a MicroDecisionEvent.

        Args:
            record: Task record

        Returns:
            MicroDecisionEvent or None
        """
        try:
            # Determine the primary related record (prefer WhatId over WhoId)
            related_record_id = record.get("WhatId") or record.get("WhoId")
            if not related_record_id:
                # Task not related to any record, skip
                return None

            record_type = self._get_record_type_from_id(related_record_id)

            # Parse timestamps
            created_date = datetime.fromisoformat(record["CreatedDate"].replace("Z", "+00:00"))

            # Use CompletedDateTime if available, otherwise CreatedDate
            completed_str = record.get("CompletedDateTime")
            if completed_str:
                timestamp_occurred = datetime.fromisoformat(completed_str.replace("Z", "+00:00"))
            else:
                timestamp_occurred = created_date

            # Build related records dict
            related_records = {}
            if record.get("WhoId"):
                related_records["WhoId"] = record["WhoId"]
            if record.get("WhatId"):
                related_records["WhatId"] = record["WhatId"]

            # Create context
            context = EventContext(
                source_object="Task",
                source_record_id=record["Id"],
                related_records=related_records,
                activity_type="Task",
                activity_subject=record.get("Subject"),
                metadata={
                    "status": record.get("Status"),
                    "priority": record.get("Priority"),
                    "task_subtype": record.get("TaskSubtype"),
                    "call_type": record.get("CallType"),
                    "call_disposition": record.get("CallDisposition"),
                    "is_closed": record.get("IsClosed"),
                    "activity_date": record.get("ActivityDate"),
                },
            )

            # Create event
            event = MicroDecisionEvent(
                event_type=EventType.ACTIVITY,
                timestamp_occurred=timestamp_occurred,
                timestamp_recorded=created_date,
                actor_id=record.get("OwnerId"),
                actor_type=ActorType.USER,
                record_type=record_type,
                record_id=related_record_id,
                field_name="TaskStatus",
                old_value=None,
                new_value=record.get("Status"),
                context=context,
                extractor_version=self.version,
            )

            return event

        except Exception as e:
            logger.error(f"Failed to convert Task to event: {e}")
            self.error_count += 1
            return None

    def _extract_events(
        self,
        incremental: bool,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> Generator[MicroDecisionEvent, None, None]:
        """
        Extract Event (calendar event) records.

        Args:
            incremental: If True, only extract recent records
            start_date: Optional start date
            end_date: Optional end date

        Yields:
            MicroDecisionEvent instances
        """
        fields = [
            "Id",
            "Subject",
            "StartDateTime",
            "EndDateTime",
            "CreatedDate",
            "WhoId",
            "WhatId",
            "OwnerId",
            "EventSubtype",
            "IsAllDayEvent",
            "Description",
            "Location",
        ]

        query = f"SELECT {', '.join(fields)} FROM Event"

        # Add date filters
        if start_date and end_date:
            start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            query += f" WHERE CreatedDate >= {start_str} AND CreatedDate <= {end_str}"
        elif incremental:
            query = self.build_incremental_query(query, "CreatedDate")

        query += " ORDER BY StartDateTime ASC"

        try:
            for record in self.execute_query(query):
                event = self._convert_event_to_event(record)
                if event:
                    yield event
                    self.extracted_count += 1

                    if self.extracted_count % 1000 == 0:
                        logger.info(f"Processed {self.extracted_count} Event records")

        except Exception as e:
            logger.error(f"Error extracting Events: {e}")
            self.error_count += 1
            raise

    def _convert_event_to_event(self, record: Dict) -> Optional[MicroDecisionEvent]:
        """
        Convert an Event (calendar) record to a MicroDecisionEvent.

        Args:
            record: Event record

        Returns:
            MicroDecisionEvent or None
        """
        try:
            # Determine the primary related record
            related_record_id = record.get("WhatId") or record.get("WhoId")
            if not related_record_id:
                return None

            record_type = self._get_record_type_from_id(related_record_id)

            # Parse timestamps
            start_datetime = datetime.fromisoformat(record["StartDateTime"].replace("Z", "+00:00"))
            end_datetime_str = record.get("EndDateTime")
            end_datetime = (
                datetime.fromisoformat(end_datetime_str.replace("Z", "+00:00"))
                if end_datetime_str
                else None
            )
            created_date = datetime.fromisoformat(record["CreatedDate"].replace("Z", "+00:00"))

            # Build related records dict
            related_records = {}
            if record.get("WhoId"):
                related_records["WhoId"] = record["WhoId"]
            if record.get("WhatId"):
                related_records["WhatId"] = record["WhatId"]

            # Calculate duration if we have end time
            duration_hours = None
            if end_datetime:
                duration_hours = (end_datetime - start_datetime).total_seconds() / 3600

            # Create context
            context = EventContext(
                source_object="Event",
                source_record_id=record["Id"],
                related_records=related_records,
                activity_type="Event",
                activity_subject=record.get("Subject"),
                metadata={
                    "event_subtype": record.get("EventSubtype"),
                    "is_all_day": record.get("IsAllDayEvent"),
                    "location": record.get("Location"),
                    "start_datetime": record.get("StartDateTime"),
                    "end_datetime": record.get("EndDateTime"),
                    "duration_hours": duration_hours,
                },
            )

            # Create event
            event = MicroDecisionEvent(
                event_type=EventType.ACTIVITY,
                timestamp_occurred=start_datetime,
                timestamp_recorded=created_date,
                actor_id=record.get("OwnerId"),
                actor_type=ActorType.USER,
                record_type=record_type,
                record_id=related_record_id,
                field_name="EventOccurred",
                old_value=None,
                new_value=record.get("Subject"),
                context=context,
                extractor_version=self.version,
            )

            return event

        except Exception as e:
            logger.error(f"Failed to convert Event to event: {e}")
            self.error_count += 1
            return None

    def _get_record_type_from_id(self, salesforce_id: str) -> str:
        """
        Determine record type from Salesforce ID prefix.

        Args:
            salesforce_id: Salesforce record ID

        Returns:
            Object type name
        """
        if not salesforce_id or len(salesforce_id) < 3:
            return "Unknown"

        prefix = salesforce_id[:3]

        prefix_map = {
            "006": "Opportunity",
            "001": "Account",
            "500": "Case",
            "00Q": "Lead",
            "003": "Contact",
            "701": "Campaign",
            "a00": "CustomObject",
        }

        return prefix_map.get(prefix, "Unknown")

    def get_activity_summary(self) -> Dict[str, any]:
        """
        Get summary statistics for activities.

        Returns:
            Dictionary with activity metrics
        """
        logger.info("Calculating activity summary...")

        # Task summary
        task_query = """
            SELECT COUNT(Id) total, Status
            FROM Task
            GROUP BY Status
        """

        # Event summary
        event_query = """
            SELECT COUNT(Id) total, EventSubtype
            FROM Event
            GROUP BY EventSubtype
        """

        summary = {
            "tasks_by_status": {},
            "events_by_subtype": {},
        }

        try:
            # Get task stats
            for record in self.execute_query(task_query):
                status = record.get("Status", "Unknown")
                count = record.get("expr0", 0)
                summary["tasks_by_status"][status] = count

            # Get event stats
            for record in self.execute_query(event_query):
                subtype = record.get("EventSubtype", "Unknown")
                count = record.get("expr0", 0)
                summary["events_by_subtype"][subtype] = count

        except Exception as e:
            logger.error(f"Failed to calculate activity summary: {e}")

        return summary
