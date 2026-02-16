"""
ApprovalHistoryExtractor for Salesforce approval process history.

This extractor navigates the approval process object hierarchy:
- ProcessInstance: The overall approval process
- ProcessInstanceStep: Individual steps in the approval process
- ProcessInstanceWorkitem: Pending approval requests

These objects capture critical decision points where records require explicit
approval from users, representing important governance and compliance micro-decisions.
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


class ApprovalHistoryExtractor(BaseExtractor):
    """
    Extract approval process history from ProcessInstance objects.

    Approval processes in Salesforce involve a hierarchy of objects:

    1. ProcessInstance: Represents a single approval process execution
       - TargetObjectId: The record being approved (Opportunity, Account, etc.)
       - Status: Overall status (Pending, Approved, Rejected)
       - SubmittedById: User who submitted for approval

    2. ProcessInstanceStep: Individual steps within the approval process
       - StepStatus: Status of this step (Pending, Approved, Rejected)
       - ActorId: User assigned to approve
       - Comments: Comments from the approver
       - CreatedDate: When step was created
       - SystemModstamp: When step was last modified

    3. ProcessInstanceWorkitem: Pending approval work items
       - ActorId: Current pending approver
       - OriginalActorId: Originally assigned approver

    This extractor flattens these relationships into MicroDecisionEvents,
    making it easy to analyze approval patterns, bottlenecks, and decision-makers.
    """

    def extract_events(
        self,
        incremental: bool = True,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Generator[MicroDecisionEvent, None, None]:
        """
        Extract approval process history as MicroDecisionEvents.

        Args:
            incremental: If True, only extract recent records
            start_date: Optional start date for extraction
            end_date: Optional end date for extraction

        Yields:
            MicroDecisionEvent instances for each approval decision
        """
        logger.info("Starting ApprovalHistory extraction...")

        # First, extract ProcessInstanceSteps (the actual approval decisions)
        yield from self._extract_process_instance_steps(incremental, start_date, end_date)

        # Then, extract pending workitems (future approvals)
        if not incremental:  # Only extract workitems in full refresh mode
            yield from self._extract_process_workitems()

        self.log_stats()

    def _extract_process_instance_steps(
        self,
        incremental: bool,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> Generator[MicroDecisionEvent, None, None]:
        """
        Extract completed approval steps (actual decisions made).

        Args:
            incremental: If True, only extract recent records
            start_date: Optional start date
            end_date: Optional end date

        Yields:
            MicroDecisionEvent instances
        """
        # Build query with subqueries to get related data
        query = """
            SELECT
                Id,
                ProcessInstanceId,
                StepStatus,
                ActorId,
                Comments,
                CreatedDate,
                SystemModstamp,
                ProcessInstance.TargetObjectId,
                ProcessInstance.Status,
                ProcessInstance.SubmittedById,
                ProcessInstance.ProcessDefinition.Name
            FROM ProcessInstanceStep
        """

        # Add date filters
        if start_date and end_date:
            start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            query += f" WHERE CreatedDate >= {start_str} AND CreatedDate <= {end_str}"
        elif incremental:
            query = self.build_incremental_query(query, "CreatedDate")

        query += " ORDER BY ProcessInstanceId, CreatedDate ASC"

        try:
            for record in self.execute_query(query):
                event = self._convert_step_to_event(record)
                if event:
                    yield event
                    self.extracted_count += 1

                    if self.extracted_count % 100 == 0:
                        logger.info(f"Processed {self.extracted_count} approval steps")

        except Exception as e:
            logger.error(f"Error extracting ProcessInstanceSteps: {e}")
            self.error_count += 1
            raise

    def _convert_step_to_event(self, record: Dict) -> Optional[MicroDecisionEvent]:
        """
        Convert a ProcessInstanceStep to a MicroDecisionEvent.

        Args:
            record: ProcessInstanceStep record

        Returns:
            MicroDecisionEvent or None
        """
        try:
            # Extract ProcessInstance data from subquery
            process_instance = record.get("ProcessInstance", {})
            target_object_id = process_instance.get("TargetObjectId")

            if not target_object_id:
                logger.warning(f"Missing TargetObjectId for step {record.get('Id')}")
                return None

            # Parse timestamps
            created_date = datetime.fromisoformat(record["CreatedDate"].replace("Z", "+00:00"))
            modified_date = datetime.fromisoformat(
                record["SystemModstamp"].replace("Z", "+00:00")
            )

            # Get actor information
            actor_id = record.get("ActorId")
            step_status = record.get("StepStatus", "Unknown")
            comments = record.get("Comments")

            # Get process definition name
            process_def = process_instance.get("ProcessDefinition", {})
            process_name = process_def.get("Name", "Unknown Process")

            # Determine record type from TargetObjectId (3-character prefix)
            # e.g., '006' = Opportunity, '001' = Account
            record_type = self._get_record_type_from_id(target_object_id)

            # Create context with approval-specific information
            context = EventContext(
                source_object="ProcessInstanceStep",
                source_record_id=record["Id"],
                related_records={
                    "ProcessInstanceId": record["ProcessInstanceId"],
                    "TargetObjectId": target_object_id,
                },
                approval_comments=comments,
                metadata={
                    "step_status": step_status,
                    "process_name": process_name,
                    "process_status": process_instance.get("Status"),
                    "submitted_by": process_instance.get("SubmittedById"),
                },
            )

            # Create the approval decision event
            event = MicroDecisionEvent(
                event_type=EventType.APPROVAL_DECISION,
                timestamp_occurred=created_date,
                timestamp_recorded=modified_date,
                actor_id=actor_id,
                actor_type=ActorType.USER if actor_id else ActorType.SYSTEM,
                record_type=record_type,
                record_id=target_object_id,
                field_name="ApprovalStatus",
                old_value="Pending",
                new_value=step_status,
                context=context,
                extractor_version=self.version,
            )

            return event

        except Exception as e:
            logger.error(f"Failed to convert approval step to event: {e}")
            self.error_count += 1
            return None

    def _extract_process_workitems(self) -> Generator[MicroDecisionEvent, None, None]:
        """
        Extract pending approval workitems (pending decisions).

        These represent approvals that are currently pending and haven't
        been acted upon yet.

        Yields:
            MicroDecisionEvent instances for pending approvals
        """
        query = """
            SELECT
                Id,
                ProcessInstanceId,
                ActorId,
                OriginalActorId,
                CreatedDate,
                ProcessInstance.TargetObjectId,
                ProcessInstance.Status,
                ProcessInstance.ProcessDefinition.Name
            FROM ProcessInstanceWorkitem
            WHERE ProcessInstance.Status = 'Pending'
        """

        query += " ORDER BY CreatedDate DESC"

        try:
            for record in self.execute_query(query):
                # Create events for pending approvals
                process_instance = record.get("ProcessInstance", {})
                target_object_id = process_instance.get("TargetObjectId")

                if not target_object_id:
                    continue

                created_date = datetime.fromisoformat(record["CreatedDate"].replace("Z", "+00:00"))
                actor_id = record.get("ActorId")
                record_type = self._get_record_type_from_id(target_object_id)

                process_def = process_instance.get("ProcessDefinition", {})
                process_name = process_def.get("Name", "Unknown Process")

                context = EventContext(
                    source_object="ProcessInstanceWorkitem",
                    source_record_id=record["Id"],
                    related_records={
                        "ProcessInstanceId": record["ProcessInstanceId"],
                        "TargetObjectId": target_object_id,
                    },
                    metadata={
                        "workitem_status": "Pending",
                        "process_name": process_name,
                        "original_actor": record.get("OriginalActorId"),
                    },
                )

                # Note: These are pending, so we don't know the outcome yet
                event = MicroDecisionEvent(
                    event_type=EventType.APPROVAL_DECISION,
                    timestamp_occurred=created_date,
                    timestamp_recorded=created_date,
                    actor_id=actor_id,
                    actor_type=ActorType.USER if actor_id else ActorType.SYSTEM,
                    record_type=record_type,
                    record_id=target_object_id,
                    field_name="ApprovalStatus",
                    old_value=None,
                    new_value="Pending",
                    context=context,
                    extractor_version=self.version,
                )

                yield event
                self.extracted_count += 1

        except Exception as e:
            logger.error(f"Error extracting ProcessInstanceWorkitems: {e}")
            self.error_count += 1

    def _get_record_type_from_id(self, salesforce_id: str) -> str:
        """
        Determine record type from Salesforce ID prefix.

        Salesforce IDs are 15 or 18 characters, with the first 3 characters
        indicating the object type.

        Args:
            salesforce_id: Salesforce record ID

        Returns:
            Object type name (e.g., 'Opportunity', 'Account')
        """
        if not salesforce_id or len(salesforce_id) < 3:
            return "Unknown"

        prefix = salesforce_id[:3]

        # Common object prefixes
        prefix_map = {
            "006": "Opportunity",
            "001": "Account",
            "500": "Case",
            "00Q": "Lead",
            "003": "Contact",
            "00T": "Task",
            "00U": "Event",
            "a00": "CustomObject",  # Custom objects start with 'a'
        }

        return prefix_map.get(prefix, "Unknown")

    def get_approval_metrics(self) -> Dict[str, any]:
        """
        Calculate approval process metrics.

        Returns:
            Dictionary with approval metrics:
            - Total approvals
            - Approval/rejection rates
            - Average approval time
            - Most frequent approvers
        """
        logger.info("Calculating approval metrics...")

        query = """
            SELECT
                COUNT(Id) total,
                StepStatus,
                ActorId
            FROM ProcessInstanceStep
            GROUP BY StepStatus, ActorId
        """

        metrics = {
            "total_steps": 0,
            "by_status": {},
            "by_actor": {},
        }

        try:
            for record in self.execute_query(query):
                count = record.get("expr0", 0)  # COUNT(Id) alias
                status = record.get("StepStatus", "Unknown")
                actor_id = record.get("ActorId", "Unknown")

                metrics["total_steps"] += count

                if status not in metrics["by_status"]:
                    metrics["by_status"][status] = 0
                metrics["by_status"][status] += count

                if actor_id not in metrics["by_actor"]:
                    metrics["by_actor"][actor_id] = 0
                metrics["by_actor"][actor_id] += count

        except Exception as e:
            logger.error(f"Failed to calculate approval metrics: {e}")

        return metrics
