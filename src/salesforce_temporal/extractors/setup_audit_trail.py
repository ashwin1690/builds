"""
SetupAuditTrailExtractor for Salesforce configuration changes.

SetupAuditTrail tracks administrative changes to Salesforce configuration:
- User permission changes
- Profile and role modifications
- Sharing rule changes
- Custom object and field modifications
- Workflow and automation changes

IMPORTANT: Salesforce only retains SetupAuditTrail data for 180 days.
This extractor should run on a schedule to preserve this critical history.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Generator, Optional

from salesforce_temporal.extractors.base import BaseExtractor
from salesforce_temporal.models.events import (
    ActorType,
    EventContext,
    EventType,
    MicroDecisionEvent,
)

logger = logging.getLogger(__name__)


class SetupAuditTrailExtractor(BaseExtractor):
    """
    Extract SetupAuditTrail records to preserve configuration change history.

    SetupAuditTrail captures administrative configuration changes in Salesforce,
    which are critical for:
    - Compliance and audit requirements
    - Security change tracking
    - Impact analysis for system issues
    - Understanding data model evolution

    Key limitation: Salesforce retains SetupAuditTrail data for only 180 days.
    This extractor should be run regularly (e.g., weekly) to preserve this
    history beyond the native retention period.

    SetupAuditTrail fields:
    - Id: Audit record ID
    - Action: Type of change (e.g., 'changedPassword', 'createdProfile')
    - Section: Area of change (e.g., 'Users', 'Profiles', 'Objects')
    - CreatedDate: When change occurred
    - CreatedById: Admin who made the change
    - Display: Human-readable description of the change
    - DelegateUser: User who made change on behalf of another (if applicable)
    """

    # Maximum retention period for SetupAuditTrail
    MAX_RETENTION_DAYS = 180

    def extract_events(
        self,
        incremental: bool = True,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        lookback_days: Optional[int] = None,
    ) -> Generator[MicroDecisionEvent, None, None]:
        """
        Extract SetupAuditTrail records as MicroDecisionEvents.

        Args:
            incremental: If True, only extract recent records
            start_date: Optional start date for extraction
            end_date: Optional end date for extraction
            lookback_days: Days to look back (max 180, default from settings)

        Yields:
            MicroDecisionEvent instances for each configuration change
        """
        logger.info("Starting SetupAuditTrail extraction...")

        # Validate lookback period
        if lookback_days:
            if lookback_days > self.MAX_RETENTION_DAYS:
                logger.warning(
                    f"Lookback period {lookback_days} exceeds maximum retention "
                    f"({self.MAX_RETENTION_DAYS} days). Using maximum."
                )
                lookback_days = self.MAX_RETENTION_DAYS
        else:
            # Use configured retention days, but cap at 180
            lookback_days = min(
                self.settings.setup_audit_retention_days,
                self.MAX_RETENTION_DAYS,
            )

        # Build the SOQL query
        fields = [
            "Id",
            "Action",
            "Section",
            "CreatedDate",
            "CreatedById",
            "Display",
            "DelegateUser",
        ]

        base_query = f"SELECT {', '.join(fields)} FROM SetupAuditTrail"

        # Add date filters
        if start_date and end_date:
            start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            query = f"{base_query} WHERE CreatedDate >= {start_str} AND CreatedDate <= {end_str}"
        elif incremental:
            # Use lookback days for incremental
            cutoff_date = datetime.utcnow() - timedelta(days=lookback_days)
            cutoff_str = cutoff_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            query = f"{base_query} WHERE CreatedDate >= {cutoff_str}"
        else:
            # Full extraction - get all available records (up to 180 days)
            cutoff_date = datetime.utcnow() - timedelta(days=self.MAX_RETENTION_DAYS)
            cutoff_str = cutoff_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            query = f"{base_query} WHERE CreatedDate >= {cutoff_str}"

        query += " ORDER BY CreatedDate ASC"

        logger.info(f"Extracting SetupAuditTrail with {lookback_days}-day lookback")

        # Execute query and process records
        try:
            for record in self.execute_query(query):
                event = self._convert_to_event(record)
                if event:
                    yield event
                    self.extracted_count += 1

                    if self.extracted_count % 100 == 0:
                        logger.info(f"Processed {self.extracted_count} SetupAuditTrail records")

        except Exception as e:
            logger.error(f"Error during SetupAuditTrail extraction: {e}")
            self.error_count += 1
            raise

        finally:
            self.log_stats()

    def _convert_to_event(self, record: Dict) -> Optional[MicroDecisionEvent]:
        """
        Convert a SetupAuditTrail record to a MicroDecisionEvent.

        Args:
            record: SetupAuditTrail record

        Returns:
            MicroDecisionEvent or None
        """
        try:
            timestamp = datetime.fromisoformat(record["CreatedDate"].replace("Z", "+00:00"))
            actor_id = record.get("CreatedById")
            delegate_user = record.get("DelegateUser")
            action = record.get("Action", "Unknown")
            section = record.get("Section", "Unknown")
            display = record.get("Display", "")

            # Create context with setup change details
            context = EventContext(
                source_object="SetupAuditTrail",
                source_record_id=record["Id"],
                metadata={
                    "action": action,
                    "section": section,
                    "display": display,
                    "delegate_user": delegate_user,
                },
            )

            # Determine actor type
            if delegate_user:
                actor_type = ActorType.USER
            elif actor_id:
                actor_type = ActorType.USER
            else:
                actor_type = ActorType.SYSTEM

            # For setup changes, we use "Configuration" as the record type
            # and the section as the record ID
            event = MicroDecisionEvent(
                event_type=EventType.SETUP_CHANGE,
                timestamp_occurred=timestamp,
                timestamp_recorded=timestamp,
                actor_id=actor_id or delegate_user,
                actor_type=actor_type,
                record_type="Configuration",
                record_id=section,
                field_name=action,
                old_value=None,
                new_value=display,
                context=context,
                extractor_version=self.version,
            )

            return event

        except Exception as e:
            logger.error(f"Failed to convert SetupAuditTrail record to event: {e}")
            self.error_count += 1
            return None

    def get_change_summary(self) -> Dict[str, any]:
        """
        Get summary of configuration changes by section and action.

        Returns:
            Dictionary with change statistics
        """
        logger.info("Calculating setup change summary...")

        query = """
            SELECT COUNT(Id) total, Section, Action
            FROM SetupAuditTrail
            GROUP BY Section, Action
        """

        summary = {
            "by_section": {},
            "by_action": {},
            "total_changes": 0,
        }

        try:
            for record in self.execute_query(query):
                count = record.get("expr0", 0)
                section = record.get("Section", "Unknown")
                action = record.get("Action", "Unknown")

                summary["total_changes"] += count

                if section not in summary["by_section"]:
                    summary["by_section"][section] = 0
                summary["by_section"][section] += count

                if action not in summary["by_action"]:
                    summary["by_action"][action] = 0
                summary["by_action"][action] += count

        except Exception as e:
            logger.error(f"Failed to calculate change summary: {e}")

        return summary

    def get_recent_critical_changes(self, days: int = 7) -> list:
        """
        Get recent critical configuration changes.

        Critical changes include:
        - Permission set/profile modifications
        - Sharing rule changes
        - User deactivations
        - Security settings changes

        Args:
            days: Number of days to look back

        Returns:
            List of critical change records
        """
        logger.info(f"Fetching critical changes from last {days} days...")

        # Define critical actions
        critical_actions = [
            "changedUserPermissions",
            "changedProfile",
            "changedSharingRules",
            "deactivatedUser",
            "changedPasswordPolicies",
            "changedSessionSettings",
            "changedNetworkAccess",
        ]

        cutoff_date = datetime.utcnow() - timedelta(days=days)
        cutoff_str = cutoff_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        actions_str = "', '".join(critical_actions)
        query = f"""
            SELECT Id, Action, Section, CreatedDate, CreatedById, Display
            FROM SetupAuditTrail
            WHERE Action IN ('{actions_str}')
            AND CreatedDate >= {cutoff_str}
            ORDER BY CreatedDate DESC
        """

        critical_changes = []
        try:
            for record in self.execute_query(query):
                critical_changes.append({
                    "id": record.get("Id"),
                    "action": record.get("Action"),
                    "section": record.get("Section"),
                    "created_date": record.get("CreatedDate"),
                    "created_by": record.get("CreatedById"),
                    "display": record.get("Display"),
                })

        except Exception as e:
            logger.error(f"Failed to fetch critical changes: {e}")

        logger.info(f"Found {len(critical_changes)} critical changes")
        return critical_changes

    def get_most_active_admins(self, days: int = 30, limit: int = 10) -> list:
        """
        Get the most active administrators by setup change count.

        Args:
            days: Number of days to look back
            limit: Maximum number of admins to return

        Returns:
            List of tuples (admin_id, change_count)
        """
        logger.info(f"Calculating most active admins over {days} days...")

        cutoff_date = datetime.utcnow() - timedelta(days=days)
        cutoff_str = cutoff_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        query = f"""
            SELECT COUNT(Id) total, CreatedById
            FROM SetupAuditTrail
            WHERE CreatedDate >= {cutoff_str}
            AND CreatedById != null
            GROUP BY CreatedById
            ORDER BY COUNT(Id) DESC
            LIMIT {limit}
        """

        active_admins = []
        try:
            for record in self.execute_query(query):
                count = record.get("expr0", 0)
                admin_id = record.get("CreatedById")
                if admin_id:
                    active_admins.append((admin_id, count))

        except Exception as e:
            logger.error(f"Failed to get active admins: {e}")

        return active_admins
