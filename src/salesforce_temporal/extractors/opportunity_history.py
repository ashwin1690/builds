"""
OpportunityHistoryExtractor - Highest-value, lowest-effort temporal data extraction.

This extractor pulls OpportunityHistory records which track stage changes and
other key field changes on Opportunity records. Stage transitions are critical
micro-decisions in the sales process.
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


class OpportunityHistoryExtractor(BaseExtractor):
    """
    Extract OpportunityHistory records and convert to MicroDecisionEvents.

    OpportunityHistory tracks changes to key Opportunity fields, most importantly
    StageName, which represents progression through the sales pipeline. This is
    one of the highest-value temporal data sources for understanding sales velocity,
    conversion patterns, and bottlenecks.

    Key fields extracted:
    - StageName changes (stage progression)
    - Amount changes (deal size evolution)
    - Probability changes (likelihood updates)
    - CloseDate changes (timeline adjustments)
    - ForecastCategory changes
    """

    def __init__(self, *args, **kwargs):
        """Initialize the OpportunityHistory extractor."""
        super().__init__(*args, **kwargs)
        self.stage_durations: Dict[str, float] = {}

    def calculate_stage_duration(
        self,
        opportunity_id: str,
        current_timestamp: datetime,
    ) -> Optional[float]:
        """
        Calculate duration in current stage.

        This is a simplified calculation. For production, you'd want to:
        1. Query all history records for the opportunity
        2. Sort by CreatedDate
        3. Calculate time between consecutive stage changes

        Args:
            opportunity_id: Salesforce Opportunity ID
            current_timestamp: Timestamp of current history record

        Returns:
            Duration in days, or None if unable to calculate
        """
        # Placeholder for stage duration calculation
        # In production, implement proper calculation by querying previous records
        return None

    def extract_events(
        self,
        incremental: bool = True,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Generator[MicroDecisionEvent, None, None]:
        """
        Extract OpportunityHistory records as MicroDecisionEvents.

        Args:
            incremental: If True, only extract recent records
            start_date: Optional start date for extraction
            end_date: Optional end date for extraction

        Yields:
            MicroDecisionEvent instances for each history record
        """
        logger.info("Starting OpportunityHistory extraction...")

        # Build the SOQL query
        fields = [
            "Id",
            "OpportunityId",
            "CreatedDate",
            "CreatedById",
            "StageName",
            "Amount",
            "ExpectedRevenue",
            "CloseDate",
            "Probability",
            "ForecastCategory",
            "IsDeleted",
        ]

        base_query = f"SELECT {', '.join(fields)} FROM OpportunityHistory"

        # Add date filters
        if start_date and end_date:
            start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            query = f"{base_query} WHERE CreatedDate >= {start_str} AND CreatedDate <= {end_str}"
        elif incremental:
            query = self.build_incremental_query(base_query, "CreatedDate")
        else:
            query = base_query

        # Add ordering
        query += " ORDER BY OpportunityId, CreatedDate ASC"

        # Execute query and process records
        try:
            previous_record: Optional[Dict] = None
            current_opportunity_id: Optional[str] = None

            for record in self.execute_query(query):
                opportunity_id = record["OpportunityId"]

                # Detect field changes by comparing with previous record
                # for the same opportunity
                if current_opportunity_id != opportunity_id:
                    # New opportunity, reset context
                    current_opportunity_id = opportunity_id
                    previous_record = None

                # Generate events for this history record
                yield from self._process_history_record(record, previous_record)

                # Store as previous for next comparison
                previous_record = record
                self.extracted_count += 1

                # Log progress
                if self.extracted_count % 1000 == 0:
                    logger.info(f"Processed {self.extracted_count} OpportunityHistory records")

        except Exception as e:
            logger.error(f"Error during OpportunityHistory extraction: {e}")
            self.error_count += 1
            raise

        finally:
            self.log_stats()

    def _process_history_record(
        self,
        record: Dict,
        previous_record: Optional[Dict],
    ) -> Generator[MicroDecisionEvent, None, None]:
        """
        Process a single OpportunityHistory record into events.

        Args:
            record: Current history record
            previous_record: Previous history record for same opportunity (if any)

        Yields:
            MicroDecisionEvent instances
        """
        opportunity_id = record["OpportunityId"]
        timestamp = datetime.fromisoformat(record["CreatedDate"].replace("Z", "+00:00"))
        actor_id = record.get("CreatedById")

        # Base context for all events from this record
        base_context = EventContext(
            source_object="OpportunityHistory",
            source_record_id=record["Id"],
            related_records={"OpportunityId": opportunity_id},
        )

        # Track which fields changed
        changed_fields = {
            "StageName": EventType.STAGE_CHANGE,
            "Amount": EventType.FIELD_CHANGE,
            "Probability": EventType.FIELD_CHANGE,
            "CloseDate": EventType.FIELD_CHANGE,
            "ForecastCategory": EventType.FIELD_CHANGE,
            "ExpectedRevenue": EventType.FIELD_CHANGE,
        }

        for field_name, event_type in changed_fields.items():
            current_value = record.get(field_name)
            previous_value = previous_record.get(field_name) if previous_record else None

            # Only emit event if value changed
            if previous_record is None or current_value != previous_value:
                # Special handling for stage changes
                context = EventContext(**base_context.model_dump())

                if field_name == "StageName":
                    context.previous_stage = previous_value
                    # Calculate stage duration if we have previous data
                    if previous_record:
                        duration = self.calculate_stage_duration(opportunity_id, timestamp)
                        context.stage_duration_days = duration

                # Create the event
                event = MicroDecisionEvent(
                    event_type=event_type,
                    timestamp_occurred=timestamp,
                    timestamp_recorded=timestamp,
                    actor_id=actor_id,
                    actor_type=ActorType.USER if actor_id else ActorType.SYSTEM,
                    record_type="Opportunity",
                    record_id=opportunity_id,
                    field_name=field_name,
                    old_value=previous_value,
                    new_value=current_value,
                    context=context,
                    extractor_version=self.version,
                )

                yield event

    def extract_stage_duration_summary(self) -> Dict[str, Dict]:
        """
        Extract stage duration analytics for all opportunities.

        This performs a more sophisticated analysis to calculate:
        - Average time in each stage
        - Conversion rates between stages
        - Stage skip patterns

        Returns:
            Dictionary with stage duration analytics
        """
        logger.info("Extracting stage duration summary...")

        # Query all OpportunityHistory records grouped by Opportunity
        query = """
            SELECT OpportunityId, StageName, CreatedDate
            FROM OpportunityHistory
            ORDER BY OpportunityId, CreatedDate ASC
        """

        stage_durations = {}
        current_opp_id = None
        last_stage_timestamp = None
        last_stage_name = None

        for record in self.execute_query(query):
            opp_id = record["OpportunityId"]
            stage = record["StageName"]
            timestamp = datetime.fromisoformat(record["CreatedDate"].replace("Z", "+00:00"))

            if opp_id != current_opp_id:
                # New opportunity
                current_opp_id = opp_id
                last_stage_name = stage
                last_stage_timestamp = timestamp
                continue

            # Calculate duration in previous stage
            if last_stage_name and last_stage_timestamp:
                duration = (timestamp - last_stage_timestamp).total_seconds() / 86400  # days

                if last_stage_name not in stage_durations:
                    stage_durations[last_stage_name] = []

                stage_durations[last_stage_name].append(duration)

            last_stage_name = stage
            last_stage_timestamp = timestamp

        # Calculate summary statistics
        summary = {}
        for stage, durations in stage_durations.items():
            summary[stage] = {
                "count": len(durations),
                "avg_days": sum(durations) / len(durations) if durations else 0,
                "min_days": min(durations) if durations else 0,
                "max_days": max(durations) if durations else 0,
            }

        logger.info(f"Stage duration analysis complete for {len(summary)} stages")
        return summary
