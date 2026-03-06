"""
Salesforce Context Enricher

Correlates advisory signals with Salesforce opportunity and activity data
to score signals by business impact. When an advisory signal references an
asset tied to a high-value deal pipeline, it gets higher priority.

Uses the existing BaseExtractor patterns for Salesforce connectivity.

Environment Variables:
    SALESFORCE_USERNAME, SALESFORCE_PASSWORD, SALESFORCE_SECURITY_TOKEN,
    SALESFORCE_DOMAIN (via salesforce_temporal.config.settings)
"""

import logging
import os
from collections import defaultdict
from typing import Dict, List, Optional

from connector_base import SignalEnricher
from enriched_signals import BusinessImpactScore, EnrichedAdvisorySignal

logger = logging.getLogger(__name__)

# Try to import Salesforce dependencies
try:
    from simple_salesforce import Salesforce
    from salesforce_temporal.config.settings import Settings, get_settings

    HAS_SALESFORCE = True
except ImportError:
    HAS_SALESFORCE = False
    logger.debug("Salesforce dependencies not available - SF enrichment will be unavailable")


# Stage weights for business impact scoring
STAGE_WEIGHTS = {
    "closed won": 1.0,
    "negotiation": 0.85,
    "proposal": 0.7,
    "qualification": 0.5,
    "prospecting": 0.3,
    "closed lost": 0.1,
}


class SalesforceContextEnricher(SignalEnricher):
    """
    Enriches advisory signals with Salesforce business context.

    For each asset referenced in advisory signals, searches Salesforce for:
    - Tasks/Events whose Subject mentions the asset name
    - Related Opportunities via Task.WhatId -> Opportunity
    - Account names and pipeline values

    Produces a BusinessImpactScore per signal based on total pipeline value
    weighted by deal stage.
    """

    def __init__(self, settings: Optional["Settings"] = None, lookback_days: int = 180):
        self._settings = settings
        self._sf = None
        self.lookback_days = lookback_days

    def is_available(self) -> bool:
        if not HAS_SALESFORCE:
            return False
        return bool(os.environ.get("SALESFORCE_USERNAME"))

    def _get_connection(self) -> "Salesforce":
        if self._sf is None:
            settings = self._settings or get_settings()
            auth = settings.get_salesforce_auth_config()
            self._sf = Salesforce(
                username=auth["username"],
                password=auth.get("password"),
                security_token=auth.get("security_token", ""),
                domain=auth["domain"],
                version=settings.salesforce_api_version,
            )
        return self._sf

    def enrich(self, signals: List[EnrichedAdvisorySignal]) -> List[EnrichedAdvisorySignal]:
        """Enrich signals with Salesforce business impact data."""
        # Collect unique asset names
        all_assets = set()
        for signal in signals:
            all_assets.update(signal.base_signal.assets_referenced)

        if not all_assets:
            return signals

        # Query Salesforce for related activities and opportunities
        asset_impact = self._query_business_impact(list(all_assets))

        # Apply impact scores to signals
        for signal in signals:
            impact = self._aggregate_impact(
                signal.base_signal.assets_referenced, asset_impact
            )
            if impact.activity_count > 0 or impact.opportunity_count > 0:
                signal.business_impact = impact

        return signals

    def _query_business_impact(self, asset_names: List[str]) -> Dict[str, dict]:
        """
        Query Salesforce for activities and opportunities referencing asset names.

        Returns dict mapping asset_name -> {activities: [...], opportunities: [...]}
        """
        sf = self._get_connection()
        result: Dict[str, dict] = {name.lower(): {"activities": [], "opportunities": []} for name in asset_names}

        # Build LIKE clauses for Task/Event subject matching
        # Process in batches to avoid SOQL length limits
        for i in range(0, len(asset_names), 10):
            batch = asset_names[i:i + 10]
            like_clauses = " OR ".join(
                f"Subject LIKE '%{name.replace(chr(39), '')}%'" for name in batch
            )

            # Query Tasks
            task_soql = f"""
                SELECT Id, Subject, WhatId, Who.Name, Status, CreatedDate
                FROM Task
                WHERE ({like_clauses})
                AND CreatedDate = LAST_N_DAYS:{self.lookback_days}
                ORDER BY CreatedDate DESC
                LIMIT 200
            """

            try:
                task_results = sf.query_all(task_soql)
                for record in task_results.get("records", []):
                    subject = (record.get("Subject") or "").lower()
                    for name in batch:
                        if name.lower() in subject:
                            result[name.lower()]["activities"].append(record)
            except Exception as e:
                logger.warning(f"Salesforce Task query failed: {e}")

            # Query Events
            event_soql = f"""
                SELECT Id, Subject, WhatId, Who.Name, StartDateTime
                FROM Event
                WHERE ({like_clauses})
                AND CreatedDate = LAST_N_DAYS:{self.lookback_days}
                ORDER BY StartDateTime DESC
                LIMIT 200
            """

            try:
                event_results = sf.query_all(event_soql)
                for record in event_results.get("records", []):
                    subject = (record.get("Subject") or "").lower()
                    for name in batch:
                        if name.lower() in subject:
                            result[name.lower()]["activities"].append(record)
            except Exception as e:
                logger.warning(f"Salesforce Event query failed: {e}")

        # Now resolve WhatId -> Opportunity for activities linked to opportunities
        opp_ids = set()
        for asset_data in result.values():
            for activity in asset_data["activities"]:
                what_id = activity.get("WhatId", "")
                if what_id and what_id.startswith("006"):  # Opportunity prefix
                    opp_ids.add(what_id)

        if opp_ids:
            opp_id_list = "','".join(opp_ids)
            opp_soql = f"""
                SELECT Id, Name, Amount, StageName, AccountId, Account.Name, CloseDate
                FROM Opportunity
                WHERE Id IN ('{opp_id_list}')
            """

            try:
                opp_results = sf.query_all(opp_soql)
                opp_map = {}
                for record in opp_results.get("records", []):
                    opp_map[record["Id"]] = record

                # Link opportunities back to assets
                for asset_data in result.values():
                    for activity in asset_data["activities"]:
                        what_id = activity.get("WhatId", "")
                        if what_id in opp_map:
                            opp = opp_map[what_id]
                            if opp not in asset_data["opportunities"]:
                                asset_data["opportunities"].append(opp)
            except Exception as e:
                logger.warning(f"Salesforce Opportunity query failed: {e}")

        return result

    def _aggregate_impact(
        self, asset_names: List[str], asset_impact: Dict[str, dict]
    ) -> BusinessImpactScore:
        """Aggregate business impact across multiple assets for a single signal."""
        impact = BusinessImpactScore()
        seen_opp_ids = set()

        for name in asset_names:
            data = asset_impact.get(name.lower(), {})
            impact.activity_count += len(data.get("activities", []))

            for opp in data.get("opportunities", []):
                opp_id = opp.get("Id")
                if opp_id and opp_id not in seen_opp_ids:
                    seen_opp_ids.add(opp_id)
                    impact.opportunity_count += 1

                    amount = opp.get("Amount") or 0
                    impact.total_opportunity_value += float(amount)

                    stage = opp.get("StageName", "")
                    if stage and stage not in impact.active_opportunity_stages:
                        impact.active_opportunity_stages.append(stage)

                    account_name = ""
                    account = opp.get("Account")
                    if isinstance(account, dict):
                        account_name = account.get("Name", "")
                    if account_name and account_name not in impact.related_account_names:
                        impact.related_account_names.append(account_name)

        # Calculate weighted business impact score (0-100)
        if impact.opportunity_count > 0:
            weighted_value = 0.0
            for opp in [
                d for data in asset_impact.values()
                for d in data.get("opportunities", [])
            ]:
                amount = float(opp.get("Amount") or 0)
                stage = (opp.get("StageName") or "").lower()
                weight = STAGE_WEIGHTS.get(stage, 0.4)
                weighted_value += amount * weight

            # Normalize to 0-100 scale
            # $1M+ pipeline = 100, scale logarithmically
            import math
            if weighted_value > 0:
                impact.score = min(
                    round(math.log10(weighted_value + 1) / math.log10(1_000_001) * 100, 1),
                    100.0,
                )
            else:
                impact.score = min(impact.activity_count * 5, 30)
        elif impact.activity_count > 0:
            impact.score = min(impact.activity_count * 5, 30)

        return impact
