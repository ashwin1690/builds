"""
Enriched Advisory Signal Data Models

Extended data models that wrap base AdvisorySignal with cross-connector enrichment:
- Confidence scoring from pattern match strength
- Atlan catalog validation (description, ownership, glossary)
- Salesforce business impact scoring (opportunity value, activity count)
- Gong call intelligence (deal context, talk ratios, sentiment)
- Tableau lineage context (dashboards, calculated fields)
- Multi-source signal aggregation and trend tracking
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from advisory_signals import AdvisorySignal, AdvisoryType


class SignalSource(Enum):
    """Sources that can generate advisory signals."""
    SLACK = "slack"
    TRANSCRIPT = "transcript"
    GONG = "gong"
    SALESFORCE = "salesforce"


@dataclass
class SignalConfidence:
    """Confidence scoring for advisory signal classification."""
    score: float = 0.0  # 0.0 - 1.0
    keyword_hits: int = 0
    pattern_hits: int = 0
    source_count: int = 1  # how many sources corroborate this signal

    @property
    def label(self) -> str:
        if self.score >= 0.8:
            return "High"
        elif self.score >= 0.5:
            return "Medium"
        return "Low"


@dataclass
class CatalogValidation:
    """Result of validating an advisory signal against the Atlan catalog."""
    asset_exists_in_atlan: bool = False
    has_description: bool = False
    has_owner: bool = False
    has_glossary_terms: bool = False
    enrichment_already_applied: bool = False
    asset_guid: Optional[str] = None
    asset_qualified_name: Optional[str] = None
    certificate_status: Optional[str] = None

    @property
    def completeness_score(self) -> float:
        """0.0-1.0 score of how complete the asset's metadata is."""
        if not self.asset_exists_in_atlan:
            return 0.0
        checks = [self.has_description, self.has_owner, self.has_glossary_terms]
        return sum(1.0 for c in checks if c) / len(checks)


@dataclass
class BusinessImpactScore:
    """Salesforce-derived business impact for an advisory signal."""
    score: float = 0.0  # 0-100
    opportunity_count: int = 0
    total_opportunity_value: float = 0.0
    active_opportunity_stages: list = field(default_factory=list)
    related_account_names: list = field(default_factory=list)
    activity_count: int = 0

    @property
    def label(self) -> str:
        if self.score >= 70:
            return "Critical"
        elif self.score >= 40:
            return "High"
        elif self.score >= 15:
            return "Medium"
        return "Low"


@dataclass
class GongCallContext:
    """Context extracted from Gong call recordings."""
    call_ids: list = field(default_factory=list)
    call_count: int = 0
    total_duration_minutes: float = 0.0
    avg_talk_ratio: float = 0.0  # % of call where rep talked about this topic
    mention_count: int = 0  # how many times asset/topic was mentioned across calls
    sentiment_scores: list = field(default_factory=list)  # per-mention sentiment
    deal_names: list = field(default_factory=list)
    deal_stages: list = field(default_factory=list)
    deal_values: list = field(default_factory=list)
    key_phrases: list = field(default_factory=list)  # surrounding context phrases
    speakers: list = field(default_factory=list)  # who mentioned it

    @property
    def avg_sentiment(self) -> float:
        if not self.sentiment_scores:
            return 0.0
        return sum(self.sentiment_scores) / len(self.sentiment_scores)

    @property
    def is_frequently_discussed(self) -> bool:
        return self.mention_count >= 3 or self.call_count >= 2


@dataclass
class TableauContext:
    """Tableau lineage context for an advisory signal."""
    dashboard_names: list = field(default_factory=list)
    calculated_fields_that_answer: list = field(default_factory=list)
    data_source_names: list = field(default_factory=list)
    filter_configs: list = field(default_factory=list)
    has_existing_definition: bool = False  # True if Tableau already defines this metric

    @property
    def is_tableau_documented(self) -> bool:
        return bool(self.calculated_fields_that_answer) or self.has_existing_definition


# Weights for composite priority calculation
PRIORITY_WEIGHTS = {
    "confidence": 0.20,
    "business_impact": 0.25,
    "catalog_gap": 0.20,
    "gong_signal": 0.20,
    "multi_source": 0.15,
}


@dataclass
class EnrichedAdvisorySignal:
    """An advisory signal enriched with cross-connector context."""
    base_signal: AdvisorySignal
    confidence: SignalConfidence = field(default_factory=SignalConfidence)
    catalog_validation: Optional[CatalogValidation] = None
    business_impact: Optional[BusinessImpactScore] = None
    gong_context: Optional[GongCallContext] = None
    tableau_context: Optional[TableauContext] = None
    composite_priority: float = 0.0
    sources: list = field(default_factory=list)
    enriched_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def compute_composite_priority(self) -> float:
        """Compute weighted composite priority from all enrichment dimensions."""
        score = 0.0

        # 1. Confidence score (0-1)
        score += self.confidence.score * PRIORITY_WEIGHTS["confidence"]

        # 2. Business impact (0-1, normalized from 0-100)
        if self.business_impact:
            score += (self.business_impact.score / 100.0) * PRIORITY_WEIGHTS["business_impact"]

        # 3. Catalog gap severity (higher = more gaps = higher priority)
        if self.catalog_validation:
            gap_score = 1.0 - self.catalog_validation.completeness_score
            score += gap_score * PRIORITY_WEIGHTS["catalog_gap"]
        else:
            # No catalog validation means we assume the asset is undocumented
            score += 1.0 * PRIORITY_WEIGHTS["catalog_gap"]

        # 4. Gong signal strength
        if self.gong_context:
            gong_score = min(self.gong_context.mention_count / 10.0, 1.0)
            if self.gong_context.avg_sentiment < -0.3:
                gong_score = min(gong_score + 0.2, 1.0)  # negative sentiment boosts priority
            score += gong_score * PRIORITY_WEIGHTS["gong_signal"]

        # 5. Multi-source corroboration
        source_score = min(len(self.sources) / 3.0, 1.0)
        score += source_score * PRIORITY_WEIGHTS["multi_source"]

        # Bonus: repeat questions
        if self.base_signal.is_repeat_question:
            score = min(score * 1.25, 1.0)

        # Bonus: high urgency
        if self.base_signal.urgency.value == "High":
            score = min(score * 1.15, 1.0)

        self.composite_priority = round(score, 4)
        return self.composite_priority

    def to_dict(self) -> dict:
        """Serialize to dictionary for reports."""
        result = {
            "thread_id": self.base_signal.thread_id,
            "advisory_type": self.base_signal.advisory_type.value,
            "urgency": self.base_signal.urgency.value,
            "question": self.base_signal.question,
            "questioner": self.base_signal.questioner,
            "assets_referenced": self.base_signal.assets_referenced,
            "is_repeat": self.base_signal.is_repeat_question,
            "confidence": {
                "score": self.confidence.score,
                "label": self.confidence.label,
                "keyword_hits": self.confidence.keyword_hits,
                "pattern_hits": self.confidence.pattern_hits,
                "source_count": self.confidence.source_count,
            },
            "composite_priority": self.composite_priority,
            "sources": self.sources,
            "enriched_at": self.enriched_at,
        }

        if self.catalog_validation:
            result["catalog_validation"] = {
                "asset_exists": self.catalog_validation.asset_exists_in_atlan,
                "has_description": self.catalog_validation.has_description,
                "has_owner": self.catalog_validation.has_owner,
                "has_glossary_terms": self.catalog_validation.has_glossary_terms,
                "completeness": self.catalog_validation.completeness_score,
                "already_enriched": self.catalog_validation.enrichment_already_applied,
            }

        if self.business_impact:
            result["business_impact"] = {
                "score": self.business_impact.score,
                "label": self.business_impact.label,
                "opportunity_count": self.business_impact.opportunity_count,
                "total_opportunity_value": self.business_impact.total_opportunity_value,
                "active_stages": self.business_impact.active_opportunity_stages,
                "accounts": self.business_impact.related_account_names,
                "activity_count": self.business_impact.activity_count,
            }

        if self.gong_context:
            result["gong_context"] = {
                "call_count": self.gong_context.call_count,
                "mention_count": self.gong_context.mention_count,
                "avg_sentiment": self.gong_context.avg_sentiment,
                "total_duration_minutes": self.gong_context.total_duration_minutes,
                "deal_names": self.gong_context.deal_names,
                "key_phrases": self.gong_context.key_phrases[:5],
                "frequently_discussed": self.gong_context.is_frequently_discussed,
            }

        if self.tableau_context:
            result["tableau_context"] = {
                "dashboards": self.tableau_context.dashboard_names,
                "answering_calc_fields": self.tableau_context.calculated_fields_that_answer,
                "data_sources": self.tableau_context.data_source_names,
                "has_existing_definition": self.tableau_context.has_existing_definition,
            }

        return result


@dataclass
class TrendSnapshot:
    """A point-in-time snapshot for trend tracking across analysis runs."""
    period_label: str  # "2026-W10", "2026-03"
    analyzed_at: str = field(default_factory=lambda: datetime.now().isoformat())
    signal_count: int = 0
    type_distribution: dict = field(default_factory=dict)
    repeat_rate: float = 0.0
    top_assets: list = field(default_factory=list)
    avg_confidence: float = 0.0
    avg_composite_priority: float = 0.0
    source_breakdown: dict = field(default_factory=dict)
    gong_call_count: int = 0
    business_impact_avg: float = 0.0

    def to_dict(self) -> dict:
        return {
            "period_label": self.period_label,
            "analyzed_at": self.analyzed_at,
            "signal_count": self.signal_count,
            "type_distribution": self.type_distribution,
            "repeat_rate": self.repeat_rate,
            "top_assets": self.top_assets,
            "avg_confidence": self.avg_confidence,
            "avg_composite_priority": self.avg_composite_priority,
            "source_breakdown": self.source_breakdown,
            "gong_call_count": self.gong_call_count,
            "business_impact_avg": self.business_impact_avg,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TrendSnapshot":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
