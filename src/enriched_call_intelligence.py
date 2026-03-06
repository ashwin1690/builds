"""
Enriched Call Intelligence Analyzer

Top-level orchestrator that combines all connectors to produce the most
comprehensive advisory signal analysis possible. Wraps the existing
CallIntelligenceAnalyzer with optional enrichment passes through:
- Gong call recordings (customer call context, deal associations)
- Atlan catalog validation (documentation completeness check)
- Salesforce business impact (pipeline value scoring)
- Tableau lineage (dashboard and metric definition context)

Gracefully degrades when connectors are unavailable - works standalone
with just conversation data from Slack or transcripts.
"""

import json
import logging
import os
from datetime import datetime
from typing import List, Optional

from advisory_signals import AdvisorySignalClassifier, format_advisory_report
from call_intelligence import CallIntelligenceAnalyzer, format_combined_report
from connector_base import SignalEnricher
from enriched_signals import (
    EnrichedAdvisorySignal,
    SignalConfidence,
    SignalSource,
    TrendSnapshot,
)
from signal_aggregator import SignalAggregator
from transcript_parser import parse_transcript, parse_transcript_file

logger = logging.getLogger(__name__)


class EnrichedCallIntelligenceAnalyzer:
    """
    Unified enriched call intelligence analyzer.

    Orchestrates the full pipeline:
    1. Parse input (Slack, transcript, or Gong)
    2. Run base advisory signal classification
    3. Convert to EnrichedAdvisorySignal with confidence scoring
    4. Run enricher chain (Atlan -> Salesforce -> Gong -> Tableau)
    5. Aggregate multi-source signals
    6. Compute composite priorities
    7. Track trends
    8. Generate enriched report
    """

    def __init__(
        self,
        messages_data: dict,
        atlan_config: Optional[dict] = None,
        salesforce_settings=None,
        tableau_workbook_paths: Optional[List[str]] = None,
        gong_config: Optional[dict] = None,
        trend_history_path: Optional[str] = None,
    ):
        self.messages_data = messages_data
        self.channel_name = messages_data.get("channel_name", "Unknown")
        self._enrichers: List[SignalEnricher] = []
        self._aggregator = SignalAggregator()
        self._trend_history_path = trend_history_path

        if trend_history_path:
            self._aggregator.load_history(trend_history_path)

        self._build_enricher_chain(
            atlan_config, salesforce_settings, tableau_workbook_paths, gong_config
        )

    def _build_enricher_chain(
        self,
        atlan_config,
        salesforce_settings,
        tableau_workbook_paths,
        gong_config,
    ):
        """Instantiate enrichers and add available ones to the chain."""
        # Gong (primary new connector)
        try:
            from gong_connector import GongCallEnricher
            gong_kwargs = gong_config or {}
            enricher = GongCallEnricher(**gong_kwargs)
            if enricher.is_available():
                self._enrichers.append(enricher)
                logger.info("Gong call intelligence enricher: ACTIVE")
            else:
                logger.info("Gong call intelligence enricher: not configured")
        except ImportError:
            logger.debug("Gong connector not available")

        # Atlan catalog validation
        try:
            from atlan_enricher import AtlanCatalogEnricher
            atlan_kwargs = atlan_config or {}
            enricher = AtlanCatalogEnricher(**atlan_kwargs)
            if enricher.is_available():
                self._enrichers.append(enricher)
                logger.info("Atlan catalog enricher: ACTIVE")
            else:
                logger.info("Atlan catalog enricher: not configured")
        except ImportError:
            logger.debug("Atlan enricher not available")

        # Salesforce business context
        try:
            from salesforce_enricher import SalesforceContextEnricher
            enricher = SalesforceContextEnricher(settings=salesforce_settings)
            if enricher.is_available():
                self._enrichers.append(enricher)
                logger.info("Salesforce context enricher: ACTIVE")
            else:
                logger.info("Salesforce context enricher: not configured")
        except ImportError:
            logger.debug("Salesforce enricher not available")

        # Tableau lineage
        try:
            from tableau_enricher import TableauLineageEnricher
            enricher = TableauLineageEnricher(workbook_paths=tableau_workbook_paths)
            if enricher.is_available():
                self._enrichers.append(enricher)
                logger.info("Tableau lineage enricher: ACTIVE")
            else:
                logger.info("Tableau lineage enricher: not configured")
        except ImportError:
            logger.debug("Tableau enricher not available")

    @classmethod
    def from_transcript(cls, file_path: str, title: Optional[str] = None, **kwargs):
        """Create analyzer from a call transcript file."""
        messages_data = parse_transcript_file(file_path, title)
        return cls(messages_data, **kwargs)

    @classmethod
    def from_transcript_text(cls, text: str, title: str = "Call Transcript", **kwargs):
        """Create analyzer from raw transcript text."""
        messages_data = parse_transcript(text, title)
        return cls(messages_data, **kwargs)

    @classmethod
    def from_json_file(cls, file_path: str, **kwargs):
        """Create analyzer from a JSON messages file."""
        with open(file_path, "r") as f:
            messages_data = json.load(f)
        return cls(messages_data, **kwargs)

    @classmethod
    def from_gong_call(cls, call_data: dict, transcript: list, **kwargs):
        """Create analyzer from Gong call data and transcript."""
        from gong_connector import parse_gong_transcript_to_messages
        messages_data = parse_gong_transcript_to_messages(call_data, transcript)
        return cls(messages_data, **kwargs)

    def analyze(self, include_base_analysis: bool = True) -> dict:
        """
        Run the full enriched analysis pipeline.

        Args:
            include_base_analysis: If True, also run the standard metadata gap analysis.

        Returns:
            Comprehensive enriched report dict.
        """
        # Step 1: Run base analysis
        if include_base_analysis:
            base_analyzer = CallIntelligenceAnalyzer(self.messages_data)
            base_results = base_analyzer.analyze()
        else:
            base_results = None

        # Step 2: Run advisory signal classification
        advisory_analyzer = AdvisorySignalClassifier(self.messages_data)
        advisory_results = advisory_analyzer.analyze()

        # Step 3: Convert to enriched signals with confidence scoring
        enriched_signals = self._convert_to_enriched(
            advisory_analyzer.signals, source=self._detect_source()
        )

        # Step 4: Run enricher chain
        for enricher in self._enrichers:
            enriched_signals = enricher.enrich_safe(enriched_signals)

        # Step 5: Compute composite priorities
        for signal in enriched_signals:
            signal.compute_composite_priority()

        # Step 6: Sort by priority
        enriched_signals.sort(key=lambda s: s.composite_priority, reverse=True)

        # Step 7: Compute trend snapshot
        period_label = datetime.now().strftime("%Y-W%U")
        trend_snapshot = self._aggregator.compute_trend_snapshot(
            enriched_signals, period_label
        )

        # Step 8: Detect emerging issues
        emerging_issues = self._aggregator.detect_emerging_issues()

        # Step 9: Save trend history
        if self._trend_history_path:
            self._aggregator.save_history(self._trend_history_path)

        # Step 10: Generate enriched report
        return self._generate_enriched_report(
            base_results=base_results,
            advisory_results=advisory_results,
            enriched_signals=enriched_signals,
            trend_snapshot=trend_snapshot,
            emerging_issues=emerging_issues,
        )

    def analyze_multi_source(
        self,
        additional_messages: Optional[List[dict]] = None,
        gong_messages: Optional[List[dict]] = None,
    ) -> dict:
        """
        Run analysis across multiple input sources and aggregate.

        Args:
            additional_messages: Extra messages_data dicts (e.g., from other Slack channels)
            gong_messages: Messages from Gong transcript parsing
        """
        all_signal_groups = []

        # Primary source
        primary_signals = self._classify_and_enrich(self.messages_data, self._detect_source())
        all_signal_groups.append(primary_signals)

        # Additional sources
        for msg_data in (additional_messages or []):
            signals = self._classify_and_enrich(msg_data, SignalSource.SLACK.value)
            all_signal_groups.append(signals)

        # Gong sources
        for msg_data in (gong_messages or []):
            signals = self._classify_and_enrich(msg_data, SignalSource.GONG.value)
            all_signal_groups.append(signals)

        # Aggregate across all sources
        merged = self._aggregator.aggregate(*all_signal_groups)

        # Run enricher chain on merged signals
        for enricher in self._enrichers:
            merged = enricher.enrich_safe(merged)

        # Recompute priorities after enrichment
        for signal in merged:
            signal.compute_composite_priority()
        merged.sort(key=lambda s: s.composite_priority, reverse=True)

        # Trend tracking
        period_label = datetime.now().strftime("%Y-W%U")
        trend_snapshot = self._aggregator.compute_trend_snapshot(merged, period_label)
        emerging_issues = self._aggregator.detect_emerging_issues()

        if self._trend_history_path:
            self._aggregator.save_history(self._trend_history_path)

        return self._generate_enriched_report(
            base_results=None,
            advisory_results=None,
            enriched_signals=merged,
            trend_snapshot=trend_snapshot,
            emerging_issues=emerging_issues,
        )

    def _classify_and_enrich(
        self, messages_data: dict, source: str
    ) -> List[EnrichedAdvisorySignal]:
        """Run classification and convert to enriched signals for a single source."""
        analyzer = AdvisorySignalClassifier(messages_data)
        analyzer.analyze()
        return self._convert_to_enriched(analyzer.signals, source=source)

    def _convert_to_enriched(
        self, signals: list, source: str
    ) -> List[EnrichedAdvisorySignal]:
        """Convert base AdvisorySignal list to EnrichedAdvisorySignal list."""
        enriched = []
        for signal in signals:
            confidence = SignalConfidence(
                score=signal.confidence,
                keyword_hits=signal.keyword_hits,
                pattern_hits=signal.pattern_hits,
                source_count=1,
            )
            enriched_signal = EnrichedAdvisorySignal(
                base_signal=signal,
                confidence=confidence,
                sources=[source],
            )
            enriched.append(enriched_signal)
        return enriched

    def _detect_source(self) -> str:
        """Detect the input source type from messages_data."""
        channel = self.messages_data.get("channel_name", "")
        if "gong" in channel.lower():
            return SignalSource.GONG.value
        elif any(
            m.get("thread_id", "").startswith("thread_")
            for m in self.messages_data.get("messages", [])
        ):
            return SignalSource.TRANSCRIPT.value
        return SignalSource.SLACK.value

    def _generate_enriched_report(
        self,
        base_results: Optional[dict],
        advisory_results: Optional[dict],
        enriched_signals: List[EnrichedAdvisorySignal],
        trend_snapshot: TrendSnapshot,
        emerging_issues: List[dict],
    ) -> dict:
        """Generate the comprehensive enriched intelligence report."""
        # Active enrichers
        active_enrichers = [e.__class__.__name__ for e in self._enrichers]

        # Enrichment coverage stats
        catalog_validated = sum(1 for s in enriched_signals if s.catalog_validation)
        business_scored = sum(1 for s in enriched_signals if s.business_impact)
        gong_enriched = sum(1 for s in enriched_signals if s.gong_context)
        tableau_enriched = sum(1 for s in enriched_signals if s.tableau_context)

        # Top signals by composite priority
        top_signals = [s.to_dict() for s in enriched_signals[:20]]

        # Priority breakdown by advisory type
        type_priority = {}
        for signal in enriched_signals:
            t = signal.base_signal.advisory_type.value
            if t not in type_priority:
                type_priority[t] = {"count": 0, "avg_priority": 0, "max_priority": 0}
            type_priority[t]["count"] += 1
            type_priority[t]["avg_priority"] += signal.composite_priority
            type_priority[t]["max_priority"] = max(
                type_priority[t]["max_priority"], signal.composite_priority
            )
        for t in type_priority:
            count = type_priority[t]["count"]
            type_priority[t]["avg_priority"] = round(
                type_priority[t]["avg_priority"] / count, 4
            )

        # Asset hotspots (assets with highest combined priority)
        asset_scores = {}
        for signal in enriched_signals:
            for asset in signal.base_signal.assets_referenced:
                if asset not in asset_scores:
                    asset_scores[asset] = {
                        "total_priority": 0,
                        "signal_count": 0,
                        "types": set(),
                        "has_catalog": False,
                        "has_gong": False,
                        "business_value": 0,
                    }
                asset_scores[asset]["total_priority"] += signal.composite_priority
                asset_scores[asset]["signal_count"] += 1
                asset_scores[asset]["types"].add(signal.base_signal.advisory_type.value)
                if signal.catalog_validation:
                    asset_scores[asset]["has_catalog"] = True
                if signal.gong_context:
                    asset_scores[asset]["has_gong"] = True
                if signal.business_impact:
                    asset_scores[asset]["business_value"] = max(
                        asset_scores[asset]["business_value"],
                        signal.business_impact.total_opportunity_value,
                    )

        asset_hotspots = sorted(
            [
                {
                    "asset": name,
                    "total_priority": round(data["total_priority"], 3),
                    "signal_count": data["signal_count"],
                    "advisory_types": sorted(data["types"]),
                    "in_atlan": data["has_catalog"],
                    "in_gong_calls": data["has_gong"],
                    "pipeline_value": data["business_value"],
                }
                for name, data in asset_scores.items()
            ],
            key=lambda x: x["total_priority"],
            reverse=True,
        )[:15]

        # Gong intelligence summary
        gong_summary = None
        gong_signals = [s for s in enriched_signals if s.gong_context]
        if gong_signals:
            all_deals = set()
            all_call_ids = set()
            total_mentions = 0
            for s in gong_signals:
                all_deals.update(s.gong_context.deal_names)
                all_call_ids.update(s.gong_context.call_ids)
                total_mentions += s.gong_context.mention_count

            gong_summary = {
                "calls_analyzed": len(all_call_ids),
                "total_asset_mentions": total_mentions,
                "deals_referenced": sorted(all_deals),
                "signals_with_gong_context": len(gong_signals),
                "frequently_discussed_assets": [
                    s.base_signal.assets_referenced
                    for s in gong_signals
                    if s.gong_context.is_frequently_discussed
                ],
            }

        report = {
            "source": self.channel_name,
            "analysis_timestamp": datetime.now().isoformat(),
            "analysis_mode": "enriched",
            "active_enrichers": active_enrichers,
            "summary": {
                "total_signals": len(enriched_signals),
                "signals_with_catalog_validation": catalog_validated,
                "signals_with_business_impact": business_scored,
                "signals_with_gong_context": gong_enriched,
                "signals_with_tableau_context": tableau_enriched,
                "avg_composite_priority": round(
                    sum(s.composite_priority for s in enriched_signals) / max(len(enriched_signals), 1),
                    4,
                ),
                "avg_confidence": round(
                    sum(s.confidence.score for s in enriched_signals) / max(len(enriched_signals), 1),
                    3,
                ),
            },
            "top_signals": top_signals,
            "type_priority_breakdown": type_priority,
            "asset_hotspots": asset_hotspots,
            "trend_snapshot": trend_snapshot.to_dict(),
            "emerging_issues": emerging_issues,
        }

        if gong_summary:
            report["gong_intelligence"] = gong_summary

        if base_results:
            report["metadata_analysis"] = base_results.get("metadata_analysis")
            report["combined_insights"] = base_results.get("combined_insights")

        if advisory_results:
            report["advisory_signals_base"] = advisory_results

        return report


def format_enriched_report(analysis: dict) -> str:
    """Format the enriched intelligence report as markdown."""
    lines = []

    lines.append("# Enriched Call Intelligence Report")
    lines.append("")
    lines.append(f"**Source:** {analysis['source']}")
    lines.append(f"**Generated:** {analysis['analysis_timestamp']}")
    lines.append(f"**Mode:** {analysis['analysis_mode']}")
    lines.append(f"**Active Enrichers:** {', '.join(analysis.get('active_enrichers', ['None']))}")
    lines.append("")

    # Summary
    summary = analysis["summary"]
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **Total signals:** {summary['total_signals']}")
    lines.append(f"- **Avg composite priority:** {summary['avg_composite_priority']}")
    lines.append(f"- **Avg confidence:** {summary['avg_confidence']}")
    lines.append(f"- **Catalog validated:** {summary['signals_with_catalog_validation']}")
    lines.append(f"- **Business impact scored:** {summary['signals_with_business_impact']}")
    lines.append(f"- **Gong enriched:** {summary['signals_with_gong_context']}")
    lines.append(f"- **Tableau enriched:** {summary['signals_with_tableau_context']}")
    lines.append("")

    # Gong Intelligence
    gong_intel = analysis.get("gong_intelligence")
    if gong_intel:
        lines.append("## Gong Call Intelligence")
        lines.append("")
        lines.append(f"- **Calls analyzed:** {gong_intel['calls_analyzed']}")
        lines.append(f"- **Total asset mentions in calls:** {gong_intel['total_asset_mentions']}")
        lines.append(f"- **Deals referenced:** {', '.join(gong_intel['deals_referenced'][:5])}")
        lines.append(f"- **Signals with Gong context:** {gong_intel['signals_with_gong_context']}")
        if gong_intel['frequently_discussed_assets']:
            lines.append("- **Frequently discussed assets:**")
            for assets in gong_intel['frequently_discussed_assets'][:5]:
                lines.append(f"  - {', '.join(assets)}")
        lines.append("")

    # Asset Hotspots
    lines.append("## Asset Hotspots")
    lines.append("")
    lines.append("Assets with the highest combined advisory signal priority:")
    lines.append("")
    for hotspot in analysis.get("asset_hotspots", [])[:10]:
        indicators = []
        if hotspot["in_atlan"]:
            indicators.append("Atlan")
        if hotspot["in_gong_calls"]:
            indicators.append("Gong")
        if hotspot["pipeline_value"] > 0:
            indicators.append(f"${hotspot['pipeline_value']:,.0f} pipeline")

        indicator_str = f" [{', '.join(indicators)}]" if indicators else ""
        lines.append(
            f"- **`{hotspot['asset']}`** - Priority: {hotspot['total_priority']:.3f} | "
            f"Signals: {hotspot['signal_count']} | "
            f"Types: {', '.join(hotspot['advisory_types'])}"
            f"{indicator_str}"
        )
    lines.append("")

    # Type Priority Breakdown
    lines.append("## Advisory Type Priority Breakdown")
    lines.append("")
    for type_name, data in sorted(
        analysis.get("type_priority_breakdown", {}).items(),
        key=lambda x: x[1]["avg_priority"],
        reverse=True,
    ):
        lines.append(
            f"- **{type_name}**: {data['count']} signals | "
            f"Avg priority: {data['avg_priority']:.4f} | "
            f"Max: {data['max_priority']:.4f}"
        )
    lines.append("")

    # Top Signals
    lines.append("## Top Priority Signals")
    lines.append("")
    for i, signal in enumerate(analysis.get("top_signals", [])[:10], 1):
        confidence = signal.get("confidence", {})
        lines.append(f"### {i}. [{signal['advisory_type']}] Priority: {signal['composite_priority']:.4f}")
        lines.append(f"**Confidence:** {confidence.get('label', 'N/A')} ({confidence.get('score', 0):.2f})")
        lines.append(f"**Question:** {signal['question'][:200]}")

        if signal.get("assets_referenced"):
            lines.append(f"**Assets:** {', '.join(f'`{a}`' for a in signal['assets_referenced'])}")

        if signal.get("gong_context"):
            gc = signal["gong_context"]
            lines.append(
                f"**Gong:** {gc['call_count']} calls, {gc['mention_count']} mentions, "
                f"sentiment: {gc['avg_sentiment']:.2f}"
            )

        if signal.get("catalog_validation"):
            cv = signal["catalog_validation"]
            status = "Documented" if cv["already_enriched"] else "Gaps found"
            lines.append(f"**Catalog:** {status} (completeness: {cv['completeness']:.0%})")

        if signal.get("business_impact"):
            bi = signal["business_impact"]
            lines.append(
                f"**Business Impact:** {bi['label']} (score: {bi['score']:.1f}, "
                f"pipeline: ${bi['total_opportunity_value']:,.0f})"
            )

        lines.append("")

    # Emerging Issues
    emerging = analysis.get("emerging_issues", [])
    if emerging:
        lines.append("## Emerging Issues")
        lines.append("")
        for issue in emerging:
            severity_icon = {"High": "!!!", "Medium": "!!", "Low": "!"}.get(
                issue["severity"], ""
            )
            lines.append(f"- **[{issue['severity']}]** {issue['description']} {severity_icon}")
        lines.append("")

    # Trend Snapshot
    trend = analysis.get("trend_snapshot", {})
    if trend:
        lines.append("## Trend Snapshot")
        lines.append("")
        lines.append(f"- **Period:** {trend.get('period_label', 'N/A')}")
        lines.append(f"- **Signal count:** {trend.get('signal_count', 0)}")
        lines.append(f"- **Repeat rate:** {trend.get('repeat_rate', 0)}%")
        lines.append(f"- **Avg confidence:** {trend.get('avg_confidence', 0):.3f}")
        lines.append(f"- **Gong calls:** {trend.get('gong_call_count', 0)}")
        lines.append(f"- **Avg business impact:** {trend.get('business_impact_avg', 0):.1f}")
        if trend.get("source_breakdown"):
            lines.append(f"- **Sources:** {trend['source_breakdown']}")
        lines.append("")

    return "\n".join(lines)
