#!/usr/bin/env python3
"""
Call Intelligence Module

Provides a unified interface for analyzing call transcripts and conversations
to extract both metadata gaps AND advisory signals. Combines the existing
SlackMetadataAnalyzer with the new AdvisorySignalClassifier to produce a
comprehensive call intelligence report.

Usage:
    from call_intelligence import CallIntelligenceAnalyzer

    # From a transcript file
    analyzer = CallIntelligenceAnalyzer.from_transcript("call_notes.txt")
    report = analyzer.analyze()

    # From Slack-format messages
    analyzer = CallIntelligenceAnalyzer(messages_data)
    report = analyzer.analyze()
"""

import json
import os
from datetime import datetime
from typing import Optional

from advisory_signals import (
    AdvisorySignalClassifier,
    format_advisory_report,
)
from slack_metadata_analyzer import SlackMetadataAnalyzer, format_markdown_report
from transcript_parser import parse_transcript, parse_transcript_file


class CallIntelligenceAnalyzer:
    """
    Unified call intelligence analyzer that produces both metadata gap analysis
    and advisory signal classification from a single input source.
    """

    def __init__(self, messages_data: dict):
        self.messages_data = messages_data
        self.channel_name = messages_data.get("channel_name", "Unknown")

    @classmethod
    def from_transcript(cls, file_path: str, title: Optional[str] = None):
        """Create analyzer from a call transcript file."""
        messages_data = parse_transcript_file(file_path, title)
        return cls(messages_data)

    @classmethod
    def from_transcript_text(cls, text: str, title: str = "Call Transcript"):
        """Create analyzer from raw transcript text."""
        messages_data = parse_transcript(text, title)
        return cls(messages_data)

    @classmethod
    def from_json_file(cls, file_path: str):
        """Create analyzer from a JSON messages file."""
        with open(file_path, "r") as f:
            messages_data = json.load(f)
        return cls(messages_data)

    def analyze(self) -> dict:
        """
        Run both metadata gap analysis and advisory signal classification.

        Returns a combined report with:
        - metadata_analysis: Standard metadata gap analysis results
        - advisory_signals: Advisory type classification and signals
        - combined_insights: Cross-cutting insights from both analyses
        """
        # Run metadata gap analysis
        metadata_analyzer = SlackMetadataAnalyzer(self.messages_data)
        metadata_results = metadata_analyzer.analyze()

        # Run advisory signal classification
        advisory_analyzer = AdvisorySignalClassifier(self.messages_data)
        advisory_results = advisory_analyzer.analyze()

        # Generate cross-cutting insights
        combined_insights = self._generate_combined_insights(
            metadata_results, advisory_results
        )

        return {
            "source": self.channel_name,
            "analysis_timestamp": datetime.now().isoformat(),
            "metadata_analysis": metadata_results,
            "advisory_signals": advisory_results,
            "combined_insights": combined_insights,
        }

    def analyze_advisory_only(self) -> dict:
        """Run only the advisory signal classification."""
        analyzer = AdvisorySignalClassifier(self.messages_data)
        return analyzer.analyze()

    def _generate_combined_insights(
        self, metadata_results: dict, advisory_results: dict
    ) -> dict:
        """Generate insights that combine metadata gap and advisory signal data."""
        insights = {
            "key_findings": [],
            "catalog_action_plan": [],
        }

        # Find assets that appear in both high-priority metadata gaps AND high-urgency advisory
        priority_assets = {
            a.get("asset", "").split(" (")[0].lower()
            for a in metadata_results.get("priority_assets", [])
        }
        advisory_assets = set()
        for signal in advisory_results.get("signals", []):
            advisory_assets.update(signal.get("assets_referenced", []))

        overlap = priority_assets & advisory_assets
        if overlap:
            insights["key_findings"].append({
                "finding": "High-demand assets with active advisory load",
                "description": (
                    f"{len(overlap)} assets appear in both priority metadata gaps and "
                    f"active advisory conversations. These are prime candidates for "
                    f"immediate catalog enrichment."
                ),
                "assets": sorted(overlap)[:10],
            })

        # Identify advisory types that map to metadata gap categories
        advisory_dist = advisory_results.get("advisory_type_distribution", {})
        metadata_gaps = metadata_results.get("metadata_gaps", [])

        gap_types = {g.get("type", "") for g in metadata_gaps}

        mapping = {
            "Missing Descriptions": "Definition Clarification",
            "Missing Ownership": "Ownership Routing",
            "Undocumented Values": "Definition Clarification",
            "Versioning Confusion": "Data Guidance",
        }

        for gap_type, advisory_type in mapping.items():
            if gap_type in gap_types and advisory_type in advisory_dist:
                count = advisory_dist[advisory_type]["count"]
                insights["key_findings"].append({
                    "finding": f"Metadata gap '{gap_type}' confirmed by advisory signals",
                    "description": (
                        f"The metadata gap '{gap_type}' is backed by "
                        f"{count} '{advisory_type}' advisory signals, confirming "
                        f"this is actively impacting users."
                    ),
                })

        # Repeat questions signal the highest-value enrichment opportunities
        repeat_rate = advisory_results.get("summary", {}).get("repeat_question_rate", 0)
        if repeat_rate > 20:
            insights["key_findings"].append({
                "finding": "High repeat question rate indicates documentation debt",
                "description": (
                    f"{repeat_rate}% of questions are repeats. This means the same "
                    f"advisory is being given multiple times - a strong signal that "
                    f"this knowledge should be captured in the data catalog."
                ),
            })

        # Build a prioritized action plan from enrichment opportunities
        for opp in advisory_results.get("enrichment_opportunities", []):
            if opp["priority"] in ("High", "Medium"):
                insights["catalog_action_plan"].append({
                    "priority": opp["priority"],
                    "area": opp["advisory_type"],
                    "actions": opp["recommended_actions"],
                    "affected_assets": opp["affected_assets"],
                })

        return insights


def format_combined_report(analysis: dict) -> str:
    """Format the full combined intelligence report as markdown."""
    lines = []

    lines.append("# Call Intelligence Report")
    lines.append("")
    lines.append(f"**Source:** {analysis['source']}")
    lines.append(f"**Generated:** {analysis['analysis_timestamp']}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Combined Insights (the high-value summary)
    insights = analysis.get("combined_insights", {})

    if insights.get("key_findings"):
        lines.append("## Key Findings")
        lines.append("")
        for finding in insights["key_findings"]:
            lines.append(f"### {finding['finding']}")
            lines.append(finding["description"])
            if "assets" in finding:
                lines.append(f"**Assets:** {', '.join(f'`{a}`' for a in finding['assets'])}")
            lines.append("")

    if insights.get("catalog_action_plan"):
        lines.append("## Catalog Action Plan")
        lines.append("")
        for item in insights["catalog_action_plan"]:
            lines.append(f"### [{item['priority']}] {item['area']}")
            if item["affected_assets"]:
                lines.append(f"**Assets:** {', '.join(f'`{a}`' for a in item['affected_assets'])}")
            for action in item["actions"]:
                lines.append(f"- {action}")
            lines.append("")

    lines.append("---")
    lines.append("")

    # Advisory Signals section
    lines.append(format_advisory_report(analysis["advisory_signals"]))

    lines.append("")
    lines.append("---")
    lines.append("")

    # Metadata Gap section
    lines.append(format_markdown_report(analysis["metadata_analysis"]))

    return "\n".join(lines)
