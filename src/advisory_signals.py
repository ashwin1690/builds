#!/usr/bin/env python3
"""
Advisory Signals Classifier for Call Intelligence

Analyzes call transcripts and conversation threads to classify the type of
advisory being given to customers. Generates signals about advisory patterns
that help teams understand what guidance they're most frequently providing.

Advisory Types:
    - Data Guidance: Directing users to the right tables/assets
    - Definition Clarification: Explaining what data means or contains
    - Best Practice: Recommending how to use data properly
    - Troubleshooting: Helping resolve data quality or access issues
    - Governance & Compliance: Advising on PII, access controls, audit needs
    - Architecture & Lineage: Explaining data flow, sources, dependencies
    - Metric Interpretation: Clarifying KPIs, scores, thresholds, calculations
    - Ownership Routing: Directing users to the right team or person
"""

import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class AdvisoryType(Enum):
    DATA_GUIDANCE = "Data Guidance"
    DEFINITION_CLARIFICATION = "Definition Clarification"
    BEST_PRACTICE = "Best Practice"
    TROUBLESHOOTING = "Troubleshooting"
    GOVERNANCE_COMPLIANCE = "Governance & Compliance"
    ARCHITECTURE_LINEAGE = "Architecture & Lineage"
    METRIC_INTERPRETATION = "Metric Interpretation"
    OWNERSHIP_ROUTING = "Ownership Routing"
    UNKNOWN = "Unknown"


class AdvisoryUrgency(Enum):
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"


@dataclass
class AdvisorySignal:
    """A single advisory signal extracted from a conversation thread."""
    thread_id: str
    advisory_type: AdvisoryType
    urgency: AdvisoryUrgency
    question: str
    questioner: str
    questioner_role: str
    advisors: list = field(default_factory=list)
    advisor_roles: list = field(default_factory=list)
    response_summary: str = ""
    assets_referenced: list = field(default_factory=list)
    is_repeat_question: bool = False
    actionable_insight: str = ""


@dataclass
class AdvisoryPattern:
    """An aggregated pattern across multiple advisory signals."""
    advisory_type: AdvisoryType
    signal_count: int = 0
    unique_questioners: int = 0
    repeat_rate: float = 0.0
    top_advisors: list = field(default_factory=list)
    common_assets: list = field(default_factory=list)
    urgency_distribution: dict = field(default_factory=dict)
    sample_questions: list = field(default_factory=list)


# Keywords and patterns for classifying advisory types from RESPONSES
ADVISORY_RESPONSE_PATTERNS = {
    AdvisoryType.DATA_GUIDANCE: {
        "keywords": [
            "use this table", "should use", "always use", "the right table",
            "instead use", "don't use", "deprecated", "source of truth",
            "production-ready", "curated", "ignore", "use v2",
            "never use for analysis", "use this one",
        ],
        "patterns": [
            r"(?:always|should|must)\s+use\s+\w+",
            r"(?:don\'t|do not|never)\s+use\s+\w+",
            r"use\s+\w+\s+instead",
            r"(?:deprecated|legacy|old)\s+(?:table|view|dataset)",
            r"source of truth",
        ],
    },
    AdvisoryType.DEFINITION_CLARIFICATION: {
        "keywords": [
            "contains", "means", "defined as", "definition",
            "values are", "represents", "is the", "includes",
            "one row per", "grain", "aggregated", "calculated",
            "daily active", "monthly recurring",
        ],
        "patterns": [
            r"\d+\s*=\s*\w+",  # enumeration values like 1=signup
            r"(?:values?|codes?)\s*(?:are|:)",
            r"(?:contains|includes|represents)\s+\w+",
            r"defined\s+as\s+",
            r"one\s+row\s+per\s+\w+",
        ],
    },
    AdvisoryType.BEST_PRACTICE: {
        "keywords": [
            "best practice", "recommend", "be careful", "make sure",
            "always", "filter on", "dedupe", "first",
            "for accurate", "standard", "proper way",
        ],
        "patterns": [
            r"be\s+careful\s+with",
            r"(?:always|make sure to)\s+\w+",
            r"filter\s+on\s+\w+",
            r"dedupe\s+on\s+\w+",
            r"for\s+accurate\s+\w+",
        ],
    },
    AdvisoryType.TROUBLESHOOTING: {
        "keywords": [
            "known issue", "bug", "fix", "working on",
            "gaps", "missing", "double count", "weird",
            "issue where", "problem", "backfilling",
            "should be fixed",
        ],
        "patterns": [
            r"known\s+issue",
            r"working\s+on\s+(?:a\s+)?fix",
            r"(?:gaps|missing)\s+(?:data|values|events)",
            r"double\s+count",
            r"we\'re\s+(?:working|fixing)",
        ],
    },
    AdvisoryType.GOVERNANCE_COMPLIANCE: {
        "keywords": [
            "pii", "restricted", "access request", "approval",
            "sensitive", "compliance", "audit", "anonymized",
            "hashed", "permission", "servicenow", "manager approval",
        ],
        "patterns": [
            r"(?:access|permission)\s+request",
            r"(?:manager|team)\s+approval",
            r"(?:pii|sensitive|restricted)\s+\w*\s*(?:data|table|column)?",
            r"(?:compliance|audit)\s+\w+",
            r"anonymized\s+\w+",
        ],
    },
    AdvisoryType.ARCHITECTURE_LINEAGE: {
        "keywords": [
            "pulls from", "synced from", "comes from", "source",
            "upstream", "downstream", "fivetran", "pipeline",
            "sync", "etl", "transform", "joins on",
            "refreshes", "latency", "updates every",
        ],
        "patterns": [
            r"(?:pulls?|synced?|comes?)\s+from\s+\w+",
            r"(?:fivetran|airbyte|stitch)\s+sync",
            r"(?:updates?|refreshes?)\s+(?:every|at)\s+\w+",
            r"\d+[\-\s]*(?:hour|minute)\s+latency",
            r"joins?\s+on\s+\w+",
        ],
    },
    AdvisoryType.METRIC_INTERPRETATION: {
        "keywords": [
            "threshold", "score", "kpi", "metric", "formula",
            "calculated field", "official", "investor reporting",
            "leadership", "approved", "auc", "model",
            "0-100", "low risk", "high risk",
        ],
        "patterns": [
            r"(?:score|threshold)\s+(?:is|of|:)\s*\d+",
            r"\d+\s*[-–]\s*\d+\s*=\s*\w+",  # ranges like 0-30 = low
            r"official\s+(?:kpi|metric|definition)",
            r"investor\s+reporting",
            r"(?:custom|calculated)\s+formula",
        ],
    },
    AdvisoryType.OWNERSHIP_ROUTING: {
        "keywords": [
            "team owns", "owned by", "contact", "responsible",
            "ping", "dm me", "file a ticket", "jira",
            "primary contact", "reach out",
        ],
        "patterns": [
            r"\w+\s+team\s+owns",
            r"@\w+\s+is\s+(?:the\s+)?(?:primary\s+)?contact",
            r"file\s+a\s+ticket",
            r"(?:ping|dm|reach out to)\s+\w+",
            r"owned\s+by\s+(?:the\s+)?\w+",
        ],
    },
}

# Patterns for detecting repeat/recurring questions
REPEAT_INDICATORS = [
    r"asked\s+(?:this\s+)?(?:same|before|last|again)",
    r"(?:this|that)\s+(?:gets?|is)\s+asked\s+(?:so\s+)?(?:often|frequently|weekly|daily|a\s+lot)",
    r"see\s+(?:the\s+)?thread\s+from",
    r"(?:we\s+)?really\s+need\s+(?:this|to)\s+(?:documented|in\s+the\s+catalog)",
    r"(?:so\s+)?often\s*!",
]

# Urgency signals
URGENCY_HIGH_PATTERNS = [
    r"(?:compliance|audit|investor|pii|sensitive|restricted)",
    r"(?:broken|breaking|wrong|critical)",
    r"(?:urgent|asap|immediately|blocking)",
]

URGENCY_MEDIUM_PATTERNS = [
    r"(?:known\s+issue|working\s+on|fix|gaps|missing)",
    r"(?:deprecated|legacy|old\s+system)",
    r"(?:asked\s+(?:so\s+)?often|weekly|repeatedly)",
]


class AdvisorySignalClassifier:
    """Classifies advisory types from conversation threads and generates signals."""

    def __init__(self, messages_data: dict):
        self.channel_name = messages_data.get("channel_name", "Unknown")
        self.date_range = messages_data.get("date_range", "Unknown")
        self.messages = messages_data.get("messages", [])
        self.signals: list[AdvisorySignal] = []
        self._question_fingerprints: dict[str, list] = defaultdict(list)

    def analyze(self) -> dict:
        """Run the full advisory signal analysis."""
        # Step 1: Classify each thread
        self._classify_threads()

        # Step 2: Detect repeat questions
        self._detect_repeats()

        # Step 3: Aggregate into patterns
        patterns = self._aggregate_patterns()

        # Step 4: Generate report
        return self._generate_report(patterns)

    def _classify_threads(self):
        """Classify each conversation thread by advisory type."""
        for msg in self.messages:
            question = msg["message"]
            questioner = msg.get("user", "Unknown")
            questioner_role = msg.get("user_role", "Unknown")
            replies = msg.get("replies", [])

            if not replies:
                continue

            # Combine all response text for classification
            response_texts = [r["message"] for r in replies]
            full_response = " ".join(response_texts)

            # Classify advisory type from the responses
            advisory_type = self._classify_advisory_type(full_response, question)

            # Determine urgency
            urgency = self._assess_urgency(question, full_response)

            # Extract advisors
            advisors = [r.get("user", "Unknown") for r in replies]
            advisor_roles = [r.get("user_role", "Unknown") for r in replies]

            # Extract referenced assets
            assets = self._extract_assets(question + " " + full_response)

            # Check for explicit repeat indicators in responses
            is_repeat = self._has_repeat_indicators(full_response)

            # Generate actionable insight
            insight = self._generate_insight(advisory_type, question, full_response)

            # Build a fingerprint for repeat detection
            fingerprint = self._build_question_fingerprint(question)
            self._question_fingerprints[fingerprint].append(msg.get("thread_id", ""))

            signal = AdvisorySignal(
                thread_id=msg.get("thread_id", ""),
                advisory_type=advisory_type,
                urgency=urgency,
                question=question,
                questioner=questioner,
                questioner_role=questioner_role,
                advisors=advisors,
                advisor_roles=advisor_roles,
                response_summary=full_response[:300],
                assets_referenced=assets,
                is_repeat_question=is_repeat,
                actionable_insight=insight,
            )
            self.signals.append(signal)

    def _classify_advisory_type(self, response: str, question: str) -> AdvisoryType:
        """Classify the type of advisory from the response content."""
        response_lower = response.lower()
        question_lower = question.lower()
        combined = response_lower + " " + question_lower

        scores: dict[AdvisoryType, float] = {}

        for advisory_type, config in ADVISORY_RESPONSE_PATTERNS.items():
            score = 0.0

            # Keyword matching (weighted by relevance)
            for keyword in config["keywords"]:
                if keyword in response_lower:
                    score += 1.5  # Responses weighted higher
                if keyword in question_lower:
                    score += 0.5

            # Regex pattern matching
            for pattern in config["patterns"]:
                if re.search(pattern, combined, re.IGNORECASE):
                    score += 2.0

            scores[advisory_type] = score

        best_match = max(scores, key=scores.get)
        if scores[best_match] > 0:
            return best_match
        return AdvisoryType.UNKNOWN

    def _assess_urgency(self, question: str, response: str) -> AdvisoryUrgency:
        """Assess the urgency level of the advisory signal."""
        combined = (question + " " + response).lower()

        for pattern in URGENCY_HIGH_PATTERNS:
            if re.search(pattern, combined, re.IGNORECASE):
                return AdvisoryUrgency.HIGH

        for pattern in URGENCY_MEDIUM_PATTERNS:
            if re.search(pattern, combined, re.IGNORECASE):
                return AdvisoryUrgency.MEDIUM

        return AdvisoryUrgency.LOW

    def _has_repeat_indicators(self, response: str) -> bool:
        """Check if the response explicitly indicates a repeat question."""
        for pattern in REPEAT_INDICATORS:
            if re.search(pattern, response, re.IGNORECASE):
                return True
        return False

    def _extract_assets(self, text: str) -> list:
        """Extract data asset references from text."""
        assets = set()

        # Backtick-wrapped names
        for match in re.findall(r"`([^`]+)`", text):
            if len(match) >= 3:
                assets.add(match.lower())

        # Qualified names (schema.table or schema.table.column)
        for match in re.findall(r"\b(\w+\.\w+(?:\.\w+)?)\b", text):
            normalized = match.lower()
            if normalized not in {"i.e", "e.g", "a.m", "p.m"}:
                assets.add(normalized)

        # Common table patterns
        for match in re.findall(
            r"\b((?:dim|fct|stg|raw)_\w+)\b", text, re.IGNORECASE
        ):
            assets.add(match.lower())

        for match in re.findall(
            r"\b(\w+_(?:daily|weekly|monthly)(?:_v\d+)?)\b", text, re.IGNORECASE
        ):
            assets.add(match.lower())

        return sorted(assets)

    def _build_question_fingerprint(self, question: str) -> str:
        """Build a normalized fingerprint for detecting duplicate questions."""
        q = question.lower()
        # Remove common filler words
        for word in ["can someone", "does anyone", "what's", "what is", "again", "?"]:
            q = q.replace(word, "")
        # Extract the core asset/topic references
        assets = self._extract_assets(question)
        if assets:
            return "|".join(sorted(assets))
        # Fall back to normalized words
        words = sorted(set(re.findall(r"\b\w{4,}\b", q)))
        return "|".join(words[:5])

    def _detect_repeats(self):
        """Mark signals as repeats based on question fingerprint clustering."""
        repeat_fingerprints = {
            fp for fp, threads in self._question_fingerprints.items() if len(threads) > 1
        }

        for signal in self.signals:
            fp = self._build_question_fingerprint(signal.question)
            if fp in repeat_fingerprints:
                signal.is_repeat_question = True

    def _generate_insight(
        self, advisory_type: AdvisoryType, question: str, response: str
    ) -> str:
        """Generate a short actionable insight for an advisory signal."""
        insights = {
            AdvisoryType.DATA_GUIDANCE: (
                "Users need clearer guidance on which assets to use. "
                "Consider adding deprecation notices and recommended alternatives in Atlan."
            ),
            AdvisoryType.DEFINITION_CLARIFICATION: (
                "Asset definitions or column values are unclear. "
                "Enrich descriptions and add enumeration documentation in the data catalog."
            ),
            AdvisoryType.BEST_PRACTICE: (
                "Users need usage guidance beyond basic definitions. "
                "Add README-style usage notes or best practice annotations to the asset."
            ),
            AdvisoryType.TROUBLESHOOTING: (
                "Known data quality issues are being communicated ad-hoc. "
                "Document known issues and caveats directly on the asset in Atlan."
            ),
            AdvisoryType.GOVERNANCE_COMPLIANCE: (
                "Governance and access processes are unclear. "
                "Document access request workflows and PII classification in the catalog."
            ),
            AdvisoryType.ARCHITECTURE_LINEAGE: (
                "Users need to understand data flow and freshness. "
                "Ensure lineage is captured and freshness/SLA metadata is documented."
            ),
            AdvisoryType.METRIC_INTERPRETATION: (
                "Metric definitions and thresholds need official documentation. "
                "Add business glossary entries with approved definitions and calculation logic."
            ),
            AdvisoryType.OWNERSHIP_ROUTING: (
                "Users can't find who owns data assets. "
                "Assign and surface ownership metadata prominently in the catalog."
            ),
            AdvisoryType.UNKNOWN: (
                "Review this conversation for potential catalog enrichment opportunities."
            ),
        }
        return insights.get(advisory_type, insights[AdvisoryType.UNKNOWN])

    def _aggregate_patterns(self) -> list[AdvisoryPattern]:
        """Aggregate individual signals into advisory patterns."""
        grouped: dict[AdvisoryType, list[AdvisorySignal]] = defaultdict(list)
        for signal in self.signals:
            grouped[signal.advisory_type].append(signal)

        patterns = []
        for advisory_type, signals in grouped.items():
            questioners = set(s.questioner for s in signals)
            all_advisors = []
            all_assets = []
            urgency_counts = defaultdict(int)

            for s in signals:
                all_advisors.extend(s.advisors)
                all_assets.extend(s.assets_referenced)
                urgency_counts[s.urgency.value] += 1

            # Count advisor frequency
            advisor_freq = defaultdict(int)
            for a in all_advisors:
                advisor_freq[a] += 1
            top_advisors = sorted(advisor_freq.items(), key=lambda x: x[1], reverse=True)

            # Count asset frequency
            asset_freq = defaultdict(int)
            for a in all_assets:
                asset_freq[a] += 1
            common_assets = sorted(asset_freq.items(), key=lambda x: x[1], reverse=True)

            repeat_count = sum(1 for s in signals if s.is_repeat_question)

            pattern = AdvisoryPattern(
                advisory_type=advisory_type,
                signal_count=len(signals),
                unique_questioners=len(questioners),
                repeat_rate=round(repeat_count / len(signals) * 100, 1) if signals else 0.0,
                top_advisors=[
                    {"name": name, "count": count} for name, count in top_advisors[:5]
                ],
                common_assets=[
                    {"name": name, "count": count} for name, count in common_assets[:5]
                ],
                urgency_distribution=dict(urgency_counts),
                sample_questions=[s.question for s in signals[:3]],
            )
            patterns.append(pattern)

        # Sort by signal count descending
        patterns.sort(key=lambda p: p.signal_count, reverse=True)
        return patterns

    def _generate_report(self, patterns: list[AdvisoryPattern]) -> dict:
        """Generate the final advisory signals report."""
        # Overall distribution
        type_distribution = {}
        total_signals = len(self.signals)
        for pattern in patterns:
            pct = round(pattern.signal_count / total_signals * 100, 1) if total_signals else 0
            type_distribution[pattern.advisory_type.value] = {
                "count": pattern.signal_count,
                "percentage": pct,
            }

        # Repeat question analysis
        repeat_signals = [s for s in self.signals if s.is_repeat_question]
        repeat_summary = {
            "total_repeats": len(repeat_signals),
            "repeat_rate": (
                round(len(repeat_signals) / total_signals * 100, 1) if total_signals else 0
            ),
            "top_repeated_topics": self._get_top_repeated_topics(),
        }

        # Advisor workload
        advisor_workload = self._calculate_advisor_workload()

        # High-urgency items
        high_urgency = [
            {
                "thread_id": s.thread_id,
                "advisory_type": s.advisory_type.value,
                "question": s.question[:200],
                "assets": s.assets_referenced,
                "insight": s.actionable_insight,
            }
            for s in self.signals
            if s.urgency == AdvisoryUrgency.HIGH
        ]

        # Catalog enrichment opportunities (actionable output)
        enrichment_opportunities = self._generate_enrichment_opportunities(patterns)

        return {
            "channel_name": self.channel_name,
            "date_range": self.date_range,
            "summary": {
                "total_advisory_signals": total_signals,
                "advisory_types_detected": len(patterns),
                "high_urgency_signals": len(high_urgency),
                "repeat_question_rate": repeat_summary["repeat_rate"],
            },
            "advisory_type_distribution": type_distribution,
            "advisory_patterns": [
                {
                    "type": p.advisory_type.value,
                    "signal_count": p.signal_count,
                    "unique_questioners": p.unique_questioners,
                    "repeat_rate": p.repeat_rate,
                    "top_advisors": p.top_advisors,
                    "common_assets": p.common_assets,
                    "urgency_distribution": p.urgency_distribution,
                    "sample_questions": p.sample_questions,
                }
                for p in patterns
            ],
            "repeat_question_analysis": repeat_summary,
            "advisor_workload": advisor_workload,
            "high_urgency_signals": high_urgency,
            "enrichment_opportunities": enrichment_opportunities,
            "signals": [
                {
                    "thread_id": s.thread_id,
                    "advisory_type": s.advisory_type.value,
                    "urgency": s.urgency.value,
                    "question": s.question,
                    "questioner": s.questioner,
                    "questioner_role": s.questioner_role,
                    "advisors": s.advisors,
                    "assets_referenced": s.assets_referenced,
                    "is_repeat": s.is_repeat_question,
                    "insight": s.actionable_insight,
                }
                for s in self.signals
            ],
        }

    def _get_top_repeated_topics(self) -> list:
        """Get the most frequently repeated question topics."""
        topic_counts = defaultdict(lambda: {"count": 0, "questions": []})

        for fp, threads in self._question_fingerprints.items():
            if len(threads) > 1:
                # Find matching signals to get readable questions
                for signal in self.signals:
                    if signal.thread_id in threads:
                        topic_counts[fp]["count"] = len(threads)
                        if len(topic_counts[fp]["questions"]) < 2:
                            topic_counts[fp]["questions"].append(signal.question[:150])
                        break

        sorted_topics = sorted(topic_counts.items(), key=lambda x: x[1]["count"], reverse=True)
        return [
            {"topic": fp, "times_asked": data["count"], "examples": data["questions"]}
            for fp, data in sorted_topics[:5]
        ]

    def _calculate_advisor_workload(self) -> list:
        """Calculate how advisory effort is distributed across team members."""
        workload: dict[str, dict] = defaultdict(
            lambda: {"signals_handled": 0, "types": defaultdict(int), "role": ""}
        )

        for signal in self.signals:
            for advisor, role in zip(signal.advisors, signal.advisor_roles):
                workload[advisor]["signals_handled"] += 1
                workload[advisor]["types"][signal.advisory_type.value] += 1
                workload[advisor]["role"] = role

        result = []
        for name, data in sorted(
            workload.items(), key=lambda x: x[1]["signals_handled"], reverse=True
        ):
            top_type = max(data["types"].items(), key=lambda x: x[1]) if data["types"] else ("Unknown", 0)
            result.append(
                {
                    "advisor": name,
                    "role": data["role"],
                    "signals_handled": data["signals_handled"],
                    "primary_advisory_type": top_type[0],
                    "type_breakdown": dict(data["types"]),
                }
            )

        return result[:10]

    def _generate_enrichment_opportunities(
        self, patterns: list[AdvisoryPattern]
    ) -> list:
        """Generate concrete catalog enrichment actions based on advisory patterns."""
        opportunities = []

        for pattern in patterns:
            if pattern.signal_count == 0:
                continue

            assets = [a["name"] for a in pattern.common_assets[:5]]

            opportunity = {
                "advisory_type": pattern.advisory_type.value,
                "priority": "High" if pattern.repeat_rate > 30 or pattern.signal_count >= 5 else (
                    "Medium" if pattern.signal_count >= 2 else "Low"
                ),
                "affected_assets": assets,
                "signal_count": pattern.signal_count,
                "repeat_rate": pattern.repeat_rate,
            }

            # Type-specific recommended actions
            actions = {
                AdvisoryType.DATA_GUIDANCE: [
                    "Add deprecation notices to legacy assets",
                    "Create 'Recommended Alternative' links between old and new assets",
                    "Add README annotations with usage guidance",
                ],
                AdvisoryType.DEFINITION_CLARIFICATION: [
                    "Enrich asset descriptions with business-friendly definitions",
                    "Document column-level enumeration values",
                    "Add grain/granularity metadata to tables",
                ],
                AdvisoryType.BEST_PRACTICE: [
                    "Add usage notes and best practice annotations",
                    "Document common query patterns and gotchas",
                    "Create sample query templates for common use cases",
                ],
                AdvisoryType.TROUBLESHOOTING: [
                    "Document known data quality issues on affected assets",
                    "Add data quality check results to asset metadata",
                    "Set up automated quality monitoring alerts",
                ],
                AdvisoryType.GOVERNANCE_COMPLIANCE: [
                    "Classify PII columns with sensitivity tags",
                    "Document access request workflows in asset descriptions",
                    "Add compliance classification metadata",
                ],
                AdvisoryType.ARCHITECTURE_LINEAGE: [
                    "Ensure automated lineage capture is configured",
                    "Document freshness SLAs and update schedules",
                    "Add source system and sync frequency metadata",
                ],
                AdvisoryType.METRIC_INTERPRETATION: [
                    "Create business glossary entries for key metrics",
                    "Document official KPI definitions with calculation logic",
                    "Add threshold and interpretation guides to metric assets",
                ],
                AdvisoryType.OWNERSHIP_ROUTING: [
                    "Assign and verify asset ownership in the catalog",
                    "Set up ownership groups for team-level accountability",
                    "Add contact information and escalation paths",
                ],
            }

            opportunity["recommended_actions"] = actions.get(
                pattern.advisory_type,
                ["Review conversations for enrichment opportunities"],
            )

            opportunities.append(opportunity)

        # Sort by priority
        priority_order = {"High": 0, "Medium": 1, "Low": 2}
        opportunities.sort(key=lambda x: priority_order.get(x["priority"], 3))

        return opportunities


def format_advisory_report(analysis: dict) -> str:
    """Format the advisory signals analysis as a markdown report."""
    lines = []

    lines.append("# Call Intelligence: Advisory Signals Report")
    lines.append("")
    lines.append(f"**Source:** {analysis['channel_name']}")
    lines.append(f"**Period:** {analysis['date_range']}")
    lines.append("")

    # Summary
    summary = analysis["summary"]
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **Total advisory signals:** {summary['total_advisory_signals']}")
    lines.append(f"- **Advisory types detected:** {summary['advisory_types_detected']}")
    lines.append(f"- **High urgency signals:** {summary['high_urgency_signals']}")
    lines.append(f"- **Repeat question rate:** {summary['repeat_question_rate']}%")
    lines.append("")

    # Advisory Type Distribution
    lines.append("## Advisory Type Distribution")
    lines.append("")
    dist = analysis["advisory_type_distribution"]
    for type_name, data in sorted(dist.items(), key=lambda x: x[1]["count"], reverse=True):
        bar = "#" * int(data["percentage"] / 5)
        lines.append(f"- **{type_name}**: {data['count']} signals ({data['percentage']}%) {bar}")
    lines.append("")

    # Advisory Patterns
    lines.append("## Advisory Patterns")
    lines.append("")
    for pattern in analysis["advisory_patterns"]:
        lines.append(f"### {pattern['type']}")
        lines.append(f"**Signals:** {pattern['signal_count']} | "
                     f"**Unique questioners:** {pattern['unique_questioners']} | "
                     f"**Repeat rate:** {pattern['repeat_rate']}%")
        lines.append("")

        if pattern["top_advisors"]:
            advisor_str = ", ".join(
                f"{a['name']} ({a['count']})" for a in pattern["top_advisors"][:3]
            )
            lines.append(f"**Top advisors:** {advisor_str}")

        if pattern["common_assets"]:
            asset_str = ", ".join(
                f"`{a['name']}` ({a['count']})" for a in pattern["common_assets"][:3]
            )
            lines.append(f"**Common assets:** {asset_str}")

        if pattern["sample_questions"]:
            lines.append("**Sample questions:**")
            for q in pattern["sample_questions"][:2]:
                lines.append(f"> {q[:150]}")

        lines.append("")
        lines.append("---")
        lines.append("")

    # Advisor Workload
    lines.append("## Advisor Workload")
    lines.append("")
    for advisor in analysis.get("advisor_workload", [])[:5]:
        lines.append(
            f"- **{advisor['advisor']}** ({advisor['role']}): "
            f"{advisor['signals_handled']} signals, "
            f"primary type: {advisor['primary_advisory_type']}"
        )
    lines.append("")

    # High Urgency
    high_urgency = analysis.get("high_urgency_signals", [])
    if high_urgency:
        lines.append("## High Urgency Signals")
        lines.append("")
        for item in high_urgency:
            lines.append(f"- **[{item['advisory_type']}]** {item['question'][:120]}")
            if item["assets"]:
                lines.append(f"  Assets: {', '.join(item['assets'][:3])}")
        lines.append("")

    # Enrichment Opportunities
    lines.append("## Catalog Enrichment Opportunities")
    lines.append("")
    for opp in analysis.get("enrichment_opportunities", []):
        priority_icon = {"High": "!!!", "Medium": "!!", "Low": "!"}.get(opp["priority"], "")
        lines.append(f"### [{opp['priority']}] {opp['advisory_type']} {priority_icon}")
        lines.append(f"**Signals:** {opp['signal_count']} | **Repeat rate:** {opp['repeat_rate']}%")
        if opp["affected_assets"]:
            lines.append(f"**Assets:** {', '.join(f'`{a}`' for a in opp['affected_assets'])}")
        lines.append("**Recommended actions:**")
        for action in opp["recommended_actions"]:
            lines.append(f"- {action}")
        lines.append("")

    # Repeat Analysis
    repeat = analysis.get("repeat_question_analysis", {})
    if repeat.get("top_repeated_topics"):
        lines.append("## Repeat Question Analysis")
        lines.append("")
        lines.append(f"**Overall repeat rate:** {repeat['repeat_rate']}%")
        lines.append("")
        for topic in repeat["top_repeated_topics"]:
            lines.append(f"- **Asked {topic['times_asked']} times:** {topic['examples'][0][:120] if topic['examples'] else 'N/A'}")
        lines.append("")

    return "\n".join(lines)
