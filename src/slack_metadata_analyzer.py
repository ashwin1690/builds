#!/usr/bin/env python3
"""
Slack Metadata Gap Analyzer for Atlan Data Catalog

This tool analyzes Slack channel messages to identify patterns in metadata demand,
extract contextual information for data assets, and generate actionable recommendations
for Atlan's enrichment agents.

Usage:
    python slack_metadata_analyzer.py --input data/sample_slack_messages.json --output reports/
"""

import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from enum import Enum


class QuestionType(Enum):
    DEFINITIONAL = "Definitional"
    LINEAGE = "Lineage"
    USAGE = "Usage"
    QUALITY = "Quality"
    BUSINESS_CONTEXT = "Business Context"
    OWNERSHIP = "Ownership"
    ACCESS = "Access"
    UNKNOWN = "Unknown"


class AssetType(Enum):
    TABLE = "Table"
    DASHBOARD = "Dashboard"
    COLUMN = "Column"
    METRIC = "Metric"
    UNKNOWN = "Unknown"


@dataclass
class ExtractedContext:
    description: str = ""
    business_context: str = ""
    gotchas: list = field(default_factory=list)
    ownership: list = field(default_factory=list)
    related_terms: list = field(default_factory=list)
    lineage_info: list = field(default_factory=list)
    freshness_info: str = ""
    access_info: str = ""


@dataclass
class AssetMention:
    thread_id: str
    timestamp: str
    question: str
    question_type: QuestionType
    questioner: str
    questioner_role: str
    answers: list
    extracted_context: ExtractedContext


@dataclass
class AnalyzedAsset:
    name: str
    asset_type: AssetType
    mentions: list = field(default_factory=list)
    priority_score: float = 0.0

    @property
    def num_questions(self):
        return len(self.mentions)

    @property
    def unique_questioners(self):
        return len(set(m.questioner for m in self.mentions))

    @property
    def question_types(self):
        return [m.question_type.value for m in self.mentions]


class SlackMetadataAnalyzer:
    """Analyzes Slack messages to identify metadata gaps and extract context."""

    # Patterns for identifying assets in messages
    ASSET_PATTERNS = [
        # Fully qualified table names
        r'(?P<schema>\w+)\.(?P<table>\w+)\.(?P<column>\w+)',
        r'(?P<schema>\w+)\.(?P<table>\w+)',
        # Backtick-wrapped names
        r'`(?P<asset>[^`]+)`',
        # Common table name patterns
        r'\b(?P<table>(?:dim|fct|stg|raw)_\w+)\b',
        r'\b(?P<table>\w+_(?:daily|weekly|monthly|v\d+))\b',
    ]

    # Keywords for question type classification
    QUESTION_KEYWORDS = {
        QuestionType.DEFINITIONAL: [
            'what does', 'what is', 'what\'s', 'meaning', 'definition', 'define',
            'explain', 'difference between', 'values', 'mean'
        ],
        QuestionType.LINEAGE: [
            'where does', 'source', 'come from', 'upstream', 'downstream',
            'derived', 'calculated', 'pulls from', 'synced from'
        ],
        QuestionType.USAGE: [
            'how do i', 'how to', 'which should i use', 'should i use',
            'when to use', 'best practice', 'how can i'
        ],
        QuestionType.QUALITY: [
            'reliable', 'accurate', 'trust', 'quality', 'issues', 'problems',
            'weird', 'wrong', 'correct', 'valid', 'safe to use'
        ],
        QuestionType.BUSINESS_CONTEXT: [
            'business', 'kpi', 'metric', 'reporting', 'investor', 'official',
            'reconcile', 'finance', 'compliance'
        ],
        QuestionType.OWNERSHIP: [
            'who owns', 'who maintains', 'contact', 'responsible', 'team owns',
            'who should i'
        ],
        QuestionType.ACCESS: [
            'access', 'permission', 'how do i get', 'request access',
            'can i access', 'restricted'
        ]
    }

    # Keywords indicating ownership in responses
    OWNERSHIP_PATTERNS = [
        r'(?P<team>\w+(?:\s+\w+)*)\s+team\s+owns',
        r'owned by\s+(?:the\s+)?(?P<team>\w+(?:\s+\w+)*)',
        r'@(?P<person>\w+)',
        r'(?P<person>\w+\.\w+)\s+is\s+(?:the\s+)?(?:primary\s+)?(?:contact|owner)',
    ]

    def __init__(self, messages_data: dict):
        self.channel_name = messages_data.get('channel_name', 'Unknown')
        self.date_range = messages_data.get('date_range', 'Unknown')
        self.messages = messages_data.get('messages', [])
        self.assets: dict[str, AnalyzedAsset] = {}

    def analyze(self) -> dict:
        """Main analysis entry point."""
        # Step 1: Extract and group by assets
        self._identify_assets()

        # Step 2: Classify questions and extract context
        self._analyze_threads()

        # Step 3: Calculate priority scores
        self._calculate_priorities()

        # Step 4: Generate analysis report
        return self._generate_report()

    def _identify_assets(self):
        """Scan messages to identify mentioned data assets."""
        for msg in self.messages:
            assets_found = self._extract_assets_from_text(msg['message'])
            for reply in msg.get('replies', []):
                assets_found.extend(self._extract_assets_from_text(reply['message']))

            # Dedupe and normalize
            unique_assets = set()
            for asset in assets_found:
                normalized = self._normalize_asset_name(asset)
                if normalized:
                    unique_assets.add(normalized)

            # Create or update asset records
            for asset_name in unique_assets:
                if asset_name not in self.assets:
                    asset_type = self._infer_asset_type(asset_name)
                    self.assets[asset_name] = AnalyzedAsset(
                        name=asset_name,
                        asset_type=asset_type
                    )

    def _extract_assets_from_text(self, text: str) -> list:
        """Extract potential asset names from text."""
        assets = []

        # Check for backtick-wrapped names
        backtick_matches = re.findall(r'`([^`]+)`', text)
        assets.extend(backtick_matches)

        # Check for qualified names (schema.table or schema.table.column)
        qualified_matches = re.findall(r'\b(\w+\.\w+(?:\.\w+)?)\b', text)
        assets.extend(qualified_matches)

        # Check for known table patterns
        table_patterns = [
            r'\b((?:dim|fct|stg|raw)_\w+)\b',
            r'\b(\w+_(?:daily|weekly|monthly)(?:_v\d+)?)\b',
            r'\b(\w+_v\d+)\b',
        ]
        for pattern in table_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            assets.extend(matches)

        return assets

    def _normalize_asset_name(self, name: str) -> Optional[str]:
        """Normalize asset names to a standard format."""
        if not name or len(name) < 3:
            return None

        # Filter out common false positives
        false_positives = {'a.m', 'p.m', 'i.e', 'e.g', 'etc', 'vs', 'v1', 'v2'}
        if name.lower() in false_positives:
            return None

        return name.lower().strip()

    def _infer_asset_type(self, asset_name: str) -> AssetType:
        """Infer the type of asset from its name."""
        name_lower = asset_name.lower()

        if 'dashboard' in name_lower:
            return AssetType.DASHBOARD
        elif any(x in name_lower for x in ['dim_', 'fct_', 'stg_', '_daily', '_weekly']):
            return AssetType.TABLE
        elif '.' in name_lower and name_lower.count('.') >= 2:
            return AssetType.COLUMN
        elif any(x in name_lower for x in ['_score', '_rate', '_count', 'mrr', 'arr']):
            return AssetType.METRIC
        else:
            return AssetType.TABLE  # Default assumption

    def _analyze_threads(self):
        """Analyze each thread to extract context and classify questions."""
        for msg in self.messages:
            question = msg['message']
            question_type = self._classify_question(question)

            # Find which assets this thread is about
            assets_in_thread = set()
            assets_in_thread.update(self._extract_assets_from_text(question))
            for reply in msg.get('replies', []):
                assets_in_thread.update(self._extract_assets_from_text(reply['message']))

            # Extract context from answers
            answers = [r['message'] for r in msg.get('replies', [])]
            context = self._extract_context(question, answers, question_type)

            # Create mention record
            mention = AssetMention(
                thread_id=msg['thread_id'],
                timestamp=msg['timestamp'],
                question=question,
                question_type=question_type,
                questioner=msg['user'],
                questioner_role=msg.get('user_role', 'Unknown'),
                answers=answers,
                extracted_context=context
            )

            # Associate with assets
            for asset in assets_in_thread:
                normalized = self._normalize_asset_name(asset)
                if normalized and normalized in self.assets:
                    self.assets[normalized].mentions.append(mention)

    def _classify_question(self, question: str) -> QuestionType:
        """Classify the type of question being asked."""
        question_lower = question.lower()

        scores = {}
        for q_type, keywords in self.QUESTION_KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in question_lower)
            scores[q_type] = score

        best_match = max(scores, key=scores.get)
        if scores[best_match] > 0:
            return best_match
        return QuestionType.UNKNOWN

    def _extract_context(self, question: str, answers: list,
                         question_type: QuestionType) -> ExtractedContext:
        """Extract reusable context from thread answers."""
        context = ExtractedContext()
        full_text = ' '.join(answers)

        # Extract ownership information
        for pattern in self.OWNERSHIP_PATTERNS:
            matches = re.finditer(pattern, full_text, re.IGNORECASE)
            for match in matches:
                if 'team' in match.groupdict() and match.group('team'):
                    context.ownership.append(f"{match.group('team')} Team")
                elif 'person' in match.groupdict() and match.group('person'):
                    context.ownership.append(match.group('person'))

        # Extract enumeration values (common definitional pattern)
        enum_pattern = r'(\d+)\s*[=:]\s*([^,\.\d][^,\.]*)'
        enum_matches = re.findall(enum_pattern, full_text)
        if enum_matches:
            enum_values = {int(k): v.strip() for k, v in enum_matches}
            if enum_values:
                context.description = f"Enumeration values: {enum_values}"

        # Extract quality caveats
        caveat_keywords = ['known issue', 'careful', 'note that', 'warning',
                          'caveat', 'gotcha', 'be aware', 'filter on', 'gaps']
        for answer in answers:
            answer_lower = answer.lower()
            for kw in caveat_keywords:
                if kw in answer_lower:
                    context.gotchas.append(answer)
                    break

        # Extract freshness information
        freshness_patterns = [
            r'(?:updates?|refreshes?|syncs?)\s+(?:every\s+)?(\d+\s*(?:minutes?|hours?|days?))',
            r'(\d+\s*(?:minutes?|hours?))\s+latency',
            r'refreshes?\s+at\s+(\d+\s*(?:am|pm)\s*\w*)',
        ]
        for pattern in freshness_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                context.freshness_info = match.group(0)
                break

        # Extract business context
        business_keywords = ['source of truth', 'official', 'reconcile', 'investor',
                            'kpi', 'leadership', 'approved']
        for answer in answers:
            answer_lower = answer.lower()
            for kw in business_keywords:
                if kw in answer_lower:
                    context.business_context = answer
                    break

        # Extract related business terms
        term_patterns = [
            r'\b(ARR|MRR|DAU|MAU|LTV|CAC|NPS|CSAT|GMV|AOV|conversion|churn|retention)\b',
            r'\b(revenue|customer|user|subscription|order|product)\b'
        ]
        for pattern in term_patterns:
            matches = re.findall(pattern, full_text, re.IGNORECASE)
            context.related_terms.extend([m.upper() if len(m) <= 4 else m.title()
                                         for m in matches])
        context.related_terms = list(set(context.related_terms))

        return context

    def _calculate_priorities(self):
        """Calculate priority scores for each asset."""
        for asset in self.assets.values():
            if not asset.mentions:
                asset.priority_score = 0
                continue

            # Base score from number of questions
            question_score = min(asset.num_questions * 1.5, 5)

            # Diversity of questioners
            diversity_score = min(asset.unique_questioners * 0.8, 2)

            # Question complexity (multiple question types = more complex)
            unique_types = len(set(asset.question_types))
            complexity_score = min(unique_types * 0.5, 2)

            # Recurring questions bonus (same question asked multiple times)
            recurrence_bonus = 0
            if asset.num_questions >= 3:
                recurrence_bonus = 1

            asset.priority_score = round(
                question_score + diversity_score + complexity_score + recurrence_bonus,
                1
            )

    def _generate_report(self) -> dict:
        """Generate the final analysis report."""
        # Sort assets by priority
        sorted_assets = sorted(
            self.assets.values(),
            key=lambda a: a.priority_score,
            reverse=True
        )

        # Calculate question type distribution
        all_types = []
        for asset in self.assets.values():
            all_types.extend(asset.question_types)

        type_distribution = {}
        total = len(all_types) if all_types else 1
        for q_type in QuestionType:
            count = all_types.count(q_type.value)
            type_distribution[q_type.value] = round(count / total * 100, 1)

        # Identify metadata gaps
        gaps = self._identify_metadata_gaps(sorted_assets)

        # Generate agent recommendations
        agent_recommendations = self._generate_agent_recommendations(sorted_assets)

        return {
            'channel_name': self.channel_name,
            'date_range': self.date_range,
            'summary': {
                'total_threads_analyzed': len(self.messages),
                'unique_assets_identified': len(self.assets),
                'assets_with_questions': len([a for a in self.assets.values() if a.mentions])
            },
            'priority_assets': [self._format_asset_report(a) for a in sorted_assets[:15]
                               if a.mentions],
            'question_type_distribution': type_distribution,
            'metadata_gaps': gaps,
            'agent_recommendations': agent_recommendations
        }

    def _format_asset_report(self, asset: AnalyzedAsset) -> dict:
        """Format a single asset for the report."""
        # Synthesize description from all extracted contexts
        descriptions = []
        business_contexts = []
        all_gotchas = []
        all_ownership = []
        all_terms = []

        for mention in asset.mentions:
            ctx = mention.extracted_context
            if ctx.description:
                descriptions.append(ctx.description)
            if ctx.business_context:
                business_contexts.append(ctx.business_context)
            all_gotchas.extend(ctx.gotchas)
            all_ownership.extend(ctx.ownership)
            all_terms.extend(ctx.related_terms)

        # Determine question complexity
        unique_types = len(set(asset.question_types))
        if unique_types >= 3:
            complexity = "High"
        elif unique_types >= 2:
            complexity = "Medium"
        else:
            complexity = "Low"

        return {
            'name': asset.name,
            'asset_type': asset.asset_type.value,
            'priority_score': asset.priority_score,
            'demand_signals': {
                'num_questions': asset.num_questions,
                'unique_questioners': asset.unique_questioners,
                'question_complexity': complexity,
                'common_question_types': list(set(asset.question_types))
            },
            'extracted_context': {
                'description': ' | '.join(set(descriptions)) if descriptions else None,
                'business_context': ' | '.join(set(business_contexts)) if business_contexts else None,
                'gotchas': list(set(all_gotchas)),
                'ownership': list(set(all_ownership)),
                'related_terms': list(set(all_terms))
            },
            'sample_questions': [m.question for m in asset.mentions[:3]]
        }

    def _identify_metadata_gaps(self, sorted_assets: list) -> list:
        """Identify systemic metadata gaps."""
        gaps = []

        # Check for definitional gaps
        definitional_assets = [a for a in sorted_assets
                              if QuestionType.DEFINITIONAL.value in a.question_types]
        if len(definitional_assets) >= 3:
            gaps.append({
                'gap_type': 'Missing Descriptions',
                'description': 'Multiple assets lack clear descriptions, causing repeated definitional questions',
                'affected_assets': [a.name for a in definitional_assets[:5]],
                'severity': 'High'
            })

        # Check for ownership gaps
        ownership_assets = [a for a in sorted_assets
                          if QuestionType.OWNERSHIP.value in a.question_types]
        if ownership_assets:
            gaps.append({
                'gap_type': 'Missing Ownership Information',
                'description': 'Users frequently ask who owns certain assets',
                'affected_assets': [a.name for a in ownership_assets[:5]],
                'severity': 'Medium'
            })

        # Check for enumeration/value gaps
        enum_mentions = []
        for asset in sorted_assets:
            for mention in asset.mentions:
                if 'values' in mention.question.lower() or '=' in mention.question:
                    enum_mentions.append(asset.name)
        if len(set(enum_mentions)) >= 2:
            gaps.append({
                'gap_type': 'Undocumented Enumeration Values',
                'description': 'Column values/codes lack documentation causing confusion',
                'affected_assets': list(set(enum_mentions))[:5],
                'severity': 'High'
            })

        # Check for deprecated/duplicate asset confusion
        version_assets = [a for a in sorted_assets if '_v' in a.name or 'legacy' in a.name]
        if version_assets:
            gaps.append({
                'gap_type': 'Versioning/Deprecation Confusion',
                'description': 'Multiple versions or legacy assets exist without clear guidance',
                'affected_assets': [a.name for a in version_assets],
                'severity': 'Medium'
            })

        return gaps

    def _generate_agent_recommendations(self, sorted_assets: list) -> dict:
        """Generate recommendations for Atlan enrichment agents."""
        recommendations = {
            'description_agent': [],
            'glossary_linkage_agent': [],
            'ownership_agent': [],
            'quality_context_agent': []
        }

        for asset in sorted_assets:
            if not asset.mentions:
                continue

            # Collect all context
            all_descriptions = []
            all_ownership = []
            all_gotchas = []
            all_terms = []

            for mention in asset.mentions:
                ctx = mention.extracted_context
                if ctx.description:
                    all_descriptions.append(ctx.description)
                all_ownership.extend(ctx.ownership)
                all_gotchas.extend(ctx.gotchas)
                all_terms.extend(ctx.related_terms)

            # Description Agent: Assets with synthesized descriptions
            if all_descriptions:
                recommendations['description_agent'].append({
                    'asset': asset.name,
                    'suggested_description': all_descriptions[0],
                    'source_threads': [m.thread_id for m in asset.mentions[:3]]
                })

            # Ownership Agent: Assets with identified owners
            if all_ownership:
                recommendations['ownership_agent'].append({
                    'asset': asset.name,
                    'identified_owners': list(set(all_ownership)),
                    'confidence': 'High' if len(all_ownership) > 1 else 'Medium'
                })

            # Quality Context Agent: Assets with quality caveats
            if all_gotchas:
                recommendations['quality_context_agent'].append({
                    'asset': asset.name,
                    'quality_notes': list(set(all_gotchas)),
                    'severity': 'High' if len(all_gotchas) > 1 else 'Medium'
                })

            # Glossary Linkage: Assets with business terms
            if all_terms:
                recommendations['glossary_linkage_agent'].append({
                    'asset': asset.name,
                    'terms_to_link': list(set(all_terms)),
                    'term_count': len(set(all_terms))
                })

        return recommendations


def format_markdown_report(analysis: dict) -> str:
    """Format the analysis as a markdown report."""
    lines = []

    lines.append("# Slack Metadata Gap Analysis Report")
    lines.append("")
    lines.append(f"**Channel:** {analysis['channel_name']}")
    lines.append(f"**Period:** {analysis['date_range']}")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # Summary
    lines.append("## Executive Summary")
    lines.append("")
    summary = analysis['summary']
    lines.append(f"- **Threads Analyzed:** {summary['total_threads_analyzed']}")
    lines.append(f"- **Unique Assets Identified:** {summary['unique_assets_identified']}")
    lines.append(f"- **Assets with Questions:** {summary['assets_with_questions']}")
    lines.append("")

    # Priority Assets
    lines.append("## Priority Assets for Metadata Curation")
    lines.append("")

    for asset in analysis['priority_assets']:
        lines.append(f"### {asset['name']}")
        lines.append("")
        lines.append(f"**Asset Type:** {asset['asset_type']}")
        lines.append(f"**Priority Score:** {asset['priority_score']}/10")
        lines.append("")

        lines.append("**Demand Signals:**")
        ds = asset['demand_signals']
        lines.append(f"- Number of questions: {ds['num_questions']}")
        lines.append(f"- Unique questioners: {ds['unique_questioners']}")
        lines.append(f"- Question complexity: {ds['question_complexity']}")
        lines.append(f"- Question types: {', '.join(ds['common_question_types'])}")
        lines.append("")

        ctx = asset['extracted_context']
        if any([ctx['description'], ctx['business_context'], ctx['gotchas'],
                ctx['ownership'], ctx['related_terms']]):
            lines.append("**Extracted Context:**")
            if ctx['description']:
                lines.append(f"- *Description:* {ctx['description']}")
            if ctx['business_context']:
                lines.append(f"- *Business Context:* {ctx['business_context'][:200]}...")
            if ctx['ownership']:
                lines.append(f"- *Ownership:* {', '.join(ctx['ownership'])}")
            if ctx['gotchas']:
                lines.append(f"- *Gotchas/Caveats:* {len(ctx['gotchas'])} noted")
            if ctx['related_terms']:
                lines.append(f"- *Related Terms:* {', '.join(ctx['related_terms'][:5])}")
            lines.append("")

        lines.append("**Sample Questions:**")
        for q in asset['sample_questions']:
            lines.append(f"> {q[:150]}{'...' if len(q) > 150 else ''}")
        lines.append("")
        lines.append("---")
        lines.append("")

    # Question Type Distribution
    lines.append("## Pattern Analysis")
    lines.append("")
    lines.append("### Question Type Distribution")
    lines.append("")
    for q_type, pct in sorted(analysis['question_type_distribution'].items(),
                              key=lambda x: x[1], reverse=True):
        if pct > 0:
            lines.append(f"- **{q_type}:** {pct}%")
    lines.append("")

    # Metadata Gaps
    lines.append("### Identified Metadata Gaps")
    lines.append("")
    for gap in analysis['metadata_gaps']:
        lines.append(f"#### {gap['gap_type']} (Severity: {gap['severity']})")
        lines.append(f"{gap['description']}")
        lines.append(f"- Affected assets: {', '.join(gap['affected_assets'])}")
        lines.append("")

    # Agent Recommendations
    lines.append("## Workflow Agent Opportunities")
    lines.append("")

    recs = analysis['agent_recommendations']

    if recs['description_agent']:
        lines.append("### Description Agent Candidates")
        lines.append("Assets with clear definitions extracted from Slack answers:")
        lines.append("")
        for item in recs['description_agent'][:5]:
            lines.append(f"- **{item['asset']}**: \"{item['suggested_description'][:100]}...\"")
        lines.append("")

    if recs['ownership_agent']:
        lines.append("### Ownership Agent Candidates")
        lines.append("Assets with identified owners in threads:")
        lines.append("")
        for item in recs['ownership_agent'][:5]:
            lines.append(f"- **{item['asset']}**: {', '.join(item['identified_owners'])} ({item['confidence']} confidence)")
        lines.append("")

    if recs['quality_context_agent']:
        lines.append("### Quality Context Agent Candidates")
        lines.append("Assets with quality caveats mentioned:")
        lines.append("")
        for item in recs['quality_context_agent'][:5]:
            lines.append(f"- **{item['asset']}**: {len(item['quality_notes'])} quality notes ({item['severity']} severity)")
        lines.append("")

    if recs['glossary_linkage_agent']:
        lines.append("### Glossary Linkage Agent Candidates")
        lines.append("Assets with business terms that should be linked:")
        lines.append("")
        for item in sorted(recs['glossary_linkage_agent'],
                          key=lambda x: x['term_count'], reverse=True)[:5]:
            lines.append(f"- **{item['asset']}**: {', '.join(item['terms_to_link'][:5])}")
        lines.append("")

    return '\n'.join(lines)


def main():
    import argparse
    import os

    parser = argparse.ArgumentParser(description='Analyze Slack messages for metadata gaps')
    parser.add_argument('--input', required=True, help='Path to Slack messages JSON file')
    parser.add_argument('--output', default='reports', help='Output directory for reports')
    args = parser.parse_args()

    # Load data
    with open(args.input, 'r') as f:
        data = json.load(f)

    # Run analysis
    analyzer = SlackMetadataAnalyzer(data)
    results = analyzer.analyze()

    # Create output directory
    os.makedirs(args.output, exist_ok=True)

    # Write JSON report
    json_path = os.path.join(args.output, 'analysis_results.json')
    with open(json_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"JSON report written to: {json_path}")

    # Write Markdown report
    md_report = format_markdown_report(results)
    md_path = os.path.join(args.output, 'analysis_report.md')
    with open(md_path, 'w') as f:
        f.write(md_report)
    print(f"Markdown report written to: {md_path}")

    # Print summary
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)
    print(f"Threads analyzed: {results['summary']['total_threads_analyzed']}")
    print(f"Assets identified: {results['summary']['unique_assets_identified']}")
    print(f"Priority assets: {len(results['priority_assets'])}")


if __name__ == '__main__':
    main()
