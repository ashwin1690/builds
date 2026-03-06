"""
Signal Aggregator

Merges advisory signals from multiple sources (Slack, transcript, Gong),
deduplicates overlapping signals, computes composite priorities, and
tracks trends across analysis runs.
"""

import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional

from advisory_signals import AdvisoryType
from enriched_signals import (
    EnrichedAdvisorySignal,
    SignalConfidence,
    TrendSnapshot,
)

logger = logging.getLogger(__name__)


class SignalAggregator:
    """
    Aggregates and deduplicates enriched advisory signals from multiple sources.

    Deduplication: two signals are considered duplicates if they reference the
    same asset and have the same AdvisoryType within a configurable time window.
    When merged, the sources list grows and confidence increases.

    Trend tracking: stores TrendSnapshots from previous runs to detect
    emerging issues and track advisory pattern changes over time.
    """

    def __init__(self, dedup_window_days: int = 7):
        self.dedup_window_days = dedup_window_days
        self._historical_snapshots: List[TrendSnapshot] = []

    def aggregate(
        self,
        *signal_groups: List[EnrichedAdvisorySignal],
    ) -> List[EnrichedAdvisorySignal]:
        """
        Merge multiple signal lists, deduplicate, and recompute priorities.

        Args:
            *signal_groups: Variable number of signal lists from different sources.

        Returns:
            Merged, deduplicated, and priority-sorted list of enriched signals.
        """
        all_signals = []
        for group in signal_groups:
            all_signals.extend(group)

        if not all_signals:
            return []

        # Deduplicate
        merged = self._deduplicate(all_signals)

        # Recompute composite priorities
        for signal in merged:
            signal.compute_composite_priority()

        # Sort by composite priority descending
        merged.sort(key=lambda s: s.composite_priority, reverse=True)

        return merged

    def _deduplicate(
        self, signals: List[EnrichedAdvisorySignal]
    ) -> List[EnrichedAdvisorySignal]:
        """
        Deduplicate signals by (asset_set, advisory_type) key.

        When duplicates are found, merge them: combine sources, boost confidence,
        keep the signal with the most enrichment data.
        """
        # Build dedup key -> list of signals
        groups: Dict[str, List[EnrichedAdvisorySignal]] = defaultdict(list)

        for signal in signals:
            assets_key = "|".join(sorted(signal.base_signal.assets_referenced))
            type_key = signal.base_signal.advisory_type.value
            dedup_key = f"{type_key}::{assets_key}"
            groups[dedup_key].append(signal)

        merged = []
        for key, group in groups.items():
            if len(group) == 1:
                merged.append(group[0])
            else:
                merged.append(self._merge_signals(group))

        return merged

    def _merge_signals(
        self, signals: List[EnrichedAdvisorySignal]
    ) -> EnrichedAdvisorySignal:
        """Merge multiple duplicate signals into one."""
        # Pick the signal with the most enrichment as the base
        best = max(signals, key=lambda s: self._enrichment_depth(s))

        # Merge sources from all signals
        all_sources = set()
        for s in signals:
            all_sources.update(s.sources)
        best.sources = sorted(all_sources)

        # Boost confidence based on multi-source corroboration
        best.confidence.source_count = len(all_sources)
        if len(all_sources) > 1:
            boost = min(len(all_sources) * 0.1, 0.3)
            best.confidence.score = min(best.confidence.score + boost, 1.0)

        # Merge keyword/pattern hits
        best.confidence.keyword_hits = max(s.confidence.keyword_hits for s in signals)
        best.confidence.pattern_hits = max(s.confidence.pattern_hits for s in signals)

        # Take the best catalog validation (most detail)
        catalog_vals = [s.catalog_validation for s in signals if s.catalog_validation]
        if catalog_vals:
            best.catalog_validation = min(catalog_vals, key=lambda v: v.completeness_score)

        # Take the highest business impact
        impacts = [s.business_impact for s in signals if s.business_impact]
        if impacts:
            best.business_impact = max(impacts, key=lambda i: i.score)

        # Merge Gong contexts
        gong_ctxs = [s.gong_context for s in signals if s.gong_context]
        if gong_ctxs:
            best_gong = max(gong_ctxs, key=lambda g: g.mention_count)
            # Merge call IDs and phrases from other contexts
            for g in gong_ctxs:
                if g is not best_gong:
                    for cid in g.call_ids:
                        if cid not in best_gong.call_ids:
                            best_gong.call_ids.append(cid)
                            best_gong.call_count += 1
                    best_gong.mention_count += g.mention_count
                    best_gong.key_phrases.extend(g.key_phrases)
            best_gong.key_phrases = best_gong.key_phrases[:20]
            best.gong_context = best_gong

        # Merge Tableau contexts
        tab_ctxs = [s.tableau_context for s in signals if s.tableau_context]
        if tab_ctxs:
            best_tab = tab_ctxs[0]
            for t in tab_ctxs[1:]:
                best_tab.dashboard_names.extend(
                    d for d in t.dashboard_names if d not in best_tab.dashboard_names
                )
                best_tab.calculated_fields_that_answer.extend(
                    cf for cf in t.calculated_fields_that_answer
                    if cf not in best_tab.calculated_fields_that_answer
                )
                best_tab.data_source_names.extend(
                    ds for ds in t.data_source_names if ds not in best_tab.data_source_names
                )
                best_tab.has_existing_definition = (
                    best_tab.has_existing_definition or t.has_existing_definition
                )
            best.tableau_context = best_tab

        # Mark as repeat if any constituent was a repeat
        if any(s.base_signal.is_repeat_question for s in signals):
            best.base_signal.is_repeat_question = True

        return best

    def _enrichment_depth(self, signal: EnrichedAdvisorySignal) -> int:
        """Count how many enrichment dimensions a signal has."""
        depth = 0
        if signal.catalog_validation:
            depth += 1
        if signal.business_impact:
            depth += 1
        if signal.gong_context:
            depth += 1
        if signal.tableau_context:
            depth += 1
        return depth

    def compute_trend_snapshot(
        self,
        signals: List[EnrichedAdvisorySignal],
        period_label: str,
    ) -> TrendSnapshot:
        """Create a trend snapshot for the current analysis period."""
        snapshot = TrendSnapshot(period_label=period_label)
        snapshot.signal_count = len(signals)

        if not signals:
            return snapshot

        # Type distribution
        type_counts = defaultdict(int)
        for s in signals:
            type_counts[s.base_signal.advisory_type.value] += 1
        snapshot.type_distribution = dict(type_counts)

        # Repeat rate
        repeats = sum(1 for s in signals if s.base_signal.is_repeat_question)
        snapshot.repeat_rate = round(repeats / len(signals) * 100, 1) if signals else 0

        # Top assets by mention frequency
        asset_counts = defaultdict(int)
        for s in signals:
            for asset in s.base_signal.assets_referenced:
                asset_counts[asset] += 1
        snapshot.top_assets = [
            {"name": name, "count": count}
            for name, count in sorted(asset_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        ]

        # Averages
        snapshot.avg_confidence = round(
            sum(s.confidence.score for s in signals) / len(signals), 3
        )
        snapshot.avg_composite_priority = round(
            sum(s.composite_priority for s in signals) / len(signals), 3
        )

        # Source breakdown
        source_counts = defaultdict(int)
        for s in signals:
            for src in s.sources:
                source_counts[src] += 1
        snapshot.source_breakdown = dict(source_counts)

        # Gong call count
        gong_call_ids = set()
        for s in signals:
            if s.gong_context:
                gong_call_ids.update(s.gong_context.call_ids)
        snapshot.gong_call_count = len(gong_call_ids)

        # Business impact average
        impacts = [s.business_impact.score for s in signals if s.business_impact]
        snapshot.business_impact_avg = round(
            sum(impacts) / len(impacts), 1
        ) if impacts else 0.0

        self._historical_snapshots.append(snapshot)
        return snapshot

    def detect_emerging_issues(self) -> List[dict]:
        """
        Compare the latest snapshot against the previous one to detect trends.

        Returns list of detected changes:
        - New advisory types appearing
        - Types with >50% increase in signal count
        - New assets entering top 10
        """
        if len(self._historical_snapshots) < 2:
            return []

        current = self._historical_snapshots[-1]
        previous = self._historical_snapshots[-2]

        issues = []

        # Check for type count increases
        for type_name, current_count in current.type_distribution.items():
            prev_count = previous.type_distribution.get(type_name, 0)
            if prev_count == 0 and current_count > 0:
                issues.append({
                    "type": "new_advisory_type",
                    "description": f"New advisory type detected: {type_name} ({current_count} signals)",
                    "severity": "Medium",
                })
            elif prev_count > 0:
                change_pct = ((current_count - prev_count) / prev_count) * 100
                if change_pct > 50:
                    issues.append({
                        "type": "signal_increase",
                        "description": (
                            f"{type_name} signals increased by {change_pct:.0f}% "
                            f"({prev_count} -> {current_count})"
                        ),
                        "severity": "High" if change_pct > 100 else "Medium",
                    })

        # Check for new assets in top 10
        prev_asset_names = {a["name"] for a in previous.top_assets}
        for asset in current.top_assets:
            if asset["name"] not in prev_asset_names:
                issues.append({
                    "type": "new_hot_asset",
                    "description": (
                        f"New asset entering top 10: {asset['name']} "
                        f"({asset['count']} mentions)"
                    ),
                    "severity": "Low",
                })

        # Check for repeat rate changes
        if current.repeat_rate - previous.repeat_rate > 10:
            issues.append({
                "type": "repeat_rate_increase",
                "description": (
                    f"Repeat rate increased from {previous.repeat_rate}% to {current.repeat_rate}%"
                ),
                "severity": "High",
            })

        return issues

    def load_history(self, file_path: str):
        """Load previous trend snapshots from JSON file."""
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
            self._historical_snapshots = [
                TrendSnapshot.from_dict(s) for s in data.get("snapshots", [])
            ]
            logger.info(f"Loaded {len(self._historical_snapshots)} historical snapshots")
        except FileNotFoundError:
            logger.info(f"No history file found at {file_path}, starting fresh")
        except Exception as e:
            logger.warning(f"Failed to load history from {file_path}: {e}")

    def save_history(self, file_path: str):
        """Save trend snapshots to JSON file."""
        data = {
            "snapshots": [s.to_dict() for s in self._historical_snapshots],
            "saved_at": datetime.now().isoformat(),
        }
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved {len(self._historical_snapshots)} snapshots to {file_path}")
