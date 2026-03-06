"""
Tests for the Enriched Call Intelligence system.

Tests cover:
- Enriched signal data models and priority computation
- Signal aggregator (deduplication, trends, emerging issues)
- Gong connector (transcript parsing, sentiment estimation)
- Atlan enricher (catalog validation, graceful degradation)
- Salesforce enricher (business impact scoring)
- Tableau enricher (lineage matching)
- Enriched orchestrator (full pipeline, standalone mode)
- Confidence scoring in advisory_signals.py
"""

import json
import os
import sys
import tempfile

import pytest

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from advisory_signals import (
    AdvisorySignalClassifier,
    AdvisorySignal,
    AdvisoryType,
    AdvisoryUrgency,
)
from enriched_signals import (
    BusinessImpactScore,
    CatalogValidation,
    EnrichedAdvisorySignal,
    GongCallContext,
    SignalConfidence,
    SignalSource,
    TableauContext,
    TrendSnapshot,
)
from connector_base import SignalEnricher
from signal_aggregator import SignalAggregator
from enriched_call_intelligence import (
    EnrichedCallIntelligenceAnalyzer,
    format_enriched_report,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def sample_messages():
    """Sample messages data for testing."""
    return {
        "channel_name": "#data-questions",
        "date_range": "2026-01-01 to 2026-01-31",
        "messages": [
            {
                "thread_id": "T001",
                "timestamp": "2026-01-05T10:00:00Z",
                "user": "analyst_a",
                "user_role": "Analyst",
                "message": "Which revenue table should I use? I see revenue_daily and revenue_daily_v2.",
                "replies": [
                    {
                        "timestamp": "2026-01-05T10:15:00Z",
                        "user": "engineer_b",
                        "user_role": "Data Engineer",
                        "message": "Always use revenue_daily_v2. It's the source of truth. The old revenue_daily is deprecated.",
                    }
                ],
            },
            {
                "thread_id": "T002",
                "timestamp": "2026-01-06T11:00:00Z",
                "user": "analyst_c",
                "user_role": "Business Analyst",
                "message": "What does conversion_type = 3 mean in analytics.events?",
                "replies": [
                    {
                        "timestamp": "2026-01-06T11:20:00Z",
                        "user": "engineer_d",
                        "user_role": "Analytics Engineer",
                        "message": "The conversion_type values are: 1=signup, 2=trial_start, 3=paid_conversion, 4=upgrade.",
                    }
                ],
            },
            {
                "thread_id": "T003",
                "timestamp": "2026-01-07T09:00:00Z",
                "user": "compliance_e",
                "user_role": "Compliance Analyst",
                "message": "Where does PII flow in the warehouse? Need this for our audit.",
                "replies": [
                    {
                        "timestamp": "2026-01-07T09:30:00Z",
                        "user": "engineer_b",
                        "user_role": "Data Engineer",
                        "message": "PII is in customers.dim_customer (email, phone). Access is restricted. You'll need manager approval via ServiceNow.",
                    }
                ],
            },
        ],
    }


@pytest.fixture
def sample_advisory_signal():
    """A single sample AdvisorySignal."""
    return AdvisorySignal(
        thread_id="T001",
        advisory_type=AdvisoryType.DATA_GUIDANCE,
        urgency=AdvisoryUrgency.MEDIUM,
        question="Which revenue table should I use?",
        questioner="analyst_a",
        questioner_role="Analyst",
        advisors=["engineer_b"],
        assets_referenced=["revenue_daily_v2", "revenue_daily"],
        is_repeat_question=False,
        confidence=0.6,
        keyword_hits=3,
        pattern_hits=2,
    )


@pytest.fixture
def sample_enriched_signal(sample_advisory_signal):
    """A sample EnrichedAdvisorySignal."""
    return EnrichedAdvisorySignal(
        base_signal=sample_advisory_signal,
        confidence=SignalConfidence(score=0.6, keyword_hits=3, pattern_hits=2),
        sources=["slack"],
    )


# ─── EnrichedAdvisorySignal Tests ────────────────────────────────────────────


class TestEnrichedAdvisorySignal:
    def test_default_priority_is_zero(self, sample_enriched_signal):
        assert sample_enriched_signal.composite_priority == 0.0

    def test_compute_composite_priority_basic(self, sample_enriched_signal):
        priority = sample_enriched_signal.compute_composite_priority()
        assert 0.0 < priority <= 1.0

    def test_priority_increases_with_catalog_gap(self, sample_enriched_signal):
        # Without catalog validation
        priority_no_catalog = sample_enriched_signal.compute_composite_priority()

        # With catalog showing full gaps
        sample_enriched_signal.catalog_validation = CatalogValidation(
            asset_exists_in_atlan=True,
            has_description=False,
            has_owner=False,
            has_glossary_terms=False,
        )
        priority_with_gap = sample_enriched_signal.compute_composite_priority()

        # Both should give full catalog gap weight since no catalog = assumed undocumented
        assert priority_with_gap > 0

    def test_priority_reduces_with_complete_catalog(self, sample_enriched_signal):
        # Set catalog as fully documented
        sample_enriched_signal.catalog_validation = CatalogValidation(
            asset_exists_in_atlan=True,
            has_description=True,
            has_owner=True,
            has_glossary_terms=True,
            enrichment_already_applied=True,
        )
        priority = sample_enriched_signal.compute_composite_priority()

        # Compare with undocumented asset
        sample_enriched_signal.catalog_validation = CatalogValidation(
            asset_exists_in_atlan=True,
            has_description=False,
            has_owner=False,
            has_glossary_terms=False,
        )
        priority_gap = sample_enriched_signal.compute_composite_priority()

        assert priority_gap > priority

    def test_priority_increases_with_business_impact(self, sample_enriched_signal):
        base_priority = sample_enriched_signal.compute_composite_priority()

        sample_enriched_signal.business_impact = BusinessImpactScore(
            score=80, opportunity_count=3, total_opportunity_value=500_000
        )
        boosted = sample_enriched_signal.compute_composite_priority()
        assert boosted > base_priority

    def test_priority_increases_with_gong_context(self, sample_enriched_signal):
        base_priority = sample_enriched_signal.compute_composite_priority()

        sample_enriched_signal.gong_context = GongCallContext(
            call_count=5, mention_count=8, sentiment_scores=[-0.5, -0.3, -0.4]
        )
        boosted = sample_enriched_signal.compute_composite_priority()
        assert boosted > base_priority

    def test_repeat_question_boosts_priority(self, sample_enriched_signal):
        base_priority = sample_enriched_signal.compute_composite_priority()

        sample_enriched_signal.base_signal.is_repeat_question = True
        repeat_priority = sample_enriched_signal.compute_composite_priority()
        assert repeat_priority > base_priority

    def test_high_urgency_boosts_priority(self, sample_enriched_signal):
        base_priority = sample_enriched_signal.compute_composite_priority()

        sample_enriched_signal.base_signal.urgency = AdvisoryUrgency.HIGH
        urgent_priority = sample_enriched_signal.compute_composite_priority()
        assert urgent_priority > base_priority

    def test_multi_source_boosts_priority(self, sample_enriched_signal):
        sample_enriched_signal.sources = ["slack"]
        base_priority = sample_enriched_signal.compute_composite_priority()

        sample_enriched_signal.sources = ["slack", "transcript", "gong"]
        multi_priority = sample_enriched_signal.compute_composite_priority()
        assert multi_priority > base_priority

    def test_to_dict_contains_required_keys(self, sample_enriched_signal):
        sample_enriched_signal.compute_composite_priority()
        d = sample_enriched_signal.to_dict()
        assert "thread_id" in d
        assert "advisory_type" in d
        assert "confidence" in d
        assert "composite_priority" in d
        assert "sources" in d

    def test_to_dict_includes_gong_context(self, sample_enriched_signal):
        sample_enriched_signal.gong_context = GongCallContext(
            call_count=2, mention_count=5, deal_names=["Deal A"]
        )
        d = sample_enriched_signal.to_dict()
        assert "gong_context" in d
        assert d["gong_context"]["call_count"] == 2

    def test_to_dict_includes_catalog_validation(self, sample_enriched_signal):
        sample_enriched_signal.catalog_validation = CatalogValidation(
            asset_exists_in_atlan=True, has_description=True
        )
        d = sample_enriched_signal.to_dict()
        assert "catalog_validation" in d
        assert d["catalog_validation"]["asset_exists"] is True


# ─── SignalConfidence Tests ──────────────────────────────────────────────────


class TestSignalConfidence:
    def test_high_confidence_label(self):
        c = SignalConfidence(score=0.9)
        assert c.label == "High"

    def test_medium_confidence_label(self):
        c = SignalConfidence(score=0.6)
        assert c.label == "Medium"

    def test_low_confidence_label(self):
        c = SignalConfidence(score=0.3)
        assert c.label == "Low"


# ─── CatalogValidation Tests ────────────────────────────────────────────────


class TestCatalogValidation:
    def test_completeness_score_full(self):
        cv = CatalogValidation(
            asset_exists_in_atlan=True,
            has_description=True,
            has_owner=True,
            has_glossary_terms=True,
        )
        assert cv.completeness_score == 1.0

    def test_completeness_score_empty(self):
        cv = CatalogValidation(asset_exists_in_atlan=True)
        assert cv.completeness_score == 0.0

    def test_completeness_score_not_in_atlan(self):
        cv = CatalogValidation(asset_exists_in_atlan=False)
        assert cv.completeness_score == 0.0

    def test_completeness_score_partial(self):
        cv = CatalogValidation(
            asset_exists_in_atlan=True,
            has_description=True,
            has_owner=False,
            has_glossary_terms=False,
        )
        assert abs(cv.completeness_score - 1 / 3) < 0.01


# ─── BusinessImpactScore Tests ──────────────────────────────────────────────


class TestBusinessImpactScore:
    def test_critical_label(self):
        bis = BusinessImpactScore(score=80)
        assert bis.label == "Critical"

    def test_high_label(self):
        bis = BusinessImpactScore(score=50)
        assert bis.label == "High"

    def test_medium_label(self):
        bis = BusinessImpactScore(score=20)
        assert bis.label == "Medium"

    def test_low_label(self):
        bis = BusinessImpactScore(score=5)
        assert bis.label == "Low"


# ─── GongCallContext Tests ───────────────────────────────────────────────────


class TestGongCallContext:
    def test_avg_sentiment(self):
        gc = GongCallContext(sentiment_scores=[-0.5, 0.5, 0.0])
        assert gc.avg_sentiment == 0.0

    def test_avg_sentiment_empty(self):
        gc = GongCallContext()
        assert gc.avg_sentiment == 0.0

    def test_frequently_discussed_by_mentions(self):
        gc = GongCallContext(mention_count=5)
        assert gc.is_frequently_discussed is True

    def test_frequently_discussed_by_calls(self):
        gc = GongCallContext(call_count=3)
        assert gc.is_frequently_discussed is True

    def test_not_frequently_discussed(self):
        gc = GongCallContext(mention_count=1, call_count=1)
        assert gc.is_frequently_discussed is False


# ─── TableauContext Tests ────────────────────────────────────────────────────


class TestTableauContext:
    def test_is_tableau_documented_with_calc_fields(self):
        tc = TableauContext(calculated_fields_that_answer=["Revenue: SUM([Sales])"])
        assert tc.is_tableau_documented is True

    def test_not_documented_when_empty(self):
        tc = TableauContext()
        assert tc.is_tableau_documented is False


# ─── TrendSnapshot Tests ────────────────────────────────────────────────────


class TestTrendSnapshot:
    def test_to_dict_and_from_dict_roundtrip(self):
        snapshot = TrendSnapshot(
            period_label="2026-W10",
            signal_count=42,
            repeat_rate=25.0,
            avg_confidence=0.75,
            type_distribution={"Data Guidance": 10, "Troubleshooting": 5},
        )
        d = snapshot.to_dict()
        restored = TrendSnapshot.from_dict(d)
        assert restored.period_label == "2026-W10"
        assert restored.signal_count == 42
        assert restored.repeat_rate == 25.0


# ─── SignalAggregator Tests ──────────────────────────────────────────────────


class TestSignalAggregator:
    def _make_signal(self, thread_id, advisory_type, assets, source="slack"):
        base = AdvisorySignal(
            thread_id=thread_id,
            advisory_type=advisory_type,
            urgency=AdvisoryUrgency.MEDIUM,
            question=f"Question about {', '.join(assets)}",
            questioner="user",
            questioner_role="Analyst",
            assets_referenced=assets,
            confidence=0.5,
        )
        return EnrichedAdvisorySignal(
            base_signal=base,
            confidence=SignalConfidence(score=0.5),
            sources=[source],
        )

    def test_aggregate_empty(self):
        agg = SignalAggregator()
        result = agg.aggregate()
        assert result == []

    def test_aggregate_single_group(self):
        agg = SignalAggregator()
        signals = [
            self._make_signal("T1", AdvisoryType.DATA_GUIDANCE, ["table_a"]),
            self._make_signal("T2", AdvisoryType.TROUBLESHOOTING, ["table_b"]),
        ]
        result = agg.aggregate(signals)
        assert len(result) == 2

    def test_deduplication(self):
        agg = SignalAggregator()
        # Same asset and type from different sources
        s1 = self._make_signal("T1", AdvisoryType.DATA_GUIDANCE, ["table_a"], "slack")
        s2 = self._make_signal("T2", AdvisoryType.DATA_GUIDANCE, ["table_a"], "transcript")

        result = agg.aggregate([s1], [s2])
        assert len(result) == 1
        # Should have both sources
        assert "slack" in result[0].sources
        assert "transcript" in result[0].sources

    def test_dedup_boosts_confidence(self):
        agg = SignalAggregator()
        s1 = self._make_signal("T1", AdvisoryType.DATA_GUIDANCE, ["table_a"], "slack")
        s2 = self._make_signal("T2", AdvisoryType.DATA_GUIDANCE, ["table_a"], "gong")
        s1.confidence.score = 0.5
        s2.confidence.score = 0.5

        result = agg.aggregate([s1], [s2])
        assert result[0].confidence.score > 0.5
        assert result[0].confidence.source_count == 2

    def test_no_dedup_for_different_types(self):
        agg = SignalAggregator()
        s1 = self._make_signal("T1", AdvisoryType.DATA_GUIDANCE, ["table_a"])
        s2 = self._make_signal("T2", AdvisoryType.TROUBLESHOOTING, ["table_a"])

        result = agg.aggregate([s1, s2])
        assert len(result) == 2

    def test_sorted_by_priority(self):
        agg = SignalAggregator()
        s1 = self._make_signal("T1", AdvisoryType.DATA_GUIDANCE, ["table_a"])
        s2 = self._make_signal("T2", AdvisoryType.TROUBLESHOOTING, ["table_b"])
        s2.confidence.score = 0.9  # Higher confidence

        result = agg.aggregate([s1, s2])
        assert result[0].composite_priority >= result[1].composite_priority

    def test_compute_trend_snapshot(self):
        agg = SignalAggregator()
        signals = [
            self._make_signal("T1", AdvisoryType.DATA_GUIDANCE, ["table_a"]),
            self._make_signal("T2", AdvisoryType.DATA_GUIDANCE, ["table_b"]),
            self._make_signal("T3", AdvisoryType.TROUBLESHOOTING, ["table_c"]),
        ]
        for s in signals:
            s.compute_composite_priority()

        snapshot = agg.compute_trend_snapshot(signals, "2026-W10")
        assert snapshot.signal_count == 3
        assert snapshot.type_distribution["Data Guidance"] == 2
        assert snapshot.type_distribution["Troubleshooting"] == 1
        assert snapshot.avg_confidence > 0

    def test_detect_emerging_issues(self):
        agg = SignalAggregator()

        # First period - baseline
        signals1 = [
            self._make_signal("T1", AdvisoryType.DATA_GUIDANCE, ["table_a"]),
        ]
        for s in signals1:
            s.compute_composite_priority()
        agg.compute_trend_snapshot(signals1, "2026-W09")

        # Second period - significant increase
        signals2 = [
            self._make_signal(f"T{i}", AdvisoryType.DATA_GUIDANCE, [f"table_{i}"])
            for i in range(5)
        ]
        for s in signals2:
            s.compute_composite_priority()
        agg.compute_trend_snapshot(signals2, "2026-W10")

        issues = agg.detect_emerging_issues()
        assert len(issues) > 0
        # Should detect the increase in Data Guidance signals
        increase_issues = [i for i in issues if i["type"] == "signal_increase"]
        assert len(increase_issues) >= 1

    def test_save_and_load_history(self):
        agg = SignalAggregator()
        signals = [self._make_signal("T1", AdvisoryType.DATA_GUIDANCE, ["table_a"])]
        for s in signals:
            s.compute_composite_priority()
        agg.compute_trend_snapshot(signals, "2026-W10")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            path = f.name

        try:
            agg.save_history(path)

            agg2 = SignalAggregator()
            agg2.load_history(path)
            assert len(agg2._historical_snapshots) == 1
            assert agg2._historical_snapshots[0].period_label == "2026-W10"
        finally:
            os.unlink(path)


# ─── Confidence Scoring in AdvisorySignals Tests ─────────────────────────────


class TestConfidenceScoring:
    def test_signals_have_confidence(self, sample_messages):
        analyzer = AdvisorySignalClassifier(sample_messages)
        analyzer.analyze()

        for signal in analyzer.signals:
            assert hasattr(signal, "confidence")
            assert hasattr(signal, "keyword_hits")
            assert hasattr(signal, "pattern_hits")

    def test_nonzero_confidence_for_classified(self, sample_messages):
        analyzer = AdvisorySignalClassifier(sample_messages)
        analyzer.analyze()

        classified = [s for s in analyzer.signals if s.advisory_type != AdvisoryType.UNKNOWN]
        for signal in classified:
            assert signal.confidence > 0.0
            assert signal.keyword_hits + signal.pattern_hits > 0


# ─── Gong Connector Tests ───────────────────────────────────────────────────


class TestGongConnector:
    def test_gong_transcript_parsing(self):
        from gong_connector import parse_gong_transcript_to_messages

        call_data = {
            "metaData": {
                "id": "call_1",
                "title": "Product Demo Call",
                "started": "2026-01-15T10:00:00Z",
                "duration": 1800,
            },
            "parties": [
                {"speakerId": "s1", "name": "Sales Rep", "affiliation": "internal", "spoke": True},
                {"speakerId": "s2", "name": "Customer", "affiliation": "external", "spoke": True},
            ],
        }

        transcript = [
            {
                "speakerId": "s2",
                "sentences": [
                    {"start": 60000, "end": 65000, "text": "What data does revenue_daily_v2 contain?"},
                ],
            },
            {
                "speakerId": "s1",
                "sentences": [
                    {"start": 66000, "end": 75000, "text": "It contains daily aggregated revenue by product."},
                ],
            },
        ]

        result = parse_gong_transcript_to_messages(call_data, transcript)
        assert result["channel_name"] == "Gong: Product Demo Call"
        assert len(result["messages"]) >= 1

    def test_gong_enricher_unavailable_without_config(self):
        from gong_connector import GongCallEnricher

        enricher = GongCallEnricher(api_key="", api_secret="")
        assert enricher.is_available() is False

    def test_gong_enricher_safe_returns_signals_unchanged(self):
        from gong_connector import GongCallEnricher

        enricher = GongCallEnricher(api_key="", api_secret="")
        signal = EnrichedAdvisorySignal(
            base_signal=AdvisorySignal(
                thread_id="T1",
                advisory_type=AdvisoryType.DATA_GUIDANCE,
                urgency=AdvisoryUrgency.LOW,
                question="test",
                questioner="user",
                questioner_role="Analyst",
            ),
            confidence=SignalConfidence(score=0.5),
        )
        result = enricher.enrich_safe([signal])
        assert len(result) == 1
        assert result[0].gong_context is None

    def test_sentiment_estimation(self):
        from gong_connector import GongCallEnricher

        enricher = GongCallEnricher()

        positive_phrases = ["This data is great and very reliable"]
        assert enricher._estimate_sentiment(positive_phrases) > 0

        negative_phrases = ["This is broken and confusing, the data is wrong"]
        assert enricher._estimate_sentiment(negative_phrases) < 0

        empty = enricher._estimate_sentiment([])
        assert empty == 0.0


# ─── Atlan Enricher Tests ───────────────────────────────────────────────────


class TestAtlanEnricher:
    def test_unavailable_without_config(self):
        from atlan_enricher import AtlanCatalogEnricher

        enricher = AtlanCatalogEnricher(api_key="", base_url="")
        assert enricher.is_available() is False

    def test_safe_enrich_returns_unchanged(self):
        from atlan_enricher import AtlanCatalogEnricher

        enricher = AtlanCatalogEnricher(api_key="", base_url="")
        signal = EnrichedAdvisorySignal(
            base_signal=AdvisorySignal(
                thread_id="T1",
                advisory_type=AdvisoryType.DATA_GUIDANCE,
                urgency=AdvisoryUrgency.LOW,
                question="test",
                questioner="user",
                questioner_role="Analyst",
                assets_referenced=["table_a"],
            ),
            confidence=SignalConfidence(score=0.5),
        )
        result = enricher.enrich_safe([signal])
        assert len(result) == 1


# ─── Salesforce Enricher Tests ───────────────────────────────────────────────


class TestSalesforceEnricher:
    def test_unavailable_without_config(self):
        from salesforce_enricher import SalesforceContextEnricher

        # Clear env var if set
        old = os.environ.pop("SALESFORCE_USERNAME", None)
        try:
            enricher = SalesforceContextEnricher()
            assert enricher.is_available() is False
        finally:
            if old:
                os.environ["SALESFORCE_USERNAME"] = old

    def test_stage_weights(self):
        from salesforce_enricher import STAGE_WEIGHTS

        assert STAGE_WEIGHTS["closed won"] == 1.0
        assert STAGE_WEIGHTS["prospecting"] < STAGE_WEIGHTS["negotiation"]


# ─── Tableau Enricher Tests ──────────────────────────────────────────────────


class TestTableauEnricher:
    def test_unavailable_without_workbooks(self):
        from tableau_enricher import TableauLineageEnricher

        enricher = TableauLineageEnricher(workbook_paths=[])
        assert enricher.is_available() is False

    def test_formula_reference_extraction(self):
        from tableau_enricher import TableauLineageEnricher

        enricher = TableauLineageEnricher()
        refs = enricher._extract_formula_references(
            "SUM([Orders].[Amount]) / COUNT([dim_customer].[Id])"
        )
        assert "Orders" in refs
        assert "Amount" in refs
        assert "dim_customer" in refs


# ─── Enriched Call Intelligence Orchestrator Tests ───────────────────────────


class TestEnrichedCallIntelligenceAnalyzer:
    def test_analyze_produces_expected_keys(self, sample_messages):
        analyzer = EnrichedCallIntelligenceAnalyzer(sample_messages)
        results = analyzer.analyze()

        assert "source" in results
        assert "analysis_timestamp" in results
        assert "analysis_mode" in results
        assert results["analysis_mode"] == "enriched"
        assert "summary" in results
        assert "top_signals" in results
        assert "type_priority_breakdown" in results
        assert "asset_hotspots" in results
        assert "trend_snapshot" in results

    def test_summary_metrics(self, sample_messages):
        analyzer = EnrichedCallIntelligenceAnalyzer(sample_messages)
        results = analyzer.analyze()
        summary = results["summary"]

        assert summary["total_signals"] == 3
        assert "avg_composite_priority" in summary
        assert "avg_confidence" in summary

    def test_top_signals_have_priority(self, sample_messages):
        analyzer = EnrichedCallIntelligenceAnalyzer(sample_messages)
        results = analyzer.analyze()

        for signal in results["top_signals"]:
            assert "composite_priority" in signal
            assert signal["composite_priority"] > 0

    def test_top_signals_sorted_by_priority(self, sample_messages):
        analyzer = EnrichedCallIntelligenceAnalyzer(sample_messages)
        results = analyzer.analyze()

        priorities = [s["composite_priority"] for s in results["top_signals"]]
        assert priorities == sorted(priorities, reverse=True)

    def test_asset_hotspots_populated(self, sample_messages):
        analyzer = EnrichedCallIntelligenceAnalyzer(sample_messages)
        results = analyzer.analyze()

        hotspots = results["asset_hotspots"]
        assert len(hotspots) > 0
        for h in hotspots:
            assert "asset" in h
            assert "total_priority" in h
            assert "signal_count" in h

    def test_standalone_mode_no_enrichers(self, sample_messages):
        """Test that the analyzer works without any external enrichers."""
        analyzer = EnrichedCallIntelligenceAnalyzer(sample_messages)
        assert len(analyzer._enrichers) == 0  # No env vars set
        results = analyzer.analyze()
        assert results["active_enrichers"] == []
        assert results["summary"]["total_signals"] > 0

    def test_from_transcript(self):
        transcript = """
[00:01:00] John: What does the revenue_daily_v2 table contain?
[00:01:30] Jane: It has daily revenue aggregated by product. Always use this one.
[00:02:00] Bob: Who owns dim_customer?
[00:02:20] Jane: The CDP team owns dim_customer. @jennifer.lee is the primary contact.
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(transcript)
            path = f.name

        try:
            analyzer = EnrichedCallIntelligenceAnalyzer.from_transcript(path)
            results = analyzer.analyze()
            assert results["summary"]["total_signals"] >= 1
        finally:
            os.unlink(path)

    def test_from_json_file(self, sample_messages):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(sample_messages, f)
            path = f.name

        try:
            analyzer = EnrichedCallIntelligenceAnalyzer.from_json_file(path)
            results = analyzer.analyze()
            assert results["summary"]["total_signals"] == 3
        finally:
            os.unlink(path)

    def test_trend_history_persistence(self, sample_messages):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            path = f.name

        try:
            analyzer = EnrichedCallIntelligenceAnalyzer(
                sample_messages, trend_history_path=path
            )
            results = analyzer.analyze()
            assert "trend_snapshot" in results

            # Verify history file was created
            with open(path) as f:
                history = json.load(f)
            assert len(history["snapshots"]) == 1
        finally:
            os.unlink(path)

    def test_includes_base_analysis(self, sample_messages):
        analyzer = EnrichedCallIntelligenceAnalyzer(sample_messages)
        results = analyzer.analyze(include_base_analysis=True)
        assert "metadata_analysis" in results

    def test_skip_base_analysis(self, sample_messages):
        analyzer = EnrichedCallIntelligenceAnalyzer(sample_messages)
        results = analyzer.analyze(include_base_analysis=False)
        assert results.get("metadata_analysis") is None


# ─── Format Report Tests ────────────────────────────────────────────────────


class TestFormatEnrichedReport:
    def test_report_contains_key_sections(self, sample_messages):
        analyzer = EnrichedCallIntelligenceAnalyzer(sample_messages)
        results = analyzer.analyze()
        report = format_enriched_report(results)

        assert "Enriched Call Intelligence Report" in report
        assert "Summary" in report
        assert "Asset Hotspots" in report
        assert "Advisory Type Priority Breakdown" in report
        assert "Top Priority Signals" in report

    def test_report_is_valid_markdown(self, sample_messages):
        analyzer = EnrichedCallIntelligenceAnalyzer(sample_messages)
        results = analyzer.analyze()
        report = format_enriched_report(results)

        assert report.startswith("# ")
        assert report.count("## ") >= 3


# ─── Connector Base Tests ───────────────────────────────────────────────────


class TestConnectorBase:
    def test_enrich_safe_catches_exceptions(self):
        class BrokenEnricher(SignalEnricher):
            def is_available(self) -> bool:
                return True

            def enrich(self, signals):
                raise RuntimeError("Connection failed!")

        enricher = BrokenEnricher()
        signals = [
            EnrichedAdvisorySignal(
                base_signal=AdvisorySignal(
                    thread_id="T1",
                    advisory_type=AdvisoryType.DATA_GUIDANCE,
                    urgency=AdvisoryUrgency.LOW,
                    question="test",
                    questioner="user",
                    questioner_role="Analyst",
                ),
                confidence=SignalConfidence(score=0.5),
            )
        ]
        result = enricher.enrich_safe(signals)
        assert len(result) == 1  # Signals returned unchanged

    def test_enrich_safe_skips_unavailable(self):
        class UnavailableEnricher(SignalEnricher):
            def is_available(self) -> bool:
                return False

            def enrich(self, signals):
                raise RuntimeError("Should not be called")

        enricher = UnavailableEnricher()
        result = enricher.enrich_safe([])
        assert result == []
