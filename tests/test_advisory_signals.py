"""Tests for the Advisory Signals Classifier."""

import pytest

from advisory_signals import (
    AdvisorySignalClassifier,
    AdvisoryType,
    AdvisoryUrgency,
    format_advisory_report,
)


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
            {
                "thread_id": "T004",
                "timestamp": "2026-01-08T14:00:00Z",
                "user": "analyst_f",
                "user_role": "Marketing Analyst",
                "message": "The campaign_attribution table has weird numbers. Conversions seem inflated.",
                "replies": [
                    {
                        "timestamp": "2026-01-08T14:20:00Z",
                        "user": "lead_g",
                        "user_role": "Marketing Data Lead",
                        "message": "There's a known issue where multi-touch attribution double counts. We're working on a fix. Filter on single_touch_attribution = true for now.",
                    }
                ],
            },
            {
                "thread_id": "T005",
                "timestamp": "2026-01-10T10:00:00Z",
                "user": "analyst_h",
                "user_role": "Analyst",
                "message": "Who owns the customers.dim_customer table?",
                "replies": [
                    {
                        "timestamp": "2026-01-10T10:15:00Z",
                        "user": "engineer_b",
                        "user_role": "Data Engineer",
                        "message": "The CDP team owns dim_customer. @jennifer.lee is the primary contact. File a ticket in their Jira board.",
                    }
                ],
            },
            {
                "thread_id": "T006",
                "timestamp": "2026-01-12T11:00:00Z",
                "user": "analyst_i",
                "user_role": "Analyst",
                "message": "What does conversion_type mean in the events table? Building a funnel report.",
                "replies": [
                    {
                        "timestamp": "2026-01-12T11:10:00Z",
                        "user": "engineer_d",
                        "user_role": "Analytics Engineer",
                        "message": "This is asked so often! 1=signup, 2=trial_start, 3=paid_conversion. We need to document this.",
                    }
                ],
            },
            {
                "thread_id": "T007",
                "timestamp": "2026-01-15T13:00:00Z",
                "user": "scientist_j",
                "user_role": "Data Scientist",
                "message": "How do I interpret the churn_risk_score in ml.customer_predictions?",
                "replies": [
                    {
                        "timestamp": "2026-01-15T13:20:00Z",
                        "user": "scientist_k",
                        "user_role": "Data Scientist",
                        "message": "Score is 0-100. Thresholds: 0-30 = low risk, 31-60 = medium, 61-100 = high risk. Official KPI approved by leadership.",
                    }
                ],
            },
            {
                "thread_id": "T008",
                "timestamp": "2026-01-18T15:00:00Z",
                "user": "analyst_l",
                "user_role": "Analyst",
                "message": "Where does customer_360 dashboard data come from?",
                "replies": [
                    {
                        "timestamp": "2026-01-18T15:30:00Z",
                        "user": "bi_lead_m",
                        "user_role": "BI Lead",
                        "message": "It pulls from dim_customer, fct_orders, fct_campaigns. Joins on customer_id. The Fivetran sync updates every 15 minutes with about 2-hour latency on orders.",
                    }
                ],
            },
        ],
    }


@pytest.fixture
def analyzer(sample_messages):
    return AdvisorySignalClassifier(sample_messages)


class TestAdvisorySignalClassifier:
    def test_analyze_returns_expected_keys(self, analyzer):
        results = analyzer.analyze()
        assert "summary" in results
        assert "advisory_type_distribution" in results
        assert "advisory_patterns" in results
        assert "advisor_workload" in results
        assert "enrichment_opportunities" in results
        assert "signals" in results

    def test_correct_signal_count(self, analyzer):
        results = analyzer.analyze()
        assert results["summary"]["total_advisory_signals"] == 8

    def test_detects_data_guidance(self, analyzer):
        results = analyzer.analyze()
        dist = results["advisory_type_distribution"]
        # T001 should be classified as Data Guidance (deprecated, source of truth, always use)
        data_guidance_signals = [
            s for s in results["signals"]
            if s["advisory_type"] == AdvisoryType.DATA_GUIDANCE.value
        ]
        assert len(data_guidance_signals) >= 1

    def test_detects_definition_clarification(self, analyzer):
        results = analyzer.analyze()
        def_signals = [
            s for s in results["signals"]
            if s["advisory_type"] == AdvisoryType.DEFINITION_CLARIFICATION.value
        ]
        # T002 and T006 are about conversion_type values
        assert len(def_signals) >= 1

    def test_detects_governance_compliance(self, analyzer):
        results = analyzer.analyze()
        gov_signals = [
            s for s in results["signals"]
            if s["advisory_type"] == AdvisoryType.GOVERNANCE_COMPLIANCE.value
        ]
        # T003 is about PII/compliance/restricted access
        assert len(gov_signals) >= 1

    def test_detects_troubleshooting(self, analyzer):
        results = analyzer.analyze()
        trouble_signals = [
            s for s in results["signals"]
            if s["advisory_type"] == AdvisoryType.TROUBLESHOOTING.value
        ]
        # T004 has known issue, double counts, working on fix
        assert len(trouble_signals) >= 1

    def test_detects_ownership_routing(self, analyzer):
        results = analyzer.analyze()
        owner_signals = [
            s for s in results["signals"]
            if s["advisory_type"] == AdvisoryType.OWNERSHIP_ROUTING.value
        ]
        # T005 is about who owns dim_customer
        assert len(owner_signals) >= 1

    def test_detects_metric_interpretation(self, analyzer):
        results = analyzer.analyze()
        metric_signals = [
            s for s in results["signals"]
            if s["advisory_type"] == AdvisoryType.METRIC_INTERPRETATION.value
        ]
        # T007 is about interpreting churn_risk_score thresholds
        assert len(metric_signals) >= 1

    def test_detects_repeat_questions(self, analyzer):
        results = analyzer.analyze()
        repeat_signals = [s for s in results["signals"] if s["is_repeat"]]
        # T006 explicitly says "asked so often!" which is a repeat indicator
        assert len(repeat_signals) >= 1

    def test_high_urgency_for_compliance(self, analyzer):
        results = analyzer.analyze()
        high_urgency = results["high_urgency_signals"]
        # The PII/compliance thread should be high urgency
        assert len(high_urgency) >= 1
        compliance_urgent = [
            s for s in high_urgency
            if s["advisory_type"] == AdvisoryType.GOVERNANCE_COMPLIANCE.value
        ]
        assert len(compliance_urgent) >= 1

    def test_advisor_workload_populated(self, analyzer):
        results = analyzer.analyze()
        workload = results["advisor_workload"]
        assert len(workload) > 0
        # engineer_b responds to 3 threads (T001, T003, T005)
        engineer_b = next((w for w in workload if w["advisor"] == "engineer_b"), None)
        assert engineer_b is not None
        assert engineer_b["signals_handled"] == 3

    def test_enrichment_opportunities_generated(self, analyzer):
        results = analyzer.analyze()
        opportunities = results["enrichment_opportunities"]
        assert len(opportunities) > 0
        # Each opportunity should have recommended actions
        for opp in opportunities:
            assert "recommended_actions" in opp
            assert len(opp["recommended_actions"]) > 0
            assert opp["priority"] in ("High", "Medium", "Low")

    def test_assets_extracted_from_signals(self, analyzer):
        results = analyzer.analyze()
        all_assets = set()
        for signal in results["signals"]:
            all_assets.update(signal.get("assets_referenced", []))
        # Should find common assets mentioned in threads
        assert len(all_assets) > 0

    def test_advisory_patterns_sorted_by_count(self, analyzer):
        results = analyzer.analyze()
        patterns = results["advisory_patterns"]
        counts = [p["signal_count"] for p in patterns]
        assert counts == sorted(counts, reverse=True)


class TestFormatAdvisoryReport:
    def test_report_contains_key_sections(self, analyzer):
        results = analyzer.analyze()
        report = format_advisory_report(results)
        assert "Advisory Signals Report" in report
        assert "Advisory Type Distribution" in report
        assert "Advisory Patterns" in report
        assert "Advisor Workload" in report
        assert "Catalog Enrichment Opportunities" in report

    def test_report_is_valid_markdown(self, analyzer):
        results = analyzer.analyze()
        report = format_advisory_report(results)
        # Should start with a heading
        assert report.startswith("# ")
        # Should have multiple sections
        assert report.count("## ") >= 3


class TestEmptyInput:
    def test_empty_messages(self):
        data = {"channel_name": "test", "date_range": "n/a", "messages": []}
        analyzer = AdvisorySignalClassifier(data)
        results = analyzer.analyze()
        assert results["summary"]["total_advisory_signals"] == 0
        assert results["advisory_patterns"] == []

    def test_messages_without_replies(self):
        data = {
            "channel_name": "test",
            "date_range": "n/a",
            "messages": [
                {
                    "thread_id": "T001",
                    "timestamp": "2026-01-01T10:00:00Z",
                    "user": "user_a",
                    "message": "What table should I use?",
                    "replies": [],
                }
            ],
        }
        analyzer = AdvisorySignalClassifier(data)
        results = analyzer.analyze()
        # No replies means no advisory signals
        assert results["summary"]["total_advisory_signals"] == 0
