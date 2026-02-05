# Slack Metadata Gap Analyzer for Atlan

A tool that analyzes Slack channel messages to identify patterns in metadata demand, extract contextual information for data assets, and generate actionable recommendations for Atlan's enrichment agents.

## Overview

Data teams frequently answer the same questions in Slack about data assets - what tables mean, who owns them, where data comes from, and whether it's reliable. This tribal knowledge represents valuable metadata that should live in your data catalog, not buried in Slack threads.

This analyzer mines those conversations to:
1. **Identify priority assets** needing metadata curation based on demand signals
2. **Extract reusable context** from Slack answers (descriptions, ownership, quality notes)
3. **Surface systemic gaps** in your metadata coverage
4. **Generate recommendations** for Atlan's enrichment agents

## Quick Start

```bash
# Run analysis on your Slack export
python src/slack_metadata_analyzer.py \
  --input data/your_slack_export.json \
  --output reports/
```

## Input Format

The analyzer expects Slack messages in the following JSON structure:

```json
{
  "channel_name": "#data-questions",
  "date_range": "2025-11-01 to 2026-01-31",
  "messages": [
    {
      "thread_id": "T001",
      "timestamp": "2025-11-05T09:23:00Z",
      "user": "sarah.chen",
      "user_role": "Product Analyst",
      "message": "What does the revenue_daily table contain?",
      "replies": [
        {
          "timestamp": "2025-11-05T09:45:00Z",
          "user": "david.kim",
          "user_role": "Data Engineer",
          "message": "It's our source of truth for daily revenue..."
        }
      ]
    }
  ]
}
```

## Output

The analyzer generates two reports:

### 1. JSON Report (`analysis_results.json`)
Machine-readable output for programmatic use with Atlan APIs.

### 2. Markdown Report (`analysis_report.md`)
Human-readable report with:
- Executive summary
- Priority assets ranked by demand signals
- Extracted context from Slack answers
- Question type distribution
- Identified metadata gaps
- Agent recommendations

## Analysis Framework

### Question Type Classification

| Type | Description | Example |
|------|-------------|---------|
| **Definitional** | What does this field/table mean? | "What does conversion_type = 3 mean?" |
| **Lineage** | Where does this data come from? | "Which tables feed the dashboard?" |
| **Usage** | How should I use this asset? | "Which revenue table should I use?" |
| **Quality** | Is this data reliable? | "Can I trust this table?" |
| **Business Context** | What business process does this support? | "Is this the official KPI?" |
| **Ownership** | Who owns/maintains this? | "Who should I contact about this table?" |
| **Access** | How do I get access? | "How do I request permissions?" |

### Priority Scoring

Assets are scored (1-10) based on:
- Number of questions asked (weighted 1.5x)
- Number of unique questioners (diversity of demand)
- Question complexity (multiple question types = more complex)
- Recurring questions bonus

### Context Extraction

The analyzer mines Slack answers for:
- **Enumeration values**: Extracts coded value mappings (e.g., "1=pending, 2=confirmed")
- **Ownership information**: Teams and individuals mentioned as owners
- **Quality caveats**: Known issues, data gaps, or usage warnings
- **Business context**: Official definitions, KPI designations, reconciliation notes
- **Related terms**: Business glossary candidates

## Agent Recommendations

Based on extracted context, the analyzer recommends actions for:

### Description Agent
Assets with clear definitions provided in Slack that can be automatically populated.

### Ownership Agent
Assets with identified owners that can be linked to teams/individuals in Atlan.

### Quality Context Agent
Assets with quality notes, caveats, or data freshness information.

### Glossary Linkage Agent
Business terms mentioned that should be linked to assets.

## Sample Analysis

See `reports/analysis_report.md` for a complete example analysis of sample data.

### Key Findings from Sample Data

| Metric | Value |
|--------|-------|
| Threads Analyzed | 23 |
| Assets Identified | 24 |
| Top Question Type | Lineage (34.1%) |
| Second Question Type | Definitional (29.5%) |

**Top Priority Assets:**
1. `revenue_daily_v2` (10.0/10) - 5 questions from 5 unique users
2. `dim_customer` (10.0/10) - 6 questions about ownership, lineage, usage
3. `fct_orders` (9.0/10) - Missing enumeration documentation

**Identified Gaps:**
- Missing descriptions on critical tables (High severity)
- Undocumented enumeration values (High severity)
- Versioning/deprecation confusion (Medium severity)

## Project Structure

```
.
├── README.md
├── src/
│   └── slack_metadata_analyzer.py    # Main analysis tool
├── data/
│   └── sample_slack_messages.json    # Example input data
└── reports/
    ├── analysis_results.json         # JSON output
    └── analysis_report.md            # Markdown report
```

## Extending the Analyzer

### Adding New Question Types

Edit the `QUESTION_KEYWORDS` dictionary in `slack_metadata_analyzer.py`:

```python
QuestionType.NEW_TYPE: [
    'keyword1', 'keyword2', ...
]
```

### Adding New Context Extraction

Add new extraction logic in the `_extract_context()` method using regex patterns.

### Custom Priority Scoring

Modify the `_calculate_priorities()` method to adjust weights for your use case.

## Integration with Atlan

The JSON output is structured for easy integration with Atlan's APIs:

1. **Bulk Asset Updates**: Use `priority_assets[].extracted_context` to populate asset metadata
2. **Ownership Assignment**: Use `agent_recommendations.ownership_agent` to link owners
3. **Glossary Linking**: Use `agent_recommendations.glossary_linkage_agent` for term associations
4. **Quality Notes**: Use `agent_recommendations.quality_context_agent` for quality tags

## License

MIT
