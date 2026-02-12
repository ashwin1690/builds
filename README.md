# Atlan Metadata Enrichment Toolkit

A comprehensive toolkit for enriching your Atlan data catalog with deep contextual metadata from multiple sources.

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Streamlit](https://img.shields.io/badge/streamlit-1.28+-red.svg)

## Overview

This toolkit provides two powerful modules for metadata enrichment:

### 1. Slack Metadata Gap Analyzer

Analyzes Slack conversations to identify patterns in metadata demand and extract tribal knowledge that should live in your data catalog.

**Key Features:**
- Identify priority assets needing metadata curation
- Extract reusable context from Slack answers (descriptions, ownership, quality notes)
- Surface systemic gaps in metadata coverage
- Generate recommendations for Atlan's enrichment agents

### 2. Tableau Deep Metadata Parser (Phase 1A - NEW!)

Extracts deep contextual metadata from Tableau workbooks (.twb/.twbx) that traditional catalog tools like Atlan often miss.

**Key Features:**
- Extract calculated field formulas and LOD expressions
- Parse layout container hierarchies and zone organization
- Extract filter configurations and dashboard actions
- Capture Data Stories narratives and annotations
- JSON-LD compatible output for semantic integration

---

## Module 1: Slack Metadata Gap Analyzer

## Quick Start

### Option 1: Web Interface (Recommended)

```bash
# Install dependencies
pip install -r requirements.txt

# Set your Slack token
export SLACK_BOT_TOKEN='xoxb-your-token-here'

# Launch the web app
streamlit run src/web_app.py
```

Then open http://localhost:8501 in your browser.

### Option 2: Command Line

```bash
# Install dependencies
pip install -r requirements.txt

# Set your Slack token
export SLACK_BOT_TOKEN='xoxb-your-token-here'

# Analyze a channel
python src/app.py analyze --channel data-questions

# Or run in interactive mode
python src/app.py
```

### Option 3: Analyze from JSON File

```bash
# Analyze exported Slack messages
python src/app.py analyze-file --input data/sample_slack_messages.json
```

## Setup

### 1. Create a Slack App

1. Go to [https://api.slack.com/apps](https://api.slack.com/apps)
2. Click "Create New App" → "From scratch"
3. Name your app (e.g., "Metadata Analyzer") and select your workspace

### 2. Configure Bot Permissions

Navigate to **OAuth & Permissions** and add these Bot Token Scopes:

| Scope | Purpose |
|-------|---------|
| `channels:history` | Read messages from public channels |
| `channels:read` | List public channels |
| `groups:history` | Read messages from private channels (optional) |
| `groups:read` | List private channels (optional) |
| `users:read` | Get user information for attribution |

### 3. Install and Get Token

1. Click "Install to Workspace" and authorize
2. Copy the **Bot User OAuth Token** (starts with `xoxb-`)
3. Set it as an environment variable:

```bash
export SLACK_BOT_TOKEN='xoxb-your-token-here'
```

### 4. Invite Bot to Channel

In Slack, invite your bot to the channel you want to analyze:
```
/invite @your-bot-name
```

## Usage

### Web Interface

The web interface provides an interactive dashboard for analysis:

```bash
streamlit run src/web_app.py
```

**Features:**
- Enter channel name and click "Run Analysis"
- View priority assets with demand signals
- See question type distribution charts
- Download JSON and Markdown reports
- Upload JSON files for offline analysis

### CLI Commands

```bash
# Analyze a Slack channel (last 90 days)
python src/app.py analyze --channel data-questions

# Analyze with custom options
python src/app.py analyze \
  --channel data-questions \
  --days 60 \
  --output ./my-reports \
  --limit 1000

# Test your Slack connection
python src/app.py test-connection

# Analyze from a JSON file
python src/app.py analyze-file --input data/messages.json

# Interactive mode (prompts for inputs)
python src/app.py
```

### CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--channel`, `-c` | Slack channel name | Required |
| `--days`, `-d` | Days to look back | 90 |
| `--output`, `-o` | Output directory | ./reports |
| `--limit`, `-l` | Max messages to analyze | 500 |

## Output

### Generated Reports

Each analysis generates three files:

1. **`{channel}_analysis_{timestamp}.json`** - Machine-readable results for Atlan APIs
2. **`{channel}_analysis_{timestamp}.md`** - Human-readable markdown report
3. **`{channel}_messages_{timestamp}.json`** - Raw messages for reference

### Report Contents

- **Executive Summary**: Threads analyzed, assets identified
- **Priority Assets**: Ranked by demand signals with extracted context
- **Question Type Distribution**: Breakdown of question categories
- **Metadata Gaps**: Systemic issues causing repeated questions
- **Agent Recommendations**: Actions for Atlan enrichment agents

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

### Priority Scoring (1-10)

Assets are scored based on:
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

| Agent | Purpose | Example |
|-------|---------|---------|
| **Description Agent** | Apply extracted definitions | "revenue_daily_v2 contains daily revenue with refunds..." |
| **Ownership Agent** | Link identified owners | "Finance Data Team owns revenue tables" |
| **Quality Context Agent** | Document quality caveats | "Multi-touch attribution has double-counting issue" |
| **Glossary Linkage Agent** | Connect business terms | "MRR", "DAU", "Churn" |

## Sample Analysis

Run the sample data to see example output:

```bash
python src/app.py analyze-file --input data/sample_slack_messages.json
```

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
├── requirements.txt
├── .env.example
├── src/
│   ├── __init__.py
│   ├── app.py                      # CLI application
│   ├── web_app.py                  # Streamlit web interface
│   ├── slack_client.py             # Slack API client
│   └── slack_metadata_analyzer.py  # Core analysis engine
├── data/
│   └── sample_slack_messages.json  # Example input data
└── reports/
    ├── analysis_results.json       # JSON output
    └── analysis_report.md          # Markdown report
```

## Integration with Atlan

The JSON output is structured for easy integration with Atlan's APIs:

```python
# Example: Apply descriptions via Atlan API
for asset in results['agent_recommendations']['description_agent']:
    atlan.asset.update(
        qualified_name=asset['asset'],
        description=asset['suggested_description']
    )
```

### Integration Points

1. **Bulk Asset Updates**: Use `priority_assets[].extracted_context` to populate asset metadata
2. **Ownership Assignment**: Use `agent_recommendations.ownership_agent` to link owners
3. **Glossary Linking**: Use `agent_recommendations.glossary_linkage_agent` for term associations
4. **Quality Notes**: Use `agent_recommendations.quality_context_agent` for quality tags

## Extending the Analyzer

### Adding New Question Types

Edit `QUESTION_KEYWORDS` in `slack_metadata_analyzer.py`:

```python
QuestionType.NEW_TYPE: [
    'keyword1', 'keyword2', ...
]
```

### Adding New Context Extraction

Add extraction logic in `_extract_context()` using regex patterns.

### Custom Priority Scoring

Modify `_calculate_priorities()` to adjust weights for your use case.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Channel not found" | Ensure bot is invited to the channel |
| "Invalid token" | Check SLACK_BOT_TOKEN is set correctly |
| "No messages found" | Try increasing `--days` or check channel has data questions |
| Rate limiting | The tool handles this automatically with backoff |

---

## Module 2: Tableau Deep Metadata Parser

A Python library for extracting deep contextual metadata from Tableau workbooks (.twb/.twbx) that traditional catalog tools miss.

### What Makes This Different

Traditional catalog tools like Atlan extract basic metadata from Tableau (table names, field names, data sources). This parser extracts the **contextual metadata** that makes Tableau powerful:

| Traditional Tools | Tableau Deep Parser |
|------------------|---------------------|
| Field names | Calculated field formulas |
| Basic lineage | LOD expression scope & type |
| Dashboard exists | Layout hierarchy with titles |
| Filter exists | Filter type, values, conditions |
| - | Dashboard action configurations |
| - | Data Story narratives |
| - | Zone organization |

### Quick Start

```python
from twb_parser import TableauWorkbookParser

# Initialize parser
parser = TableauWorkbookParser()

# Parse a workbook (supports both .twb and .twbx)
metadata = parser.parse_file('workbook.twb')

# Access extracted metadata
print(f"Workbook: {metadata.workbook_name}")
print(f"Worksheets: {len(metadata.worksheets)}")
print(f"Dashboards: {len(metadata.dashboards)}")
print(f"Stories: {len(metadata.stories)}")

# Export to JSON-LD
import json
json_ld = metadata.to_json_ld()
with open('metadata.json', 'w') as f:
    json.dump(json_ld, f, indent=2)
```

### Extracted Metadata

The parser extracts:

1. **Calculated Fields & LOD Expressions**
   - Formula text
   - LOD type (FIXED, INCLUDE, EXCLUDE)
   - Scope fields
   - Comments and captions

2. **Layout Containers**
   - Dashboard layout hierarchy
   - Container types and titles
   - Position and size information
   - Nested container relationships

3. **Zone Hierarchies**
   - Worksheet zone organization
   - Rows, columns, filters, pages shelves
   - Marks card organization
   - Field placement

4. **Filter Configurations**
   - Filter types (categorical, quantitative, date, relative date)
   - Filter values and conditions
   - Global vs local filters
   - Customization settings

5. **Dashboard Actions**
   - Action types (filter, highlight, URL, navigation, parameter)
   - Source and target sheets
   - Field mappings
   - URL templates

6. **Data Stories**
   - Story points with captions
   - Narrative text and annotations
   - Worksheet/dashboard references
   - Sequential ordering

### Usage Examples

See `examples/parse_tableau_workbook.py` for comprehensive examples including:
- Extracting calculated fields and LOD expressions
- Navigating layout hierarchies
- Analyzing dashboard actions
- Extracting Data Story narratives
- Exporting to JSON-LD format

Run the example:
```bash
python examples/parse_tableau_workbook.py
```

### Integration with Atlan

The extracted metadata can enrich your Atlan catalog:

```python
from pyatlan.client.atlan import AtlanClient
from twb_parser import TableauWorkbookParser

# Parse Tableau workbook
parser = TableauWorkbookParser()
metadata = parser.parse_file('workbook.twb')

# Initialize Atlan client
client = AtlanClient()

# Enrich calculated fields
for worksheet in metadata.worksheets:
    for calc_field in worksheet.calculated_fields:
        # Update Atlan asset with formula and LOD info
        client.asset.update_custom_metadata(
            qualified_name=f"{metadata.workbook_name}/{worksheet.name}/{calc_field.name}",
            attributes={
                "formula": calc_field.formula,
                "is_lod": calc_field.is_lod,
                "lod_type": calc_field.lod_type.value if calc_field.is_lod else None
            }
        )
```

### Running Tests

```bash
# Run all tests
python -m pytest tests/twb_parser/

# Run with coverage
python -m pytest --cov=twb_parser tests/twb_parser/

# Run specific test file
python -m pytest tests/twb_parser/test_parser.py
```

### Architecture

```
twb_parser/
├── __init__.py           # Main exports
├── parser.py             # Main parser class
├── schema.py             # JSON-LD compatible schema
└── extractors/           # Specialized extractors
    ├── calc_fields.py    # Calculated fields & LOD
    ├── layout.py         # Layout containers & zones
    ├── filters.py        # Filter configurations
    ├── actions.py        # Dashboard actions
    └── stories.py        # Data Stories narratives
```

See `twb_parser/README.md` for detailed documentation.

### Roadmap: Phase 1B (Coming Soon)

The next phase will add **Tableau Metadata API integration** for server-side metadata:

- Description inheritance tracking
- Quality warnings aggregation
- Pulse metric definitions
- Insight Bundles
- Custom SQL column lineage
- GraphQL query templates

---

## License

MIT
