# Slack Metadata Gap Analyzer for Atlan

A tool that analyzes Slack channel messages to identify patterns in metadata demand, extract contextual information for data assets, and generate actionable recommendations for Atlan's enrichment agents.

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Streamlit](https://img.shields.io/badge/streamlit-1.28+-red.svg)

## Overview

Data teams frequently answer the same questions in Slack about data assets - what tables mean, who owns them, where data comes from, and whether it's reliable. This tribal knowledge represents valuable metadata that should live in your data catalog, not buried in Slack threads.

This analyzer mines those conversations to:
1. **Identify priority assets** needing metadata curation based on demand signals
2. **Extract reusable context** from Slack answers (descriptions, ownership, quality notes)
3. **Surface systemic gaps** in your metadata coverage
4. **Generate recommendations** for Atlan's enrichment agents

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

## License

MIT
