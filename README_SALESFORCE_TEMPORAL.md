# Salesforce Temporal Data Extraction Pipeline

A comprehensive Python toolkit for extracting temporal/historical data from Salesforce and standardizing it into MicroDecisionEvent records for integration with Atlan and downstream analytics.

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Pydantic](https://img.shields.io/badge/pydantic-2.0+-green.svg)

## Overview

This pipeline extracts micro-decisions and temporal data from Salesforce, capturing:
- Field history changes across all objects
- Opportunity stage progressions
- Approval process decisions
- User activities (Tasks and Events)
- Setup/configuration changes

All data is standardized into a unified `MicroDecisionEvent` schema for consistent processing and analysis.

## Project Structure

```
salesforce-temporal-extractor/
├── src/
│   └── salesforce_temporal/
│       ├── models/
│       │   └── events.py              # Pydantic event schemas
│       ├── extractors/
│       │   ├── base.py                # Base extractor class
│       │   ├── opportunity_history.py # Opportunity stage changes
│       │   ├── field_history.py       # Generic field history
│       │   ├── approval_history.py    # Approval processes
│       │   ├── activity.py            # Tasks and Events
│       │   └── setup_audit_trail.py   # Config changes
│       ├── exploration/
│       │   ├── explore_salesforce.py  # SF API exploration
│       │   └── explore_atlan.py       # Atlan SDK exploration
│       ├── config/
│       │   └── settings.py            # Configuration management
│       └── utils/
├── tests/
│   ├── models/
│   ├── extractors/
│   └── integration/
├── pyproject.toml
└── README_SALESFORCE_TEMPORAL.md
```

## Installation

### Prerequisites

- Python 3.8 or higher
- Salesforce account with API access
- Atlan account with API key

### Install Dependencies

```bash
# Install the package in development mode
pip install -e .

# Or install with optional dependencies
pip install -e ".[dev]"          # Development tools
pip install -e ".[dbt]"           # DBT support
pip install -e ".[sqlmesh]"       # SQLMesh support
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
# Copy the example file
cp .env.salesforce.example .env
```

Edit `.env` with your credentials:

```bash
# Salesforce Configuration
SALESFORCE_USERNAME=your@email.com
SALESFORCE_PASSWORD=yourpassword
SALESFORCE_SECURITY_TOKEN=yoursecuritytoken
SALESFORCE_DOMAIN=login  # or 'test' for sandbox

# Atlan Configuration
ATLAN_BASE_URL=https://your-tenant.atlan.com
ATLAN_API_KEY=your-api-key

# Extraction Configuration
BULK_API_BATCH_SIZE=10000
MAX_CONCURRENT_REQUESTS=5
INCREMENTAL_LOOKBACK_DAYS=7
SETUP_AUDIT_RETENTION_DAYS=180
```

## Phase 0: Exploration & Validation

Before running extractors, validate your Salesforce and Atlan access:

### 1. Explore Salesforce API

```bash
python -m salesforce_temporal.exploration.explore_salesforce
```

This will:
- Connect to Salesforce
- List all history objects
- Show record counts
- Display sample data
- Save results to `salesforce_exploration_results.json`

### 2. Explore Atlan SDK

```bash
python -m salesforce_temporal.exploration.explore_atlan
```

This will:
- Connect to Atlan
- List Salesforce assets in catalog
- Show custom metadata definitions
- Save results to `atlan_exploration_results.json`

## Phase 1: Data Extraction

### Extract Opportunity History

Highest-value extraction - opportunity stage changes:

```python
from salesforce_temporal.extractors.opportunity_history import OpportunityHistoryExtractor

extractor = OpportunityHistoryExtractor()

# Extract recent changes (last 7 days)
for event in extractor.extract_events(incremental=True):
    print(f"Opportunity {event.record_id}: {event.old_value} → {event.new_value}")

# Get stage duration analytics
stage_summary = extractor.extract_stage_duration_summary()
```

### Extract Generic Field History

Works with any field history tracking object:

```python
from salesforce_temporal.extractors.field_history import (
    FieldHistoryExtractor,
    create_opportunity_field_history_extractor,
    create_account_history_extractor,
)

# Option 1: Use factory functions
extractor = create_opportunity_field_history_extractor()

# Option 2: Create manually
extractor = FieldHistoryExtractor("CaseHistory", "Case")

# Extract all field changes
for event in extractor.extract_events(incremental=True):
    print(f"{event.field_name}: {event.old_value} → {event.new_value}")

# Extract specific field only
for event in extractor.extract_by_field_name("Status"):
    print(f"Status changed: {event.old_value} → {event.new_value}")

# Get list of tracked fields
tracked_fields = extractor.get_tracked_fields()
print(f"Tracked fields: {tracked_fields}")
```

### Extract Approval History

Approval process decisions:

```python
from salesforce_temporal.extractors.approval_history import ApprovalHistoryExtractor

extractor = ApprovalHistoryExtractor()

# Extract approval decisions
for event in extractor.extract_events(incremental=True):
    print(f"Approval: {event.new_value} by {event.actor_id}")
    print(f"Comments: {event.context.approval_comments}")

# Get approval metrics
metrics = extractor.get_approval_metrics()
print(f"Total approvals: {metrics['total_steps']}")
print(f"By status: {metrics['by_status']}")
```

### Extract Activities

Task and Event records:

```python
from salesforce_temporal.extractors.activity import ActivityExtractor

extractor = ActivityExtractor()

# Extract both tasks and events
for event in extractor.extract_events(
    incremental=True,
    extract_tasks=True,
    extract_events=True,
):
    print(f"Activity: {event.context.activity_subject}")
    print(f"Type: {event.context.activity_type}")

# Get activity summary
summary = extractor.get_activity_summary()
print(f"Tasks by status: {summary['tasks_by_status']}")
print(f"Events by type: {summary['events_by_subtype']}")
```

### Extract Setup Audit Trail

Configuration changes (runs on schedule to preserve 180-day history):

```python
from salesforce_temporal.extractors.setup_audit_trail import SetupAuditTrailExtractor

extractor = SetupAuditTrailExtractor()

# Extract all available audit records (up to 180 days)
for event in extractor.extract_events(incremental=False):
    print(f"Setup change: {event.field_name}")
    print(f"Section: {event.record_id}")
    print(f"Details: {event.new_value}")

# Get change summary
summary = extractor.get_change_summary()
print(f"Total changes: {summary['total_changes']}")
print(f"By section: {summary['by_section']}")

# Get recent critical changes
critical = extractor.get_recent_critical_changes(days=7)
for change in critical:
    print(f"Critical: {change['action']} in {change['section']}")
```

## Event Schema

All extractors emit standardized `MicroDecisionEvent` records:

```python
{
    "event_type": "stage_change",           # field_change, stage_change, approval_decision, activity, setup_change
    "timestamp_occurred": "2024-01-15T10:30:00Z",
    "timestamp_recorded": "2024-01-15T10:30:01Z",
    "actor_id": "005xx000001X8Uz",
    "actor_type": "user",                   # user, system, automation, integration
    "record_type": "Opportunity",
    "record_id": "006xx000001X8Uz",
    "field_name": "StageName",
    "old_value": "Prospecting",
    "new_value": "Qualification",
    "context": {
        "source_object": "OpportunityHistory",
        "source_record_id": "001xx000001X8Uz",
        "related_records": {"OpportunityId": "006xx000001X8Uz"},
        "previous_stage": "Prospecting",
        "stage_duration_days": 5.2,
        "metadata": {}
    },
    "extracted_at": "2024-01-15T12:00:00Z",
    "extractor_version": "0.1.0"
}
```

## Output Formats

Events can be exported in multiple formats:

```python
from salesforce_temporal.extractors.opportunity_history import OpportunityHistoryExtractor

extractor = OpportunityHistoryExtractor()

# Export to JSONL
with open("events.jsonl", "w") as f:
    for event in extractor.extract_events():
        f.write(event.to_json() + "\n")

# Export to dictionary (for Pandas/Parquet)
import pandas as pd

events = [event.to_dict() for event in extractor.extract_events()]
df = pd.DataFrame(events)
df.to_parquet("events.parquet")
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=salesforce_temporal --cov-report=html

# Run specific test file
pytest tests/models/test_events.py

# Run specific test class
pytest tests/extractors/test_field_history.py::TestFieldHistoryExtractor
```

## Development

### Code Quality

```bash
# Format code with black
black src/ tests/

# Lint with ruff
ruff check src/ tests/

# Type checking with mypy
mypy src/
```

### Pre-commit Hooks

```bash
# Install pre-commit hooks
pip install pre-commit
pre-commit install

# Run manually
pre-commit run --all-files
```

## Common Patterns

### Incremental Extraction

```python
# Extract only records created in last N days
for event in extractor.extract_events(
    incremental=True,  # Uses INCREMENTAL_LOOKBACK_DAYS from config
):
    process(event)
```

### Date Range Extraction

```python
from datetime import datetime, timedelta

start = datetime.utcnow() - timedelta(days=30)
end = datetime.utcnow()

for event in extractor.extract_events(
    incremental=False,
    start_date=start,
    end_date=end,
):
    process(event)
```

### Bulk API for Large Volumes

```python
# Use Bulk API 2.0 for large extractions
extractor = FieldHistoryExtractor("OpportunityFieldHistory", "Opportunity")

for event in extractor.extract_events(
    incremental=False,
    use_bulk_api=True,  # Enable Bulk API
):
    process(event)
```

## Performance Tips

1. **Use incremental extraction** for regular syncs
2. **Enable Bulk API** for initial loads (>10K records)
3. **Extract specific fields** when possible to reduce data volume
4. **Batch process events** for better throughput
5. **Run SetupAuditTrail weekly** to preserve history beyond 180 days

## Troubleshooting

### Connection Issues

```python
# Test Salesforce connection
from salesforce_temporal.extractors.base import BaseExtractor

extractor = BaseExtractor()
sf = extractor.connect()  # Will raise exception if connection fails
```

### Query Errors

- Check field-level security settings
- Verify history tracking is enabled for objects/fields
- Ensure API user has appropriate permissions

### Rate Limiting

The extractors use `tenacity` for automatic retry with exponential backoff.

## Integration with Atlan

```python
from pyatlan.client.atlan import AtlanClient
from salesforce_temporal.extractors.opportunity_history import OpportunityHistoryExtractor

# Extract events
extractor = OpportunityHistoryExtractor()
events = list(extractor.extract_events())

# Push to Atlan as custom metadata
client = AtlanClient(base_url="...", api_key="...")

for event in events:
    # Update asset with temporal data
    client.asset.update_custom_metadata(
        qualified_name=f"salesforce/{event.record_type}/{event.record_id}",
        attributes={
            "last_change_date": event.timestamp_occurred,
            "last_change_field": event.field_name,
            "change_count": 1,  # Aggregate as needed
        }
    )
```

## Roadmap

### Phase 2: Transformation (Weeks 5-6)
- Event deduplication
- Late-arriving data handling
- Schema evolution support

### Phase 3: Loading to Atlan (Weeks 7-8)
- Batch upload to Atlan custom metadata
- Asset lineage graph construction
- Quality score calculation

### Phase 4: Analytics & Insights (Weeks 9-12)
- DBT/SQLMesh models for analysis
- Velocity metrics calculation
- Bottleneck detection
- Predictive analytics

## License

MIT

## Support

For issues and questions:
- Open an issue on GitHub
- Contact: support@atlan.com
