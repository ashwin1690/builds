# Tableau Deep Metadata Parser

A Python library for extracting deep contextual metadata from Tableau workbooks (.twb/.twbx files) that traditional catalog tools like Atlan often miss.

## Features

### Extracted Metadata

- **Calculated Fields & Formulas**: Extract all calculated field definitions including complex formulas
- **LOD Expressions**: Parse and classify Level of Detail expressions (FIXED, INCLUDE, EXCLUDE)
- **Layout Containers**: Extract dashboard layout hierarchy with titles and positions
- **Zone Hierarchies**: Extract worksheet zone organization (rows, columns, filters, marks)
- **Filter Configurations**: Detailed filter settings including categorical, quantitative, date, and relative date filters
- **Dashboard Actions**: Extract all dashboard actions (filter, highlight, URL, navigation, parameter changes)
- **Data Stories**: Extract story points with narrative text and annotations

### Output Format

Metadata is output in a **JSON-LD compatible format** that can be:
- Integrated with Atlan's enrichment APIs
- Stored as RDF triples
- Used for semantic web applications
- Consumed by metadata management tools

## Installation

```bash
pip install -r requirements.txt
```

Required dependencies:
- `lxml>=4.9.0` - XML parsing
- `pyatlan>=2.0.0` - Atlan integration (optional)

## Quick Start

### Parse a .twb file

```python
from twb_parser import TableauWorkbookParser

parser = TableauWorkbookParser()
metadata = parser.parse_file('workbook.twb')

# Access extracted metadata
print(f"Workbook: {metadata.workbook_name}")
print(f"Version: {metadata.version}")
print(f"Worksheets: {len(metadata.worksheets)}")
print(f"Dashboards: {len(metadata.dashboards)}")
print(f"Stories: {len(metadata.stories)}")
```

### Parse a .twbx file

```python
# Works seamlessly with packaged workbooks
metadata = parser.parse_file('workbook.twbx')
```

### Export to JSON-LD

```python
import json

metadata = parser.parse_file('workbook.twb')
json_ld = metadata.to_json_ld()

# Save to file
with open('metadata.json', 'w') as f:
    json.dump(json_ld, f, indent=2)
```

## Usage Examples

### Extract Calculated Fields

```python
metadata = parser.parse_file('workbook.twb')

for worksheet in metadata.worksheets:
    print(f"\nWorksheet: {worksheet.name}")
    for calc_field in worksheet.calculated_fields:
        print(f"  - {calc_field.name}: {calc_field.formula}")

        if calc_field.is_lod:
            print(f"    LOD Type: {calc_field.lod_type.value}")
            print(f"    Scope: {', '.join(calc_field.lod_scope)}")
```

### Extract Dashboard Actions

```python
for dashboard in metadata.dashboards:
    print(f"\nDashboard: {dashboard.name}")
    for action in dashboard.actions:
        print(f"  Action: {action.action_name}")
        print(f"  Type: {action.action_type.value}")
        print(f"  Source: {', '.join(action.source_sheets)}")
        print(f"  Target: {', '.join(action.target_sheets)}")
```

### Extract Layout Hierarchy

```python
def print_layout(container, indent=0):
    prefix = "  " * indent
    print(f"{prefix}- {container.container_type}: {container.title or 'unnamed'}")
    for child in container.children:
        print_layout(child, indent + 1)

for dashboard in metadata.dashboards:
    print(f"\nDashboard: {dashboard.name}")
    for container in dashboard.layout_containers:
        print_layout(container)
```

### Extract Data Stories

```python
for story in metadata.stories:
    print(f"\nStory: {story.story_name}")
    print(f"Description: {story.description}")

    for point in story.points:
        print(f"\n  Point {point.order + 1}: {point.caption}")
        if point.narrative_text:
            print(f"  Narrative: {point.narrative_text}")
        if point.worksheet_name:
            print(f"  Worksheet: {point.worksheet_name}")
```

### Extract Filter Configurations

```python
for worksheet in metadata.worksheets:
    if worksheet.filters:
        print(f"\nWorksheet: {worksheet.name}")
        for filter_config in worksheet.filters:
            print(f"  - {filter_config.field_name}")
            print(f"    Type: {filter_config.filter_type.value}")
            if filter_config.filter_values:
                print(f"    Values: {filter_config.filter_values}")
```

## Integration with Atlan

The extracted metadata can be pushed to Atlan to enrich your data catalog:

```python
from pyatlan.client.atlan import AtlanClient
from twb_parser import TableauWorkbookParser

# Parse Tableau workbook
parser = TableauWorkbookParser()
metadata = parser.parse_file('workbook.twb')

# Initialize Atlan client
client = AtlanClient()

# Enrich Atlan assets with extracted metadata
for worksheet in metadata.worksheets:
    for calc_field in worksheet.calculated_fields:
        # Find corresponding Atlan asset
        asset = client.asset.find_by_qualified_name(
            qualified_name=f"{metadata.workbook_name}/{worksheet.name}/{calc_field.name}",
            asset_type="TableauCalculatedField"
        )

        # Update with extracted metadata
        if asset:
            client.asset.update_custom_metadata(
                guid=asset.guid,
                attributes={
                    "formula": calc_field.formula,
                    "is_lod": calc_field.is_lod,
                    "lod_type": calc_field.lod_type.value if calc_field.is_lod else None,
                    "comment": calc_field.comment
                }
            )
```

## Metadata Schema

The library uses a JSON-LD compatible schema with the following structure:

```json
{
  "@context": "https://schema.org/",
  "@type": "Dataset",
  "name": "workbook_name",
  "version": "18.1",
  "dateExtracted": "2024-01-15T10:30:00Z",
  "worksheets": [...],
  "dashboards": [...],
  "stories": [...],
  "dataSources": [...],
  "parameters": [...]
}
```

See `twb_parser/schema.py` for detailed schema definitions.

## Running Tests

```bash
# Run all tests
python -m pytest tests/twb_parser/

# Run specific test file
python -m pytest tests/twb_parser/test_parser.py

# Run with coverage
python -m pytest --cov=twb_parser tests/twb_parser/
```

## Architecture

```
twb_parser/
├── __init__.py           # Main exports
├── parser.py             # Main parser class
├── schema.py             # Metadata schema definitions
└── extractors/           # Specialized extractors
    ├── __init__.py
    ├── calc_fields.py    # Calculated fields & LOD
    ├── layout.py         # Layout containers & zones
    ├── filters.py        # Filter configurations
    ├── actions.py        # Dashboard actions
    └── stories.py        # Data Stories narratives
```

## What Makes This Different

Traditional catalog tools like Atlan extract basic metadata from Tableau (table names, field names, data sources). This parser extracts the **contextual metadata** that makes Tableau powerful:

| Traditional Tools | This Parser |
|------------------|-------------|
| Field names | Calculated field formulas |
| Basic lineage | LOD expression scope |
| Dashboard exists | Layout hierarchy with titles |
| Filter exists | Filter type, values, conditions |
| - | Dashboard action configurations |
| - | Data Story narratives |
| - | Zone organization |

## Limitations

- Requires direct access to .twb/.twbx files
- Does not extract:
  - Parameter values from Tableau Server
  - Runtime metrics or usage statistics
  - User permissions
  - Thumbnail images
- Tested with Tableau versions 2018.1 - 2024.1

## Contributing

Contributions welcome! Please ensure:
1. All tests pass: `python -m pytest tests/twb_parser/`
2. Code follows existing style
3. New features include tests

## License

MIT

## Related Projects

- [Atlan](https://atlan.com/) - Active metadata platform
- [Tableau Metadata API](https://help.tableau.com/current/api/metadata_api/en-us/) - Official Tableau metadata access
- Phase 1B of this project will add Tableau Metadata API integration for server-side metadata
