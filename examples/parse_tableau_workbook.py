"""
Sample script demonstrating Tableau workbook parsing

This script shows how to use the TableauWorkbookParser to extract
deep metadata from Tableau workbooks.
"""

import json
import sys
from pathlib import Path

# Add parent directory to path to import twb_parser
sys.path.insert(0, str(Path(__file__).parent.parent))

from twb_parser import TableauWorkbookParser


def main():
    """Main execution function"""

    # Initialize parser
    parser = TableauWorkbookParser()

    # Example 1: Parse from file path
    print("=" * 80)
    print("EXAMPLE 1: Parse Tableau Workbook")
    print("=" * 80)

    # You can parse either .twb or .twbx files
    # workbook_path = "path/to/your/workbook.twb"
    # metadata = parser.parse_file(workbook_path)

    # For demonstration, we'll use a sample XML string
    sample_xml = '''<?xml version='1.0' encoding='utf-8' ?>
    <workbook version='18.1' xmlns:user='http://www.tableausoftware.com/xml/user'>
        <preferences />

        <datasources>
            <datasource name='Sample - Superstore' version='18.1' inline='true'>
                <column name='[Profit Ratio]' datatype='real' role='measure' caption='Profit %'>
                    <calculation formula='SUM([Profit])/SUM([Sales])' comment='Profit as percentage of sales' />
                </column>
                <column name='[Total Sales by Region]' datatype='real' role='measure'>
                    <calculation formula='{FIXED [Region] : SUM([Sales])}' />
                </column>
                <column name='[Sales with Category]' datatype='real' role='measure'>
                    <calculation formula='{INCLUDE [Category] : AVG([Sales])}' />
                </column>
            </datasource>
        </datasources>

        <worksheets>
            <worksheet name='Sales by Region' caption='Regional Sales Analysis'>
                <table>
                    <view>
                        <datasources>
                            <datasource name='Sample - Superstore' />
                        </datasources>
                    </view>
                    <rows>
                        <field name='[Region]' />
                        <field name='[Sales]' />
                    </rows>
                    <cols>
                        <field name='[Order Date]' />
                    </cols>
                </table>
                <filters>
                    <filter column='[Category]' class='categorical' global='true'>
                        <groupfilter>
                            <member value='Furniture' />
                            <member value='Technology' />
                        </groupfilter>
                    </filter>
                </filters>
            </worksheet>

            <worksheet name='Sales Details' caption='Detailed Sales View'>
                <table>
                    <view>
                        <datasources>
                            <datasource name='Sample - Superstore' />
                        </datasources>
                    </view>
                </table>
            </worksheet>
        </worksheets>

        <dashboards>
            <dashboard name='Executive Dashboard' caption='Executive Summary'>
                <zones>
                    <zone id='1' type='vertical' x='0' y='0' w='1200' h='800' name='Main Container'>
                        <zone id='2' type='layout-basic' x='0' y='0' w='1200' h='400' name='Sales by Region' />
                        <zone id='3' type='layout-basic' x='0' y='400' w='1200' h='400' name='Sales Details' />
                    </zone>
                </zones>
                <actions>
                    <filter name='Region Filter' enabled='true'>
                        <source>
                            <worksheet name='Sales by Region' />
                        </source>
                        <target>
                            <worksheet name='Sales Details' />
                        </target>
                        <filter>
                            <field name='[Region]' />
                        </filter>
                    </filter>
                    <highlight name='Highlight Action'>
                        <source>
                            <worksheet name='Sales by Region' />
                        </source>
                        <target all='true' />
                    </highlight>
                    <url name='Google Search'>
                        <url>https://www.google.com/search?q=&lt;Product Name&gt;</url>
                    </url>
                </actions>
            </dashboard>
        </dashboards>

        <stories>
            <story name='Q4 Sales Story' description='Quarterly sales analysis for Q4'>
                <story-points>
                    <story-point caption='Overview'>
                        <zone worksheet='Sales by Region' />
                    </story-point>
                    <story-point caption='Key Findings' description='Regional breakdown and insights'>
                        <zone type='text'>
                            <text>Sales increased 25% in Q4, with Technology leading growth.</text>
                        </zone>
                    </story-point>
                    <story-point caption='Recommendations'>
                        <zone dashboard='Executive Dashboard' />
                        <zone type='text'>
                            <text>Focus marketing efforts on high-performing regions.</text>
                        </zone>
                    </story-point>
                </story-points>
            </story>
        </stories>

        <parameters>
            <parameter name='[Select Region]' datatype='string' value='East' caption='Region Selector'>
            </parameter>
        </parameters>

    </workbook>
    '''

    # Parse the XML
    metadata = parser.parse_xml_string(sample_xml)

    # Display summary
    print(f"\nWorkbook: {metadata.workbook_name}")
    print(f"Version: {metadata.version}")
    print(f"Extracted at: {metadata.extracted_at}")
    print(f"\nSummary:")
    print(f"  - Worksheets: {len(metadata.worksheets)}")
    print(f"  - Dashboards: {len(metadata.dashboards)}")
    print(f"  - Stories: {len(metadata.stories)}")
    print(f"  - Data Sources: {len(metadata.data_sources)}")
    print(f"  - Parameters: {len(metadata.parameters)}")

    # Example 2: Extract Calculated Fields
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Extracted Calculated Fields")
    print("=" * 80)

    for worksheet in metadata.worksheets:
        if worksheet.calculated_fields:
            print(f"\nWorksheet: {worksheet.name}")
            for calc_field in worksheet.calculated_fields:
                print(f"\n  Field: {calc_field.name}")
                print(f"  Formula: {calc_field.formula}")
                if calc_field.caption:
                    print(f"  Caption: {calc_field.caption}")
                if calc_field.comment:
                    print(f"  Comment: {calc_field.comment}")

                if calc_field.is_lod:
                    print(f"  ⚡ LOD Expression!")
                    print(f"    Type: {calc_field.lod_type.value}")
                    print(f"    Scope: {', '.join(calc_field.lod_scope)}")

    # Example 3: Extract Dashboard Actions
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Dashboard Actions")
    print("=" * 80)

    for dashboard in metadata.dashboards:
        print(f"\nDashboard: {dashboard.name}")
        print(f"Caption: {dashboard.caption}")

        if dashboard.actions:
            print("\nActions:")
            for action in dashboard.actions:
                print(f"\n  - {action.action_name}")
                print(f"    Type: {action.action_type.value}")
                print(f"    Enabled: {action.enabled}")
                print(f"    Source Sheets: {', '.join(action.source_sheets)}")
                print(f"    Target Sheets: {', '.join(action.target_sheets)}")
                if action.fields:
                    print(f"    Fields: {', '.join(action.fields)}")
                if action.url_template:
                    print(f"    URL: {action.url_template}")

    # Example 4: Extract Layout Hierarchy
    print("\n" + "=" * 80)
    print("EXAMPLE 4: Layout Hierarchy")
    print("=" * 80)

    def print_layout(container, indent=0):
        """Recursively print layout hierarchy"""
        prefix = "  " * indent
        title = container.title or container.worksheet_name or "unnamed"
        print(f"{prefix}- {container.container_type}: {title}")
        if container.position:
            pos = container.position
            print(f"{prefix}  Position: ({pos['x']}, {pos['y']}) Size: {pos['width']}x{pos['height']}")
        for child in container.children:
            print_layout(child, indent + 1)

    for dashboard in metadata.dashboards:
        print(f"\nDashboard: {dashboard.name}")
        for container in dashboard.layout_containers:
            print_layout(container)

    # Example 5: Extract Data Stories
    print("\n" + "=" * 80)
    print("EXAMPLE 5: Data Stories")
    print("=" * 80)

    for story in metadata.stories:
        print(f"\nStory: {story.story_name}")
        if story.description:
            print(f"Description: {story.description}")

        print(f"\nStory Points ({len(story.points)}):")
        for point in story.points:
            print(f"\n  {point.order + 1}. {point.caption}")
            if point.description:
                print(f"     Description: {point.description}")
            if point.worksheet_name:
                print(f"     Worksheet: {point.worksheet_name}")
            if point.dashboard_name:
                print(f"     Dashboard: {point.dashboard_name}")
            if point.narrative_text:
                print(f"     Narrative: {point.narrative_text}")

    # Example 6: Export to JSON-LD
    print("\n" + "=" * 80)
    print("EXAMPLE 6: Export to JSON-LD")
    print("=" * 80)

    json_ld = metadata.to_json_ld()
    print("\nJSON-LD Output (first 500 chars):")
    json_str = json.dumps(json_ld, indent=2)
    print(json_str[:500] + "...")

    # Save to file
    output_path = Path(__file__).parent / "sample_metadata.json"
    with open(output_path, 'w') as f:
        json.dump(json_ld, f, indent=2)
    print(f"\nFull metadata saved to: {output_path}")

    # Example 7: Filter Information
    print("\n" + "=" * 80)
    print("EXAMPLE 7: Filter Configurations")
    print("=" * 80)

    for worksheet in metadata.worksheets:
        if worksheet.filters:
            print(f"\nWorksheet: {worksheet.name}")
            for filter_config in worksheet.filters:
                print(f"\n  Filter: {filter_config.field_name}")
                print(f"  Type: {filter_config.filter_type.value}")
                print(f"  Global: {filter_config.is_global}")
                if filter_config.filter_values:
                    print(f"  Values: {filter_config.filter_values}")

    print("\n" + "=" * 80)
    print("Demo Complete!")
    print("=" * 80)


if __name__ == '__main__':
    main()
