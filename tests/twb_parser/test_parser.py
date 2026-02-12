"""
Unit tests for TableauWorkbookParser
"""

import unittest
import tempfile
import zipfile
from pathlib import Path
from lxml import etree

from twb_parser import TableauWorkbookParser
from twb_parser.schema import LODType, ActionType, FilterType


class TestTableauWorkbookParser(unittest.TestCase):
    """Test the main TableauWorkbookParser class"""

    def setUp(self):
        """Set up test fixtures"""
        self.parser = TableauWorkbookParser()

    def test_parse_simple_workbook(self):
        """Test parsing a simple workbook XML"""
        xml_content = '''<?xml version='1.0' encoding='utf-8' ?>
        <workbook version='18.1' xmlns:user='http://www.tableausoftware.com/xml/user'>
            <preferences />
            <datasources>
                <datasource name='Sample' version='18.1' inline='true'>
                </datasource>
            </datasources>
            <worksheets>
                <worksheet name='Sheet1'>
                    <table>
                        <view>
                            <datasources />
                        </view>
                    </table>
                </worksheet>
            </worksheets>
            <dashboards />
        </workbook>
        '''

        metadata = self.parser.parse_xml_string(xml_content)

        self.assertEqual(metadata.version, '18.1')
        self.assertEqual(len(metadata.worksheets), 1)
        self.assertEqual(metadata.worksheets[0].name, 'Sheet1')

    def test_parse_calculated_field(self):
        """Test parsing calculated fields"""
        xml_content = '''<?xml version='1.0' encoding='utf-8' ?>
        <workbook version='18.1'>
            <datasources>
                <datasource name='Sample' inline='true'>
                    <column name='[Profit Ratio]' datatype='real' role='measure'>
                        <calculation formula='SUM([Profit])/SUM([Sales])' comment='Profit as percentage of sales' />
                    </column>
                </datasource>
            </datasources>
            <worksheets>
                <worksheet name='Sheet1'>
                    <datasource name='Sample' />
                </worksheet>
            </worksheets>
        </workbook>
        '''

        metadata = self.parser.parse_xml_string(xml_content)

        # Check data source with calculated field
        self.assertEqual(len(metadata.data_sources), 1)

    def test_parse_lod_expression(self):
        """Test parsing LOD expressions"""
        xml_content = '''<?xml version='1.0' encoding='utf-8' ?>
        <workbook version='18.1'>
            <datasources>
                <datasource name='Sample' inline='true'>
                    <column name='[Total Sales by Region]' datatype='real'>
                        <calculation formula='{FIXED [Region] : SUM([Sales])}' />
                    </column>
                    <column name='[Sales with Category]' datatype='real'>
                        <calculation formula='{INCLUDE [Category] : SUM([Sales])}' />
                    </column>
                    <column name='[Sales without SubCategory]' datatype='real'>
                        <calculation formula='{EXCLUDE [Sub-Category] : SUM([Sales])}' />
                    </column>
                </datasource>
            </datasources>
            <worksheets>
                <worksheet name='Sheet1'>
                    <datasource name='Sample' />
                </worksheet>
            </worksheets>
        </workbook>
        '''

        metadata = self.parser.parse_xml_string(xml_content)

        # LOD expressions would be in worksheet calculated fields after full extraction
        # This test validates the XML can be parsed

    def test_parse_dashboard_with_actions(self):
        """Test parsing dashboard with actions"""
        xml_content = '''<?xml version='1.0' encoding='utf-8' ?>
        <workbook version='18.1'>
            <worksheets>
                <worksheet name='Sheet1' />
                <worksheet name='Sheet2' />
            </worksheets>
            <dashboards>
                <dashboard name='Dashboard1'>
                    <actions>
                        <filter name='FilterAction' enabled='true'>
                            <source>
                                <worksheet name='Sheet1' />
                            </source>
                            <target>
                                <worksheet name='Sheet2' />
                            </target>
                            <filter>
                                <field name='[Category]' />
                            </filter>
                        </filter>
                        <highlight name='HighlightAction'>
                            <source>
                                <worksheet name='Sheet1' />
                            </source>
                            <target all='true' />
                        </highlight>
                    </actions>
                </dashboard>
            </dashboards>
        </workbook>
        '''

        metadata = self.parser.parse_xml_string(xml_content)

        self.assertEqual(len(metadata.dashboards), 1)
        dashboard = metadata.dashboards[0]
        self.assertEqual(dashboard.name, 'Dashboard1')

        # Check actions
        self.assertEqual(len(dashboard.actions), 2)

        # Check filter action
        filter_action = dashboard.actions[0]
        self.assertEqual(filter_action.action_type, ActionType.FILTER)
        self.assertEqual(filter_action.action_name, 'FilterAction')
        self.assertTrue(filter_action.enabled)

        # Check highlight action
        highlight_action = dashboard.actions[1]
        self.assertEqual(highlight_action.action_type, ActionType.HIGHLIGHT)

    def test_parse_story(self):
        """Test parsing Data Stories"""
        xml_content = '''<?xml version='1.0' encoding='utf-8' ?>
        <workbook version='18.1'>
            <worksheets>
                <worksheet name='Sheet1' />
            </worksheets>
            <stories>
                <story name='Sales Story' description='Q4 Sales Analysis'>
                    <story-points>
                        <story-point caption='Overview'>
                            <zone worksheet='Sheet1' />
                        </story-point>
                        <story-point caption='Key Findings' description='Regional breakdown'>
                            <zone type='text'>
                                <text>Sales increased 25% in Q4</text>
                            </zone>
                        </story-point>
                    </story-points>
                </story>
            </stories>
        </workbook>
        '''

        metadata = self.parser.parse_xml_string(xml_content)

        self.assertEqual(len(metadata.stories), 1)
        story = metadata.stories[0]
        self.assertEqual(story.story_name, 'Sales Story')
        self.assertEqual(story.description, 'Q4 Sales Analysis')
        self.assertEqual(len(story.points), 2)

        # Check first point
        point1 = story.points[0]
        self.assertEqual(point1.caption, 'Overview')
        self.assertEqual(point1.worksheet_name, 'Sheet1')

        # Check second point
        point2 = story.points[1]
        self.assertEqual(point2.caption, 'Key Findings')

    def test_json_ld_output(self):
        """Test JSON-LD output format"""
        xml_content = '''<?xml version='1.0' encoding='utf-8' ?>
        <workbook version='18.1'>
            <worksheets>
                <worksheet name='Sheet1' caption='Sales Analysis' />
            </worksheets>
        </workbook>
        '''

        metadata = self.parser.parse_xml_string(xml_content)
        json_ld = metadata.to_json_ld()

        # Check JSON-LD structure
        self.assertEqual(json_ld['@context'], 'https://schema.org/')
        self.assertEqual(json_ld['@type'], 'Dataset')
        self.assertIn('dateExtracted', json_ld)
        self.assertIn('worksheets', json_ld)
        self.assertEqual(len(json_ld['worksheets']), 1)

    def test_parse_twbx_file(self):
        """Test parsing .twbx packaged workbook"""
        # Create a temporary .twbx file
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create sample .twb content
            twb_content = '''<?xml version='1.0' encoding='utf-8' ?>
            <workbook version='18.1'>
                <worksheets>
                    <worksheet name='TestSheet' />
                </worksheets>
            </workbook>
            '''

            # Create .twbx (zip file)
            twbx_path = temp_path / 'test.twbx'
            with zipfile.ZipFile(twbx_path, 'w') as zf:
                zf.writestr('test.twb', twb_content)

            # Parse the .twbx
            metadata = self.parser.parse_file(twbx_path)

            self.assertEqual(metadata.workbook_name, 'test')
            self.assertEqual(len(metadata.worksheets), 1)
            self.assertEqual(metadata.worksheets[0].name, 'TestSheet')


if __name__ == '__main__':
    unittest.main()
