"""
Unit tests for individual extractors
"""

import unittest
from lxml import etree

from twb_parser.extractors.calc_fields import CalculatedFieldExtractor
from twb_parser.extractors.layout import LayoutExtractor
from twb_parser.extractors.filters import FilterExtractor
from twb_parser.extractors.actions import ActionExtractor
from twb_parser.extractors.stories import StoryExtractor
from twb_parser.schema import LODType, ActionType, FilterType


class TestCalculatedFieldExtractor(unittest.TestCase):
    """Test CalculatedFieldExtractor"""

    def setUp(self):
        self.extractor = CalculatedFieldExtractor()

    def test_extract_simple_calculation(self):
        """Test extracting a simple calculated field"""
        xml = '''
        <column name='[Profit Ratio]' datatype='real' role='measure' caption='Profit %'>
            <calculation formula='SUM([Profit])/SUM([Sales])' comment='Profit percentage' />
        </column>
        '''
        column = etree.fromstring(xml)

        calc_field = self.extractor._extract_column_calculation(column)

        self.assertIsNotNone(calc_field)
        self.assertEqual(calc_field.name, 'Profit Ratio')
        self.assertEqual(calc_field.formula, 'SUM([Profit])/SUM([Sales])')
        self.assertEqual(calc_field.caption, 'Profit %')
        self.assertEqual(calc_field.datatype, 'real')
        self.assertEqual(calc_field.comment, 'Profit percentage')
        self.assertFalse(calc_field.is_lod)

    def test_extract_fixed_lod(self):
        """Test extracting FIXED LOD expression"""
        xml = '''
        <column name='[Total Sales by Region]' datatype='real'>
            <calculation formula='{FIXED [Region], [Year] : SUM([Sales])}' />
        </column>
        '''
        column = etree.fromstring(xml)

        calc_field = self.extractor._extract_column_calculation(column)

        self.assertTrue(calc_field.is_lod)
        self.assertEqual(calc_field.lod_type, LODType.FIXED)
        self.assertEqual(calc_field.lod_scope, ['Region', 'Year'])

    def test_extract_include_lod(self):
        """Test extracting INCLUDE LOD expression"""
        xml = '''
        <column name='[Sales with Category]' datatype='real'>
            <calculation formula='{INCLUDE [Category] : AVG([Sales])}' />
        </column>
        '''
        column = etree.fromstring(xml)

        calc_field = self.extractor._extract_column_calculation(column)

        self.assertTrue(calc_field.is_lod)
        self.assertEqual(calc_field.lod_type, LODType.INCLUDE)
        self.assertEqual(calc_field.lod_scope, ['Category'])

    def test_extract_exclude_lod(self):
        """Test extracting EXCLUDE LOD expression"""
        xml = '''
        <column name='[Sales without Sub]' datatype='real'>
            <calculation formula='{EXCLUDE [Sub-Category] : SUM([Sales])}' />
        </column>
        '''
        column = etree.fromstring(xml)

        calc_field = self.extractor._extract_column_calculation(column)

        self.assertTrue(calc_field.is_lod)
        self.assertEqual(calc_field.lod_type, LODType.EXCLUDE)
        self.assertEqual(calc_field.lod_scope, ['Sub-Category'])


class TestLayoutExtractor(unittest.TestCase):
    """Test LayoutExtractor"""

    def setUp(self):
        self.extractor = LayoutExtractor()

    def test_extract_layout_container(self):
        """Test extracting a layout container"""
        xml = '''
        <zone id='1' type='vertical' x='0' y='0' w='1000' h='800' name='Main Container'>
            <zone id='2' type='horizontal' x='0' y='0' w='1000' h='400' />
        </zone>
        '''
        zone = etree.fromstring(xml)

        container = self.extractor._extract_container(zone)

        self.assertIsNotNone(container)
        self.assertEqual(container.container_id, '1')
        self.assertEqual(container.container_type, 'vertical')
        self.assertEqual(container.title, 'Main Container')
        self.assertEqual(container.position['x'], 0)
        self.assertEqual(container.position['width'], 1000)
        self.assertEqual(len(container.children), 1)

    def test_extract_zones_from_shelf(self):
        """Test extracting zones from shelves"""
        xml = '''
        <rows>
            <field name='[Region]' />
            <field name='[Category]' />
        </rows>
        '''
        rows = etree.fromstring(xml)

        zone = self.extractor._extract_zone_from_shelf(rows, 'rows')

        self.assertIsNotNone(zone)
        self.assertEqual(zone.zone_type, 'rows')
        self.assertEqual(len(zone.fields), 2)
        self.assertIn('Region', zone.fields)
        self.assertIn('Category', zone.fields)


class TestFilterExtractor(unittest.TestCase):
    """Test FilterExtractor"""

    def setUp(self):
        self.extractor = FilterExtractor()

    def test_extract_categorical_filter(self):
        """Test extracting a categorical filter"""
        xml = '''
        <filter column='[Category]' class='categorical' global='true'>
            <groupfilter>
                <member value='Furniture' />
                <member value='Technology' />
            </groupfilter>
        </filter>
        '''
        filter_elem = etree.fromstring(xml)

        filter_config = self.extractor._extract_filter(filter_elem, worksheet='Sheet1')

        self.assertIsNotNone(filter_config)
        self.assertEqual(filter_config.field_name, 'Category')
        self.assertEqual(filter_config.filter_type, FilterType.CATEGORICAL)
        self.assertTrue(filter_config.is_global)
        self.assertEqual(len(filter_config.filter_values), 2)
        self.assertIn('Furniture', filter_config.filter_values)

    def test_extract_quantitative_filter(self):
        """Test extracting a quantitative filter"""
        xml = '''
        <filter column='[Sales]' class='quantitative'>
            <min>1000</min>
            <max>10000</max>
        </filter>
        '''
        filter_elem = etree.fromstring(xml)

        filter_config = self.extractor._extract_filter(filter_elem, worksheet='Sheet1')

        self.assertEqual(filter_config.filter_type, FilterType.QUANTITATIVE)
        self.assertIn('BETWEEN', filter_config.condition)

    def test_extract_relative_date_filter(self):
        """Test extracting a relative date filter"""
        xml = '''
        <filter column='[Order Date]' class='relative-date'>
            <relative-date period='month' quantity='3' />
        </filter>
        '''
        filter_elem = etree.fromstring(xml)

        filter_config = self.extractor._extract_filter(filter_elem, worksheet='Sheet1')

        self.assertEqual(filter_config.filter_type, FilterType.RELATIVE_DATE)
        self.assertIsNotNone(filter_config.relative_date_period)


class TestActionExtractor(unittest.TestCase):
    """Test ActionExtractor"""

    def setUp(self):
        self.extractor = ActionExtractor()

    def test_extract_filter_action(self):
        """Test extracting a filter action"""
        xml = '''
        <filter name='Region Filter' enabled='true'>
            <source>
                <worksheet name='Map' />
            </source>
            <target>
                <worksheet name='Details' />
            </target>
            <filter>
                <field name='[Region]' />
            </filter>
        </filter>
        '''
        action_elem = etree.fromstring(xml)

        action = self.extractor._extract_action(action_elem, 'Dashboard1')

        self.assertIsNotNone(action)
        self.assertEqual(action.action_type, ActionType.FILTER)
        self.assertEqual(action.action_name, 'Region Filter')
        self.assertTrue(action.enabled)
        self.assertEqual(action.source_sheets, ['Map'])
        self.assertEqual(action.target_sheets, ['Details'])
        self.assertIn('Region', action.fields)

    def test_extract_url_action(self):
        """Test extracting a URL action"""
        xml = '''
        <url name='Google Search'>
            <url>https://www.google.com/search?q=&lt;Product Name&gt;</url>
        </url>
        '''
        action_elem = etree.fromstring(xml)

        action = self.extractor._extract_action(action_elem, 'Dashboard1')

        self.assertEqual(action.action_type, ActionType.URL)
        self.assertIsNotNone(action.url_template)


class TestStoryExtractor(unittest.TestCase):
    """Test StoryExtractor"""

    def setUp(self):
        self.extractor = StoryExtractor()

    def test_extract_story_with_points(self):
        """Test extracting a complete story"""
        xml = '''
        <story name='Sales Analysis' description='Q4 2023 Analysis'>
            <story-points>
                <story-point caption='Overview' description='High-level summary'>
                    <zone worksheet='Summary' />
                </story-point>
                <story-point caption='Regional Breakdown'>
                    <zone dashboard='Regional Dashboard' />
                    <zone type='text'>
                        <text>Key insight: West region grew 30%</text>
                    </zone>
                </story-point>
            </story-points>
        </story>
        '''
        story_elem = etree.fromstring(xml)

        story = self.extractor.extract(story_elem)

        self.assertIsNotNone(story)
        self.assertEqual(story.story_name, 'Sales Analysis')
        self.assertEqual(story.description, 'Q4 2023 Analysis')
        self.assertEqual(len(story.points), 2)

        # Check first point
        point1 = story.points[0]
        self.assertEqual(point1.caption, 'Overview')
        self.assertEqual(point1.description, 'High-level summary')
        self.assertEqual(point1.worksheet_name, 'Summary')
        self.assertEqual(point1.order, 0)

        # Check second point
        point2 = story.points[1]
        self.assertEqual(point2.caption, 'Regional Breakdown')
        self.assertEqual(point2.dashboard_name, 'Regional Dashboard')
        self.assertEqual(point2.order, 1)


if __name__ == '__main__':
    unittest.main()
