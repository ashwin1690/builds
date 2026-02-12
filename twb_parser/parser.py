"""
Main Tableau Workbook Parser

Handles .twb and .twbx file parsing and coordinates metadata extraction.
"""

import zipfile
import tempfile
import os
from pathlib import Path
from typing import Union, Optional
from lxml import etree

from .schema import TableauMetadata, WorksheetMetadata, DashboardMetadata
from .extractors.calc_fields import CalculatedFieldExtractor
from .extractors.layout import LayoutExtractor
from .extractors.filters import FilterExtractor
from .extractors.actions import ActionExtractor
from .extractors.stories import StoryExtractor


class TableauWorkbookParser:
    """
    Main parser for Tableau workbook files (.twb/.twbx).

    Extracts deep metadata that traditional catalog tools miss:
    - Calculated fields and LOD expressions
    - Layout container hierarchies
    - Zone hierarchies
    - Filter configurations
    - Dashboard actions
    - Data Stories narratives
    """

    def __init__(self):
        self.calc_field_extractor = CalculatedFieldExtractor()
        self.layout_extractor = LayoutExtractor()
        self.filter_extractor = FilterExtractor()
        self.action_extractor = ActionExtractor()
        self.story_extractor = StoryExtractor()

    def parse_file(self, file_path: Union[str, Path]) -> TableauMetadata:
        """
        Parse a Tableau workbook file (.twb or .twbx).

        Args:
            file_path: Path to the .twb or .twbx file

        Returns:
            TableauMetadata object containing extracted metadata
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        if file_path.suffix.lower() == '.twbx':
            return self._parse_twbx(file_path)
        elif file_path.suffix.lower() == '.twb':
            return self._parse_twb(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}. Expected .twb or .twbx")

    def _parse_twbx(self, twbx_path: Path) -> TableauMetadata:
        """
        Parse a .twbx file (packaged workbook).
        Extracts the .twb XML file from the zip archive.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(twbx_path, 'r') as zip_ref:
                # Find the .twb file in the archive
                twb_files = [f for f in zip_ref.namelist() if f.endswith('.twb')]

                if not twb_files:
                    raise ValueError(f"No .twb file found in {twbx_path}")

                if len(twb_files) > 1:
                    # Use the first one or the one that matches the twbx name
                    twb_name = twbx_path.stem + '.twb'
                    twb_file = next((f for f in twb_files if f.endswith(twb_name)), twb_files[0])
                else:
                    twb_file = twb_files[0]

                # Extract to temp directory
                zip_ref.extract(twb_file, temp_dir)
                extracted_path = Path(temp_dir) / twb_file

                return self._parse_twb(extracted_path)

    def _parse_twb(self, twb_path: Path) -> TableauMetadata:
        """
        Parse a .twb file (XML workbook definition).
        """
        # Parse XML
        tree = etree.parse(str(twb_path))
        root = tree.getroot()

        # Create metadata object
        metadata = TableauMetadata()
        metadata.workbook_name = twb_path.stem

        # Extract workbook version
        version = root.get('version')
        if version:
            metadata.version = version

        # Extract worksheets
        worksheets = root.findall('.//worksheet')
        for worksheet in worksheets:
            ws_metadata = self._extract_worksheet_metadata(worksheet)
            if ws_metadata:
                metadata.worksheets.append(ws_metadata)

        # Extract dashboards
        dashboards = root.findall('.//dashboard')
        for dashboard in dashboards:
            dash_metadata = self._extract_dashboard_metadata(dashboard)
            if dash_metadata:
                metadata.dashboards.append(dash_metadata)

        # Extract stories
        stories = root.findall('.//story')
        for story in stories:
            story_metadata = self.story_extractor.extract(story)
            if story_metadata:
                metadata.stories.append(story_metadata)

        # Extract data sources (only top-level ones from datasources element)
        datasources_elem = root.find('datasources')
        if datasources_elem is not None:
            datasources = datasources_elem.findall('datasource')
            for datasource in datasources:
                ds_metadata = self._extract_datasource_metadata(datasource)
                if ds_metadata:
                    metadata.data_sources.append(ds_metadata)

        # Extract parameters
        parameters = root.findall('.//parameter')
        for parameter in parameters:
            param_metadata = self._extract_parameter_metadata(parameter)
            if param_metadata:
                metadata.parameters.append(param_metadata)

        return metadata

    def _extract_worksheet_metadata(self, worksheet_element) -> Optional[WorksheetMetadata]:
        """Extract metadata from a worksheet element"""
        name = worksheet_element.get('name')
        if not name:
            return None

        ws_metadata = WorksheetMetadata(name=name)

        # Caption
        caption = worksheet_element.get('caption')
        if caption:
            ws_metadata.caption = caption

        # Extract calculated fields
        calc_fields = self.calc_field_extractor.extract(worksheet_element)
        ws_metadata.calculated_fields.extend(calc_fields)

        # Extract zones
        zones = self.layout_extractor.extract_zones(worksheet_element)
        ws_metadata.zones.extend(zones)

        # Extract filters
        filters = self.filter_extractor.extract_worksheet_filters(worksheet_element, name)
        ws_metadata.filters.extend(filters)

        return ws_metadata

    def _extract_dashboard_metadata(self, dashboard_element) -> Optional[DashboardMetadata]:
        """Extract metadata from a dashboard element"""
        name = dashboard_element.get('name')
        if not name:
            return None

        dash_metadata = DashboardMetadata(name=name)

        # Caption
        caption = dashboard_element.get('caption')
        if caption:
            dash_metadata.caption = caption

        # Extract layout containers
        layouts = self.layout_extractor.extract_layout_containers(dashboard_element)
        dash_metadata.layout_containers.extend(layouts)

        # Extract actions
        actions = self.action_extractor.extract(dashboard_element, name)
        dash_metadata.actions.extend(actions)

        # Extract dashboard filters
        filters = self.filter_extractor.extract_dashboard_filters(dashboard_element, name)
        dash_metadata.filters.extend(filters)

        return dash_metadata

    def _extract_datasource_metadata(self, datasource_element) -> Optional[dict]:
        """Extract basic datasource metadata"""
        name = datasource_element.get('name')
        if not name or name == 'Parameters':
            return None

        ds_metadata = {
            'name': name,
            'caption': datasource_element.get('caption'),
            'inline': datasource_element.get('inline') == 'true'
        }

        # Extract connection info
        connection = datasource_element.find('.//connection')
        if connection is not None:
            ds_metadata['connection'] = {
                'class': connection.get('class'),
                'server': connection.get('server'),
                'dbname': connection.get('dbname'),
                'schema': connection.get('schema')
            }

        return ds_metadata

    def _extract_parameter_metadata(self, parameter_element) -> Optional[dict]:
        """Extract parameter metadata"""
        name = parameter_element.get('name')
        if not name:
            return None

        param_metadata = {
            'name': name,
            'caption': parameter_element.get('caption'),
            'datatype': parameter_element.get('datatype'),
            'value': parameter_element.get('value')
        }

        # Extract allowed values if present
        aliases = parameter_element.findall('.//alias')
        if aliases:
            param_metadata['allowed_values'] = [
                {'key': alias.get('key'), 'value': alias.get('value')}
                for alias in aliases
            ]

        return param_metadata

    def parse_xml_string(self, xml_string: str) -> TableauMetadata:
        """
        Parse Tableau XML from a string.

        Args:
            xml_string: XML content as string

        Returns:
            TableauMetadata object
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.twb', delete=False) as temp_file:
            temp_file.write(xml_string)
            temp_path = temp_file.name

        try:
            return self._parse_twb(Path(temp_path))
        finally:
            os.unlink(temp_path)
