"""
Layout Container and Zone Hierarchy Extractor

Extracts dashboard layout containers and worksheet zone hierarchies
from Tableau workbooks.
"""

from typing import List, Optional, Dict
from lxml import etree

from ..schema import LayoutContainer, ZoneHierarchy


class LayoutExtractor:
    """
    Extracts layout and zone information from Tableau XML.

    Layout containers represent the visual structure of dashboards.
    Zone hierarchies represent the pill/field organization in worksheets.
    """

    def extract_layout_containers(self, dashboard_element) -> List[LayoutContainer]:
        """
        Extract layout container hierarchy from a dashboard.

        Args:
            dashboard_element: lxml Element representing a dashboard

        Returns:
            List of root-level LayoutContainer objects with nested children
        """
        containers = []

        # Find zones element which contains layout information
        zones = dashboard_element.find('.//zones')
        if zones is None:
            return containers

        # Process each zone (layout container)
        for zone in zones.findall('zone'):
            container = self._extract_container(zone)
            if container:
                containers.append(container)

        return containers

    def _extract_container(self, zone_element, parent_id: Optional[str] = None) -> Optional[LayoutContainer]:
        """
        Recursively extract layout container from a zone element.

        Args:
            zone_element: lxml Element representing a zone
            parent_id: ID of the parent container

        Returns:
            LayoutContainer object with nested children
        """
        zone_id = zone_element.get('id')
        if not zone_id:
            return None

        # Get container type
        container_type = zone_element.get('type', 'unknown')

        # Create container
        container = LayoutContainer(
            container_id=zone_id,
            container_type=container_type
        )

        # Extract title if present
        title = zone_element.get('name')
        if title:
            container.title = title

        # Extract position information
        x = zone_element.get('x')
        y = zone_element.get('y')
        w = zone_element.get('w')
        h = zone_element.get('h')

        if all([x, y, w, h]):
            container.position = {
                'x': int(x),
                'y': int(y),
                'width': int(w),
                'height': int(h)
            }

        # Extract zone name (for worksheet zones)
        zone_name = zone_element.get('zone-name')
        if zone_name:
            container.zone_name = zone_name

        # Check if this is a worksheet reference
        worksheet_name = zone_element.get('name')
        if worksheet_name and container_type == 'layout-basic':
            container.worksheet_name = worksheet_name

        # Recursively extract child zones
        for child_zone in zone_element.findall('zone'):
            child_container = self._extract_container(child_zone, zone_id)
            if child_container:
                container.children.append(child_container)

        return container

    def extract_zones(self, worksheet_element) -> List[ZoneHierarchy]:
        """
        Extract zone hierarchies from a worksheet.

        Zones represent the shelves in Tableau (rows, columns, filters, pages, etc.)
        and their field organization.

        Args:
            worksheet_element: lxml Element representing a worksheet

        Returns:
            List of ZoneHierarchy objects
        """
        zones = []

        # Find the table element which contains zone definitions
        table = worksheet_element.find('.//table')
        if table is None:
            return zones

        # Extract panes/zone information
        panes = table.find('.//panes')
        if panes is not None:
            for pane in panes.findall('pane'):
                pane_zones = self._extract_pane_zones(pane)
                zones.extend(pane_zones)

        # Extract rows zone
        rows = table.find('.//rows')
        if rows is not None:
            rows_zone = self._extract_zone_from_shelf(rows, 'rows')
            if rows_zone:
                zones.append(rows_zone)

        # Extract columns zone
        cols = table.find('.//cols')
        if cols is not None:
            cols_zone = self._extract_zone_from_shelf(cols, 'columns')
            if cols_zone:
                zones.append(cols_zone)

        # Extract filters
        filter_shelf = worksheet_element.find('.//filters')
        if filter_shelf is not None:
            filter_zone = self._extract_zone_from_shelf(filter_shelf, 'filters')
            if filter_zone:
                zones.append(filter_zone)

        # Extract pages
        pages = worksheet_element.find('.//pages')
        if pages is not None:
            pages_zone = self._extract_zone_from_shelf(pages, 'pages')
            if pages_zone:
                zones.append(pages_zone)

        # Extract color/size/label/tooltip marks
        marks = worksheet_element.find('.//marks')
        if marks is not None:
            marks_zones = self._extract_marks_zones(marks)
            zones.extend(marks_zones)

        return zones

    def _extract_pane_zones(self, pane_element) -> List[ZoneHierarchy]:
        """Extract zones from a pane element"""
        zones = []
        pane_id = pane_element.get('id', 'unknown')

        # Look for encodings which represent visual encodings
        for encoding in ['rows', 'cols', 'color', 'size', 'text', 'detail']:
            encoding_elem = pane_element.find(f'.//{encoding}')
            if encoding_elem is not None:
                zone = ZoneHierarchy(
                    zone_id=f"{pane_id}_{encoding}",
                    zone_name=encoding,
                    zone_type=encoding
                )

                # Extract field references
                fields = encoding_elem.findall('.//field')
                for field in fields:
                    field_name = field.text or field.get('name')
                    if field_name:
                        zone.fields.append(self._clean_field_name(field_name))

                if zone.fields:
                    zones.append(zone)

        return zones

    def _extract_zone_from_shelf(self, shelf_element, zone_type: str) -> Optional[ZoneHierarchy]:
        """Extract zone information from a shelf element (rows, columns, etc.)"""
        zone = ZoneHierarchy(
            zone_id=f"shelf_{zone_type}",
            zone_name=zone_type,
            zone_type=zone_type
        )

        # Extract fields in this shelf
        fields = shelf_element.findall('.//field')
        position = 0
        for field in fields:
            field_name = field.text or field.get('name')
            if field_name:
                zone.fields.append(self._clean_field_name(field_name))
                position += 1

        if zone.fields:
            return zone
        return None

    def _extract_marks_zones(self, marks_element) -> List[ZoneHierarchy]:
        """Extract marks zones (color, size, label, tooltip, etc.)"""
        zones = []

        # Common mark types
        mark_types = ['color', 'size', 'text', 'label', 'tooltip', 'detail', 'shape', 'path']

        for mark_type in mark_types:
            mark_elem = marks_element.find(f'.//{mark_type}')
            if mark_elem is not None:
                zone = ZoneHierarchy(
                    zone_id=f"marks_{mark_type}",
                    zone_name=mark_type,
                    zone_type='marks',
                    parent_zone='marks'
                )

                # Extract fields
                fields = mark_elem.findall('.//field')
                for field in fields:
                    field_name = field.text or field.get('name')
                    if field_name:
                        zone.fields.append(self._clean_field_name(field_name))

                if zone.fields:
                    zones.append(zone)

        return zones

    def _clean_field_name(self, name: str) -> str:
        """Clean field name by removing Tableau's internal prefixes"""
        if name.startswith('[') and name.endswith(']'):
            return name[1:-1]
        return name
