"""
Filter Configuration Extractor

Extracts filter configurations from Tableau workbooks including
categorical, quantitative, date, and relative date filters.
"""

from typing import List, Optional, Any
from lxml import etree

from ..schema import FilterConfig, FilterType


class FilterExtractor:
    """
    Extracts filter configurations from worksheets and dashboards.

    Handles various filter types:
    - Categorical (dimension filters)
    - Quantitative (measure filters)
    - Date filters
    - Relative date filters
    - Wildcard filters
    - Conditional filters
    - Top N filters
    """

    def extract_worksheet_filters(self, worksheet_element, worksheet_name: str) -> List[FilterConfig]:
        """
        Extract all filters from a worksheet.

        Args:
            worksheet_element: lxml Element representing a worksheet
            worksheet_name: Name of the worksheet

        Returns:
            List of FilterConfig objects
        """
        filters = []

        # Find filter shelf
        filter_shelf = worksheet_element.find('.//filters')
        if filter_shelf is None:
            return filters

        # Process each filter
        for filter_elem in filter_shelf.findall('.//filter'):
            filter_config = self._extract_filter(filter_elem, worksheet=worksheet_name)
            if filter_config:
                filters.append(filter_config)

        return filters

    def extract_dashboard_filters(self, dashboard_element, dashboard_name: str) -> List[FilterConfig]:
        """
        Extract filters from a dashboard.

        Args:
            dashboard_element: lxml Element representing a dashboard
            dashboard_name: Name of the dashboard

        Returns:
            List of FilterConfig objects
        """
        filters = []

        # Find zones with filters
        zones = dashboard_element.find('.//zones')
        if zones is None:
            return filters

        # Look for filter zones
        for zone in zones.findall('.//zone'):
            zone_type = zone.get('type')
            if zone_type == 'filter':
                filter_config = self._extract_dashboard_filter_zone(zone, dashboard_name)
                if filter_config:
                    filters.append(filter_config)

        return filters

    def _extract_filter(self, filter_elem, worksheet: Optional[str] = None,
                       dashboard: Optional[str] = None) -> Optional[FilterConfig]:
        """
        Extract filter configuration from a filter element.

        Args:
            filter_elem: lxml Element representing a filter
            worksheet: Name of the worksheet (if applicable)
            dashboard: Name of the dashboard (if applicable)

        Returns:
            FilterConfig object or None
        """
        # Get filter field name
        field_name = filter_elem.get('column') or filter_elem.get('name')
        if not field_name:
            return None

        field_name = self._clean_field_name(field_name)

        # Generate filter ID
        filter_id = f"{worksheet or dashboard}_{field_name}".replace(' ', '_')

        # Determine filter type
        filter_type = self._determine_filter_type(filter_elem)

        # Create filter config
        filter_config = FilterConfig(
            filter_id=filter_id,
            field_name=field_name,
            filter_type=filter_type,
            worksheet=worksheet,
            dashboard=dashboard
        )

        # Check if it's a global filter
        is_global = filter_elem.get('global') == 'true'
        filter_config.is_global = is_global

        # Extract filter values based on type
        if filter_type in [FilterType.CATEGORICAL, FilterType.WILDCARD]:
            filter_config.filter_values = self._extract_categorical_values(filter_elem)
            filter_config.exclude_values = self._extract_excluded_values(filter_elem)

        elif filter_type == FilterType.QUANTITATIVE:
            filter_config.condition = self._extract_quantitative_condition(filter_elem)

        elif filter_type == FilterType.DATE:
            filter_config.filter_values = self._extract_date_values(filter_elem)

        elif filter_type == FilterType.RELATIVE_DATE:
            filter_config.relative_date_period = self._extract_relative_date_period(filter_elem)

        elif filter_type == FilterType.TOP_N:
            filter_config.top_n_value = self._extract_top_n_value(filter_elem)
            filter_config.condition = self._extract_top_n_condition(filter_elem)

        elif filter_type == FilterType.CONDITION:
            filter_config.condition = self._extract_condition(filter_elem)

        # Extract UI settings
        filter_config.allow_customization = filter_elem.get('customizable') == 'true'
        filter_config.show_controls = filter_elem.get('show-controls') != 'false'

        return filter_config

    def _extract_dashboard_filter_zone(self, zone_elem, dashboard_name: str) -> Optional[FilterConfig]:
        """Extract filter from a dashboard zone"""
        # Find the filter element within the zone
        filter_elem = zone_elem.find('.//filter')
        if filter_elem is None:
            return None

        return self._extract_filter(filter_elem, dashboard=dashboard_name)

    def _determine_filter_type(self, filter_elem) -> FilterType:
        """Determine the type of filter from the filter element"""
        # Check for explicit filter class
        filter_class = filter_elem.get('class')

        if filter_class == 'categorical':
            return FilterType.CATEGORICAL
        elif filter_class == 'quantitative':
            return FilterType.QUANTITATIVE
        elif filter_class == 'relative-date':
            return FilterType.RELATIVE_DATE

        # Check for wildcard
        if filter_elem.find('.//wildcard') is not None:
            return FilterType.WILDCARD

        # Check for top-n
        if filter_elem.find('.//top') is not None:
            return FilterType.TOP_N

        # Check for condition
        if filter_elem.find('.//condition') is not None:
            return FilterType.CONDITION

        # Check for date range
        if filter_elem.find('.//min-date') is not None or filter_elem.find('.//max-date') is not None:
            return FilterType.DATE

        # Default to categorical
        return FilterType.CATEGORICAL

    def _extract_categorical_values(self, filter_elem) -> Optional[List[Any]]:
        """Extract selected values from a categorical filter"""
        values = []

        # Find groupfilter which contains selected values
        groupfilter = filter_elem.find('.//groupfilter')
        if groupfilter is None:
            return None

        # Extract members
        for member in groupfilter.findall('.//member'):
            value = member.get('value')
            if value:
                values.append(value)

        return values if values else None

    def _extract_excluded_values(self, filter_elem) -> Optional[List[Any]]:
        """Extract excluded values from a categorical filter"""
        # Check for exclusion attribute
        groupfilter = filter_elem.find('.//groupfilter')
        if groupfilter is None:
            return None

        if groupfilter.get('function') == 'except':
            # Values are excluded rather than included
            values = []
            for member in groupfilter.findall('.//member'):
                value = member.get('value')
                if value:
                    values.append(value)
            return values if values else None

        return None

    def _extract_quantitative_condition(self, filter_elem) -> Optional[str]:
        """Extract condition from a quantitative filter"""
        # Find min/max elements
        min_elem = filter_elem.find('.//min')
        max_elem = filter_elem.find('.//max')

        if min_elem is not None and max_elem is not None:
            min_val = min_elem.text
            max_val = max_elem.text
            return f"BETWEEN {min_val} AND {max_val}"
        elif min_elem is not None:
            return f">= {min_elem.text}"
        elif max_elem is not None:
            return f"<= {max_elem.text}"

        return None

    def _extract_date_values(self, filter_elem) -> Optional[List[str]]:
        """Extract date range from a date filter"""
        values = []

        min_date = filter_elem.find('.//min-date')
        max_date = filter_elem.find('.//max-date')

        if min_date is not None:
            values.append(f"min: {min_date.text}")
        if max_date is not None:
            values.append(f"max: {max_date.text}")

        return values if values else None

    def _extract_relative_date_period(self, filter_elem) -> Optional[str]:
        """Extract relative date period"""
        period = filter_elem.get('period')
        if period:
            return period

        # Look for relative date element
        rel_date = filter_elem.find('.//relative-date')
        if rel_date is not None:
            period = rel_date.get('period')
            quantity = rel_date.get('quantity')
            if period and quantity:
                return f"{quantity} {period}"

        return None

    def _extract_top_n_value(self, filter_elem) -> Optional[int]:
        """Extract N value from a top-N filter"""
        top_elem = filter_elem.find('.//top')
        if top_elem is not None:
            n = top_elem.get('n')
            if n:
                return int(n)
        return None

    def _extract_top_n_condition(self, filter_elem) -> Optional[str]:
        """Extract condition from a top-N filter"""
        top_elem = filter_elem.find('.//top')
        if top_elem is not None:
            by = top_elem.get('by')
            direction = top_elem.get('direction', 'top')
            if by:
                return f"{direction} by {by}"
        return None

    def _extract_condition(self, filter_elem) -> Optional[str]:
        """Extract condition from a conditional filter"""
        condition_elem = filter_elem.find('.//condition')
        if condition_elem is not None:
            formula = condition_elem.get('formula')
            if formula:
                return formula

            # Try to construct from operator and value
            operator = condition_elem.get('op')
            value = condition_elem.get('value')
            if operator and value:
                return f"{operator} {value}"

        return None

    def _clean_field_name(self, name: str) -> str:
        """Clean field name by removing Tableau's internal prefixes"""
        if name.startswith('[') and name.endswith(']'):
            return name[1:-1]
        return name
