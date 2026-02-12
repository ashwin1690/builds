"""
Dashboard Actions Extractor

Extracts dashboard actions (filter, highlight, URL, navigation, parameter changes)
from Tableau workbooks.
"""

from typing import List, Optional
from lxml import etree

from ..schema import DashboardAction, ActionType


class ActionExtractor:
    """
    Extracts dashboard actions from Tableau XML.

    Action types:
    - Filter: Cross-sheet filtering
    - Highlight: Cross-sheet highlighting
    - URL: Open URL with parameter passing
    - Go to Sheet: Navigate to another sheet
    - Change Parameter: Modify parameter values
    - Change Set Values: Modify set membership
    """

    def extract(self, dashboard_element, dashboard_name: str) -> List[DashboardAction]:
        """
        Extract all actions from a dashboard.

        Args:
            dashboard_element: lxml Element representing a dashboard
            dashboard_name: Name of the dashboard

        Returns:
            List of DashboardAction objects
        """
        actions = []

        # Find actions element
        actions_elem = dashboard_element.find('.//actions')
        if actions_elem is None:
            return actions

        # Process each action type
        for action_elem in actions_elem:
            action = self._extract_action(action_elem, dashboard_name)
            if action:
                actions.append(action)

        return actions

    def _extract_action(self, action_elem, dashboard_name: str) -> Optional[DashboardAction]:
        """
        Extract a single action from an action element.

        Args:
            action_elem: lxml Element representing an action
            dashboard_name: Name of the dashboard

        Returns:
            DashboardAction object or None
        """
        action_name = action_elem.get('name')
        if not action_name:
            return None

        # Determine action type from tag name
        action_type = self._determine_action_type(action_elem.tag)
        if not action_type:
            return None

        # Generate action ID
        action_id = f"{dashboard_name}_{action_name}".replace(' ', '_')

        # Create action
        action = DashboardAction(
            action_id=action_id,
            action_name=action_name,
            action_type=action_type
        )

        # Extract enabled status
        enabled = action_elem.get('enabled')
        if enabled is not None:
            action.enabled = enabled.lower() != 'false'

        # Extract source sheets
        source = action_elem.find('.//source')
        if source is not None:
            source_sheets = self._extract_sheets(source)
            action.source_sheets = source_sheets

        # Extract target sheets
        target = action_elem.find('.//target')
        if target is not None:
            target_sheets = self._extract_sheets(target)
            action.target_sheets = target_sheets

        # Extract action-specific details
        if action_type == ActionType.FILTER:
            action.fields = self._extract_filter_fields(action_elem)

        elif action_type == ActionType.HIGHLIGHT:
            action.fields = self._extract_highlight_fields(action_elem)

        elif action_type == ActionType.URL:
            action.url_template = self._extract_url_template(action_elem)
            action.fields = self._extract_url_fields(action_elem)

        elif action_type == ActionType.GO_TO_SHEET:
            # Target sheets already extracted above
            pass

        elif action_type == ActionType.CHANGE_PARAMETER:
            action.parameter_name = self._extract_parameter_name(action_elem)
            action.fields = self._extract_parameter_fields(action_elem)

        elif action_type == ActionType.CHANGE_SET_VALUES:
            action.fields = self._extract_set_fields(action_elem)

        return action

    def _determine_action_type(self, tag_name: str) -> Optional[ActionType]:
        """Determine action type from XML tag name"""
        tag_lower = tag_name.lower()

        if 'filter' in tag_lower:
            return ActionType.FILTER
        elif 'highlight' in tag_lower:
            return ActionType.HIGHLIGHT
        elif 'url' in tag_lower:
            return ActionType.URL
        elif 'sheet' in tag_lower or 'navigate' in tag_lower:
            return ActionType.GO_TO_SHEET
        elif 'parameter' in tag_lower:
            return ActionType.CHANGE_PARAMETER
        elif 'set' in tag_lower:
            return ActionType.CHANGE_SET_VALUES

        return None

    def _extract_sheets(self, element) -> List[str]:
        """Extract sheet names from source or target element"""
        sheets = []

        # Look for worksheet references
        for worksheet in element.findall('.//worksheet'):
            name = worksheet.get('name')
            if name:
                sheets.append(name)

        # Also check for dashboard references
        for dashboard in element.findall('.//dashboard'):
            name = dashboard.get('name')
            if name:
                sheets.append(name)

        # If no specific sheets, check for "all" attribute
        if not sheets:
            if element.get('all') == 'true':
                sheets.append('*')  # Indicates all sheets

        return sheets

    def _extract_filter_fields(self, action_elem) -> List[str]:
        """Extract filter fields from a filter action"""
        fields = []

        # Find filter element
        filter_elem = action_elem.find('.//filter')
        if filter_elem is not None:
            # Extract field references
            for field in filter_elem.findall('.//field'):
                field_name = field.get('name') or field.text
                if field_name:
                    fields.append(self._clean_field_name(field_name))

        return fields

    def _extract_highlight_fields(self, action_elem) -> List[str]:
        """Extract highlight fields from a highlight action"""
        fields = []

        # Find highlight element
        highlight_elem = action_elem.find('.//highlight')
        if highlight_elem is not None:
            for field in highlight_elem.findall('.//field'):
                field_name = field.get('name') or field.text
                if field_name:
                    fields.append(self._clean_field_name(field_name))

        return fields

    def _extract_url_template(self, action_elem) -> Optional[str]:
        """Extract URL template from a URL action"""
        url_elem = action_elem.find('.//url')
        if url_elem is not None:
            url = url_elem.text or url_elem.get('value')
            return url

        return None

    def _extract_url_fields(self, action_elem) -> List[str]:
        """Extract fields used in URL parameters"""
        fields = []

        # URL actions may reference fields in the URL template
        url_elem = action_elem.find('.//url')
        if url_elem is not None:
            # Look for field references in URL encode elements
            for encode in url_elem.findall('.//url-encode'):
                field_name = encode.get('field')
                if field_name:
                    fields.append(self._clean_field_name(field_name))

        return fields

    def _extract_parameter_name(self, action_elem) -> Optional[str]:
        """Extract parameter name from a change parameter action"""
        param_elem = action_elem.find('.//parameter')
        if param_elem is not None:
            param_name = param_elem.get('name')
            if param_name:
                return self._clean_field_name(param_name)

        return None

    def _extract_parameter_fields(self, action_elem) -> List[str]:
        """Extract source fields for parameter action"""
        fields = []

        param_elem = action_elem.find('.//parameter')
        if param_elem is not None:
            # Look for source field
            source_field = param_elem.get('source-field')
            if source_field:
                fields.append(self._clean_field_name(source_field))

        return fields

    def _extract_set_fields(self, action_elem) -> List[str]:
        """Extract set fields from a change set values action"""
        fields = []

        set_elem = action_elem.find('.//set')
        if set_elem is not None:
            set_name = set_elem.get('name')
            if set_name:
                fields.append(self._clean_field_name(set_name))

        return fields

    def _clean_field_name(self, name: str) -> str:
        """Clean field name by removing Tableau's internal prefixes"""
        if name.startswith('[') and name.endswith(']'):
            return name[1:-1]
        return name
