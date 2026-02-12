"""
Calculated Field and LOD Expression Extractor

Extracts calculated field formulas, LOD expressions, and their metadata
from Tableau workbooks.
"""

import re
from typing import List, Optional, Tuple
from lxml import etree

from ..schema import CalculatedField, LODType


class CalculatedFieldExtractor:
    """
    Extracts calculated fields and LOD expressions from Tableau XML.

    LOD (Level of Detail) expressions include:
    - FIXED: Computes values at a specified level of detail
    - INCLUDE: Computes values at a finer level of detail
    - EXCLUDE: Computes values at a coarser level of detail
    """

    LOD_PATTERNS = {
        LODType.FIXED: re.compile(r'\{FIXED\s+([^:]+):\s*(.+?)\}', re.IGNORECASE | re.DOTALL),
        LODType.INCLUDE: re.compile(r'\{INCLUDE\s+([^:]+):\s*(.+?)\}', re.IGNORECASE | re.DOTALL),
        LODType.EXCLUDE: re.compile(r'\{EXCLUDE\s+([^:]+):\s*(.+?)\}', re.IGNORECASE | re.DOTALL)
    }

    def extract(self, worksheet_element) -> List[CalculatedField]:
        """
        Extract all calculated fields from a worksheet.

        Args:
            worksheet_element: lxml Element representing a worksheet

        Returns:
            List of CalculatedField objects
        """
        calc_fields = []

        # Look for column elements with calculations
        columns = worksheet_element.findall('.//column')
        for column in columns:
            calc_field = self._extract_column_calculation(column)
            if calc_field:
                calc_fields.append(calc_field)

        # Also check in datasource definitions
        datasources = worksheet_element.findall('.//datasource')
        for datasource in datasources:
            ds_calc_fields = self._extract_datasource_calculations(datasource)
            calc_fields.extend(ds_calc_fields)

        return calc_fields

    def _extract_column_calculation(self, column_element) -> Optional[CalculatedField]:
        """Extract calculation from a column element"""
        # Get column name
        name = column_element.get('name')
        if not name:
            return None

        # Check for calculation element
        calculation = column_element.find('calculation')
        if calculation is None:
            return None

        formula = calculation.get('formula')
        if not formula:
            return None

        # Create calculated field
        calc_field = CalculatedField(
            name=self._clean_field_name(name),
            formula=formula
        )

        # Extract additional metadata
        calc_field.caption = column_element.get('caption')
        calc_field.datatype = column_element.get('datatype')
        calc_field.role = column_element.get('role')
        calc_field.type_ = column_element.get('type')
        calc_field.hidden = column_element.get('hidden') == 'true'

        # Check for comment
        comment = calculation.get('comment')
        if comment:
            calc_field.comment = comment

        # Check if it's a LOD expression
        lod_info = self._parse_lod_expression(formula)
        if lod_info:
            calc_field.is_lod = True
            calc_field.lod_type = lod_info[0]
            calc_field.lod_scope = lod_info[1]

        return calc_field

    def _extract_datasource_calculations(self, datasource_element) -> List[CalculatedField]:
        """Extract calculated fields defined in a datasource"""
        calc_fields = []

        columns = datasource_element.findall('.//column')
        for column in columns:
            calc_field = self._extract_column_calculation(column)
            if calc_field:
                calc_fields.append(calc_field)

        return calc_fields

    def _parse_lod_expression(self, formula: str) -> Optional[Tuple[LODType, List[str]]]:
        """
        Parse LOD expression to extract type and scope.

        Args:
            formula: Calculation formula string

        Returns:
            Tuple of (LODType, scope_fields) or None if not a LOD expression
        """
        for lod_type, pattern in self.LOD_PATTERNS.items():
            match = pattern.search(formula)
            if match:
                scope_str = match.group(1).strip()
                # Parse scope fields (comma-separated) and clean them
                scope_fields = [self._clean_field_name(f.strip()) for f in scope_str.split(',')]
                return (lod_type, scope_fields)

        return None

    def _clean_field_name(self, name: str) -> str:
        """
        Clean field name by removing Tableau's internal prefixes.

        Examples:
            [Calculation_123456789] -> Calculation_123456789
            [abc] -> abc
        """
        if name.startswith('[') and name.endswith(']'):
            return name[1:-1]
        return name

    def extract_from_datasource(self, datasource_element) -> List[CalculatedField]:
        """
        Extract calculated fields directly from a datasource element.

        This is useful when parsing datasource definitions separately.
        """
        return self._extract_datasource_calculations(datasource_element)
