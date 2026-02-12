"""
Tableau Workbook (.twb/.twbx) Deep Metadata Parser

This module extracts contextual metadata from Tableau workbooks that traditional
catalog tools like Atlan often miss, including:
- Layout container titles and hierarchies
- Zone hierarchies
- Calculated field formulas and LOD expressions
- Data Stories narratives
- Filter configurations
- Dashboard actions
"""

from .parser import TableauWorkbookParser
from .schema import TableauMetadata

__version__ = "0.1.0"
__all__ = ["TableauWorkbookParser", "TableauMetadata"]
