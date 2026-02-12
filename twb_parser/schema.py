"""
Metadata Output Schema for Tableau Workbook Extraction

JSON-LD compatible schema for representing deep metadata extracted from Tableau workbooks.
Compatible with RDF and semantic web standards.
"""

from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum


class LODType(Enum):
    """Level of Detail expression types"""
    FIXED = "FIXED"
    INCLUDE = "INCLUDE"
    EXCLUDE = "EXCLUDE"


class ActionType(Enum):
    """Dashboard action types"""
    FILTER = "filter"
    HIGHLIGHT = "highlight"
    URL = "url"
    GO_TO_SHEET = "go_to_sheet"
    CHANGE_PARAMETER = "change_parameter"
    CHANGE_SET_VALUES = "change_set_values"


class FilterType(Enum):
    """Filter types in Tableau"""
    CATEGORICAL = "categorical"
    QUANTITATIVE = "quantitative"
    DATE = "date"
    RELATIVE_DATE = "relative_date"
    WILDCARD = "wildcard"
    CONDITION = "condition"
    TOP_N = "top_n"


@dataclass
class CalculatedField:
    """Represents a calculated field or LOD expression"""
    name: str
    formula: str
    caption: Optional[str] = None
    datatype: Optional[str] = None
    role: Optional[str] = None
    type_: Optional[str] = None  # regular, quantitative, ordinal, nominal
    is_lod: bool = False
    lod_type: Optional[LODType] = None
    lod_scope: Optional[List[str]] = None
    comment: Optional[str] = None
    hidden: bool = False

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        if self.lod_type:
            result['lod_type'] = self.lod_type.value
        return result


@dataclass
class LayoutContainer:
    """Represents a layout container in a dashboard"""
    container_id: str
    container_type: str  # vertical, horizontal, grid, etc.
    title: Optional[str] = None
    position: Optional[Dict[str, int]] = None  # x, y, width, height
    children: List['LayoutContainer'] = field(default_factory=list)
    zone_name: Optional[str] = None
    worksheet_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        return result


@dataclass
class ZoneHierarchy:
    """Represents zone hierarchy in a worksheet or dashboard"""
    zone_id: str
    zone_name: str
    zone_type: str  # rows, columns, pages, filters, marks, etc.
    parent_zone: Optional[str] = None
    fields: List[str] = field(default_factory=list)
    position: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FilterConfig:
    """Represents a filter configuration"""
    filter_id: str
    field_name: str
    filter_type: FilterType
    worksheet: Optional[str] = None
    dashboard: Optional[str] = None
    is_global: bool = False
    filter_values: Optional[List[Any]] = None
    exclude_values: Optional[List[Any]] = None
    condition: Optional[str] = None
    top_n_value: Optional[int] = None
    relative_date_period: Optional[str] = None
    allow_customization: bool = False
    show_controls: bool = True

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['filter_type'] = self.filter_type.value
        return result


@dataclass
class DashboardAction:
    """Represents a dashboard action"""
    action_id: str
    action_name: str
    action_type: ActionType
    source_sheets: List[str] = field(default_factory=list)
    target_sheets: List[str] = field(default_factory=list)
    fields: List[str] = field(default_factory=list)
    url_template: Optional[str] = None
    parameter_name: Optional[str] = None
    enabled: bool = True

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['action_type'] = self.action_type.value
        return result


@dataclass
class StoryPoint:
    """Represents a story point in a Data Story"""
    point_id: str
    caption: str
    description: Optional[str] = None
    order: int = 0
    worksheet_name: Optional[str] = None
    dashboard_name: Optional[str] = None
    narrative_text: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DataStory:
    """Represents a Tableau Data Story"""
    story_name: str
    story_id: str
    description: Optional[str] = None
    points: List[StoryPoint] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['points'] = [p.to_dict() for p in self.points]
        return result


@dataclass
class WorksheetMetadata:
    """Metadata for a single worksheet"""
    name: str
    caption: Optional[str] = None
    calculated_fields: List[CalculatedField] = field(default_factory=list)
    zones: List[ZoneHierarchy] = field(default_factory=list)
    filters: List[FilterConfig] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'caption': self.caption,
            'calculated_fields': [f.to_dict() for f in self.calculated_fields],
            'zones': [z.to_dict() for z in self.zones],
            'filters': [f.to_dict() for f in self.filters]
        }


@dataclass
class DashboardMetadata:
    """Metadata for a single dashboard"""
    name: str
    caption: Optional[str] = None
    layout_containers: List[LayoutContainer] = field(default_factory=list)
    actions: List[DashboardAction] = field(default_factory=list)
    filters: List[FilterConfig] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'caption': self.caption,
            'layout_containers': [c.to_dict() for c in self.layout_containers],
            'actions': [a.to_dict() for a in self.actions],
            'filters': [f.to_dict() for f in self.filters]
        }


@dataclass
class TableauMetadata:
    """
    Complete metadata extracted from a Tableau workbook.
    JSON-LD compatible structure.
    """
    context: str = "https://schema.org/"
    type_: str = "Dataset"
    workbook_name: str = ""
    version: str = ""
    extracted_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    worksheets: List[WorksheetMetadata] = field(default_factory=list)
    dashboards: List[DashboardMetadata] = field(default_factory=list)
    stories: List[DataStory] = field(default_factory=list)

    data_sources: List[Dict[str, Any]] = field(default_factory=list)
    parameters: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-LD compatible dictionary"""
        return {
            '@context': self.context,
            '@type': self.type_,
            'name': self.workbook_name,
            'version': self.version,
            'dateExtracted': self.extracted_at,
            'worksheets': [w.to_dict() for w in self.worksheets],
            'dashboards': [d.to_dict() for d in self.dashboards],
            'stories': [s.to_dict() for s in self.stories],
            'dataSources': self.data_sources,
            'parameters': self.parameters
        }

    def to_json_ld(self) -> Dict[str, Any]:
        """Alias for to_dict() for clarity"""
        return self.to_dict()
