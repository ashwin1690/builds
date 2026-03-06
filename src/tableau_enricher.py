"""
Tableau Lineage Enricher

Checks advisory signals against parsed Tableau workbook metadata to determine
if dashboards or calculated fields already define or answer the question being asked.

Uses the existing twb_parser module for workbook metadata extraction.

Environment Variables:
    TABLEAU_WORKBOOKS_DIR: Directory containing .twb/.twbx files to scan
"""

import logging
import os
import re
from typing import Dict, List, Optional

from connector_base import SignalEnricher
from enriched_signals import EnrichedAdvisorySignal, TableauContext

logger = logging.getLogger(__name__)

# Try to import Tableau parser
try:
    from twb_parser.parser import TableauWorkbookParser
    from twb_parser.schema import (
        CalculatedField,
        TableauMetadata,
    )

    HAS_TABLEAU = True
except ImportError:
    HAS_TABLEAU = False
    logger.debug("twb_parser not available - Tableau enrichment will be unavailable")


class TableauLineageEnricher(SignalEnricher):
    """
    Enriches advisory signals with Tableau workbook lineage context.

    For each asset referenced in a signal, checks if:
    - Any Tableau dashboard uses that asset as a data source
    - Any calculated field formula references the asset
    - The asset already has a formal definition via calculated field logic

    Key insight: if a DEFINITION_CLARIFICATION or METRIC_INTERPRETATION signal
    asks about a concept that's already defined in a Tableau calculated field,
    the enricher notes that the answer exists but needs to be surfaced in the
    data catalog (not just hidden in Tableau).
    """

    def __init__(
        self,
        workbook_paths: Optional[List[str]] = None,
        metadata_list: Optional[List["TableauMetadata"]] = None,
    ):
        self._workbook_paths = workbook_paths or []
        self._metadata_list = metadata_list or []
        self._asset_index: Optional[Dict[str, dict]] = None

        # Check for directory env var
        workbooks_dir = os.environ.get("TABLEAU_WORKBOOKS_DIR", "")
        if workbooks_dir and os.path.isdir(workbooks_dir):
            for f in os.listdir(workbooks_dir):
                if f.endswith((".twb", ".twbx")):
                    path = os.path.join(workbooks_dir, f)
                    if path not in self._workbook_paths:
                        self._workbook_paths.append(path)

    def is_available(self) -> bool:
        if not HAS_TABLEAU:
            return False
        return bool(self._workbook_paths or self._metadata_list)

    def _parse_workbooks(self) -> List["TableauMetadata"]:
        """Parse all configured workbooks into TableauMetadata objects."""
        if self._metadata_list:
            return self._metadata_list

        parsed = []
        parser = TableauWorkbookParser()

        for path in self._workbook_paths:
            try:
                metadata = parser.parse(path)
                parsed.append(metadata)
                logger.info(f"Parsed Tableau workbook: {path}")
            except Exception as e:
                logger.warning(f"Failed to parse Tableau workbook {path}: {e}")

        self._metadata_list = parsed
        return parsed

    def _build_asset_index(self) -> Dict[str, dict]:
        """
        Build an index mapping asset/keyword names to Tableau context.

        Index keys are normalized names (lowercase). Values contain:
        - dashboards: list of dashboard names that use this asset
        - calc_fields: list of CalculatedField objects that reference this asset
        - data_sources: list of data source names
        """
        if self._asset_index is not None:
            return self._asset_index

        metadata_list = self._parse_workbooks()
        index: Dict[str, dict] = {}

        for metadata in metadata_list:
            workbook_name = metadata.workbook_name

            # Index data sources
            for ds in metadata.data_sources:
                ds_name = ds.get("name", "").lower()
                # Extract table names from connection info
                tables = ds.get("tables", [])
                connection = ds.get("connection", {})

                for table in tables:
                    table_name = table.lower() if isinstance(table, str) else str(table).lower()
                    if table_name not in index:
                        index[table_name] = {"dashboards": [], "calc_fields": [], "data_sources": []}
                    if ds_name not in index[table_name]["data_sources"]:
                        index[table_name]["data_sources"].append(ds_name)

            # Index calculated fields from worksheets
            for worksheet in metadata.worksheets:
                for calc_field in worksheet.calculated_fields:
                    # The calc field name itself is a potential match
                    cf_name = calc_field.name.lower().strip("[]")
                    if cf_name not in index:
                        index[cf_name] = {"dashboards": [], "calc_fields": [], "data_sources": []}
                    index[cf_name]["calc_fields"].append(calc_field)

                    # Also index any table/column references in the formula
                    formula_refs = self._extract_formula_references(calc_field.formula)
                    for ref in formula_refs:
                        ref_lower = ref.lower()
                        if ref_lower not in index:
                            index[ref_lower] = {"dashboards": [], "calc_fields": [], "data_sources": []}
                        if calc_field not in index[ref_lower]["calc_fields"]:
                            index[ref_lower]["calc_fields"].append(calc_field)

            # Index dashboards
            for dashboard in metadata.dashboards:
                dash_name = dashboard.name
                # Associate dashboard with all data sources in the workbook
                for ds in metadata.data_sources:
                    for table in ds.get("tables", []):
                        table_name = table.lower() if isinstance(table, str) else str(table).lower()
                        if table_name in index:
                            if dash_name not in index[table_name]["dashboards"]:
                                index[table_name]["dashboards"].append(dash_name)

        self._asset_index = index
        return index

    def _extract_formula_references(self, formula: str) -> List[str]:
        """Extract table/column references from a Tableau calculated field formula."""
        refs = []

        # Match [TableName].[ColumnName] patterns
        bracket_refs = re.findall(r'\[([^\]]+)\]', formula)
        refs.extend(bracket_refs)

        # Match common table name patterns
        table_refs = re.findall(r'\b((?:dim|fct|stg|raw)_\w+)\b', formula, re.IGNORECASE)
        refs.extend(table_refs)

        return refs

    def enrich(self, signals: List[EnrichedAdvisorySignal]) -> List[EnrichedAdvisorySignal]:
        """Enrich signals with Tableau lineage context."""
        index = self._build_asset_index()

        if not index:
            return signals

        for signal in signals:
            tableau_ctx = TableauContext()

            for asset_name in signal.base_signal.assets_referenced:
                asset_lower = asset_name.lower()
                entry = index.get(asset_lower, {})

                if entry:
                    tableau_ctx.dashboard_names.extend(
                        d for d in entry.get("dashboards", [])
                        if d not in tableau_ctx.dashboard_names
                    )
                    for cf in entry.get("calc_fields", []):
                        cf_info = f"{cf.name}: {cf.formula[:100]}"
                        if cf_info not in tableau_ctx.calculated_fields_that_answer:
                            tableau_ctx.calculated_fields_that_answer.append(cf_info)
                    tableau_ctx.data_source_names.extend(
                        ds for ds in entry.get("data_sources", [])
                        if ds not in tableau_ctx.data_source_names
                    )

            # Check if Tableau already has a formal definition
            if tableau_ctx.calculated_fields_that_answer:
                advisory_type = signal.base_signal.advisory_type.value
                if advisory_type in ("Definition Clarification", "Metric Interpretation"):
                    tableau_ctx.has_existing_definition = True

            if (tableau_ctx.dashboard_names or tableau_ctx.calculated_fields_that_answer
                    or tableau_ctx.data_source_names):
                signal.tableau_context = tableau_ctx

        return signals
