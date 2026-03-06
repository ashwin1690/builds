"""
Atlan Catalog Enricher

Validates advisory signals against the Atlan data catalog to determine
whether referenced assets already have adequate documentation, ownership,
and glossary coverage. Reduces priority for signals that reference
well-documented assets and boosts priority for undocumented ones.

Uses the same PyAtlan SDK patterns as dq_enrichment.py for catalog lookups.

Environment Variables:
    ATLAN_API_KEY: Atlan API authentication key
    ATLAN_BASE_URL: Atlan instance base URL
"""

import logging
import os
from typing import Dict, List, Optional

from connector_base import SignalEnricher
from enriched_signals import CatalogValidation, EnrichedAdvisorySignal

logger = logging.getLogger(__name__)

# Try to import PyAtlan
try:
    from pyatlan.client.atlan import AtlanClient
    from pyatlan.model.assets import Asset, Column, Table
    from pyatlan.model.search import DSL, Bool, Term

    HAS_PYATLAN = True
except ImportError:
    HAS_PYATLAN = False
    logger.debug("PyAtlan not installed - Atlan enrichment will be unavailable")


class AtlanCatalogEnricher(SignalEnricher):
    """
    Enriches advisory signals by validating assets against the Atlan catalog.

    For each asset referenced in a signal, checks:
    - Does the asset exist in Atlan?
    - Does it have a description?
    - Does it have assigned owners?
    - Does it have glossary terms linked?
    - What is its certificate status?

    Signals referencing fully-documented assets get reduced composite_priority
    (the advisory is confirming existing docs, not revealing a gap).
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        self.api_key = api_key or os.environ.get("ATLAN_API_KEY", "")
        self.base_url = base_url or os.environ.get("ATLAN_BASE_URL", "")
        self._client = None

    def is_available(self) -> bool:
        if not HAS_PYATLAN:
            return False
        return bool(self.api_key and self.base_url)

    def _get_client(self) -> "AtlanClient":
        if self._client is None:
            self._client = AtlanClient(api_key=self.api_key, base_url=self.base_url)
        return self._client

    def enrich(self, signals: List[EnrichedAdvisorySignal]) -> List[EnrichedAdvisorySignal]:
        """Validate advisory signal assets against Atlan catalog."""
        # Collect unique asset names
        all_assets = set()
        for signal in signals:
            all_assets.update(signal.base_signal.assets_referenced)

        if not all_assets:
            return signals

        # Search Atlan for these assets
        asset_map = self._search_assets(list(all_assets))

        # Apply catalog validation to each signal
        for signal in signals:
            validations = []
            for asset_name in signal.base_signal.assets_referenced:
                atlan_asset = asset_map.get(asset_name.lower())
                if atlan_asset:
                    validation = self._validate_asset(atlan_asset)
                else:
                    validation = CatalogValidation(asset_exists_in_atlan=False)
                validations.append(validation)

            # Use the worst (most gappy) validation as the signal's catalog state
            if validations:
                signal.catalog_validation = min(
                    validations, key=lambda v: v.completeness_score
                )

        return signals

    def _search_assets(self, asset_names: List[str]) -> Dict[str, "Asset"]:
        """
        Search Atlan catalog for assets by name.

        Uses DSL queries with Bool.should for OR-matching multiple asset names.
        Returns dict mapping normalized asset name to Asset object.
        """
        client = self._get_client()
        result: Dict[str, Asset] = {}

        # Search in batches of 50
        for i in range(0, len(asset_names), 50):
            batch = asset_names[i:i + 50]
            terms = [Term(field="name.keyword", value=name) for name in batch]

            dsl = DSL(
                query=Bool(should=terms, minimum_should_match=1),
                size=min(len(batch) * 2, 100),  # allow for multiple matches
            )

            try:
                response = client.asset.search(dsl)
                for asset in response:
                    name_lower = asset.name.lower() if asset.name else ""
                    if name_lower not in result:
                        result[name_lower] = asset
            except Exception as e:
                logger.warning(f"Atlan search failed for batch: {e}")

        return result

    def _validate_asset(self, asset: "Asset") -> CatalogValidation:
        """Check documentation completeness of an Atlan asset."""
        validation = CatalogValidation(
            asset_exists_in_atlan=True,
            asset_guid=getattr(asset, "guid", None),
            asset_qualified_name=getattr(asset, "qualified_name", None),
        )

        # Check description
        desc = getattr(asset, "description", None) or getattr(asset, "user_description", None)
        validation.has_description = bool(desc and len(str(desc).strip()) > 10)

        # Check ownership
        owner_users = getattr(asset, "owner_users", None)
        owner_groups = getattr(asset, "owner_groups", None)
        validation.has_owner = bool(
            (owner_users and len(owner_users) > 0)
            or (owner_groups and len(owner_groups) > 0)
        )

        # Check glossary terms
        meanings = getattr(asset, "meanings", None)
        validation.has_glossary_terms = bool(meanings and len(meanings) > 0)

        # Check certificate status
        cert = getattr(asset, "certificate_status", None)
        validation.certificate_status = str(cert) if cert else None

        # Determine if enrichment is already comprehensive
        validation.enrichment_already_applied = (
            validation.has_description and validation.has_owner
        )

        return validation
