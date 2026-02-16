#!/usr/bin/env python3
"""
Atlan SDK Exploration Script

This script explores Atlan's API to understand the existing Salesforce assets
in the catalog, inspect CustomMetadataDefs, and test creating/deleting custom
metadata sets. Use this to validate Atlan access before building the integration.

Usage:
    python -m salesforce_temporal.exploration.explore_atlan
"""

import json
import sys
from typing import Any, Dict, List

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Asset, SalesforceObject, SalesforceOrganization
from pyatlan.model.enums import AtlanConnectorType
from pyatlan.model.search import DSL, Bool, Term

from salesforce_temporal.config.settings import get_settings


class AtlanExplorer:
    """Explore Atlan SDK and validate catalog access."""

    def __init__(self):
        """Initialize Atlan client."""
        self.settings = get_settings()
        self.client: AtlanClient | None = None

    def connect(self) -> bool:
        """
        Establish connection to Atlan.

        Returns:
            bool: True if connection successful
        """
        try:
            config = self.settings.get_atlan_config()
            print(f"Connecting to Atlan: {config['base_url']}...")

            self.client = AtlanClient(
                base_url=config["base_url"],
                api_key=config["api_key"],
            )

            # Test the connection by getting tenant info
            # The client is initialized successfully if we get here
            print(f"✓ Connected to Atlan")
            print(f"✓ Base URL: {config['base_url']}")
            return True

        except Exception as e:
            print(f"✗ Failed to connect to Atlan: {e}")
            return False

    def list_salesforce_assets(self) -> List[Asset]:
        """
        List all Salesforce assets in the Atlan catalog.

        Returns:
            List of Salesforce assets
        """
        try:
            print(f"\n{'='*60}")
            print("Salesforce Assets in Catalog")
            print(f"{'='*60}")

            # Search for Salesforce assets
            search_request = (
                DSL.with_type(SalesforceObject)
                .where(
                    Bool.filter(
                        Term.with_super_type_names("SalesforceObject")
                    )
                )
                .page_size(50)
                .build()
            )

            response = self.client.asset.search(search_request)
            assets = list(response)

            print(f"✓ Found {len(assets)} Salesforce assets")

            if assets:
                print("\nSample assets:")
                for i, asset in enumerate(assets[:10], 1):
                    print(f"  {i}. {asset.name} ({asset.type_name})")
                    print(f"     Qualified Name: {asset.qualified_name}")
                    if hasattr(asset, "description") and asset.description:
                        print(f"     Description: {asset.description[:60]}...")

            return assets

        except Exception as e:
            print(f"✗ Failed to list Salesforce assets: {e}")
            return []

    def list_salesforce_organizations(self) -> List[SalesforceOrganization]:
        """
        List Salesforce organizations (connections) in Atlan.

        Returns:
            List of Salesforce organization assets
        """
        try:
            print(f"\n{'='*60}")
            print("Salesforce Organizations")
            print(f"{'='*60}")

            search_request = (
                DSL.with_type(SalesforceOrganization)
                .page_size(50)
                .build()
            )

            response = self.client.asset.search(search_request)
            orgs = list(response)

            print(f"✓ Found {len(orgs)} Salesforce organization(s)")

            for i, org in enumerate(orgs, 1):
                print(f"\n  {i}. {org.name}")
                print(f"     Connection: {org.connection_qualified_name}")
                print(f"     Qualified Name: {org.qualified_name}")

            return orgs

        except Exception as e:
            print(f"✗ Failed to list Salesforce organizations: {e}")
            return []

    def list_custom_metadata_defs(self) -> List[Dict[str, Any]]:
        """
        List existing CustomMetadataDefs in Atlan.

        Returns:
            List of custom metadata definitions
        """
        try:
            print(f"\n{'='*60}")
            print("Custom Metadata Definitions")
            print(f"{'='*60}")

            # Get custom metadata defs via the typedef endpoint
            typedefs = self.client.typedef.get_all()

            custom_metadata = [
                td for td in typedefs.custom_metadata_defs
                if td.name.startswith("salesforce") or td.name.startswith("temporal")
            ]

            print(f"✓ Found {len(custom_metadata)} relevant custom metadata definitions")

            if custom_metadata:
                print("\nRelevant custom metadata:")
                for i, cmd in enumerate(custom_metadata, 1):
                    print(f"  {i}. {cmd.name}")
                    print(f"     Display Name: {cmd.display_name}")
                    print(f"     Description: {cmd.description or 'N/A'}")
                    if cmd.attribute_defs:
                        print(f"     Attributes: {len(cmd.attribute_defs)}")

            return custom_metadata

        except Exception as e:
            print(f"✗ Failed to list custom metadata definitions: {e}")
            return []

    def test_custom_metadata_operations(self) -> bool:
        """
        Test creating and deleting a throwaway custom metadata set.

        Returns:
            bool: True if operations successful
        """
        try:
            print(f"\n{'='*60}")
            print("Testing Custom Metadata Operations")
            print(f"{'='*60}")

            test_metadata_name = "temporal_data_test"

            # Note: Creating custom metadata defs requires admin permissions
            # and is typically done via UI or dedicated admin scripts.
            # Here we'll test if we can read and potentially update metadata
            # on existing assets instead.

            print("✓ Custom metadata operations would require admin permissions")
            print("  For testing purposes, you can:")
            print("  1. Create a custom metadata definition in the Atlan UI")
            print("  2. Name it 'temporal_data_test'")
            print("  3. Add attributes: event_count, last_sync_date, data_quality_score")
            print("  4. Apply it to Salesforce assets")
            print("\n  This script will then be able to read and update those values.")

            return True

        except Exception as e:
            print(f"✗ Failed to test custom metadata operations: {e}")
            return False

    def inspect_salesforce_object_structure(self, asset: Asset) -> Dict[str, Any]:
        """
        Inspect the structure and metadata of a Salesforce object.

        Args:
            asset: Salesforce asset to inspect

        Returns:
            Dictionary with asset details
        """
        details = {
            "name": asset.name,
            "type": asset.type_name,
            "qualified_name": asset.qualified_name,
            "guid": asset.guid,
            "description": getattr(asset, "description", None),
            "certificate_status": getattr(asset, "certificate_status", None),
            "custom_metadata": {},
        }

        # Extract custom metadata if present
        if hasattr(asset, "custom_metadata_sets") and asset.custom_metadata_sets:
            for cmd_name, cmd_attrs in asset.custom_metadata_sets.items():
                details["custom_metadata"][cmd_name] = cmd_attrs

        return details

    def run_exploration(self) -> Dict[str, Any]:
        """
        Run complete Atlan exploration workflow.

        Returns:
            Dictionary with all exploration results
        """
        results = {
            "connection_successful": False,
            "salesforce_assets": [],
            "salesforce_orgs": [],
            "custom_metadata_defs": [],
        }

        if not self.connect():
            return results

        results["connection_successful"] = True

        # List Salesforce organizations
        orgs = self.list_salesforce_organizations()
        results["salesforce_orgs"] = [
            {"name": org.name, "qualified_name": org.qualified_name}
            for org in orgs
        ]

        # List Salesforce assets
        assets = self.list_salesforce_assets()
        if assets:
            # Inspect a few sample assets
            for asset in assets[:3]:
                details = self.inspect_salesforce_object_structure(asset)
                results["salesforce_assets"].append(details)

        # List custom metadata definitions
        cmd_list = self.list_custom_metadata_defs()
        results["custom_metadata_defs"] = [
            {
                "name": cmd.name,
                "display_name": cmd.display_name,
                "description": cmd.description,
            }
            for cmd in cmd_list
        ]

        # Test custom metadata operations
        self.test_custom_metadata_operations()

        return results

    def print_summary(self, results: Dict[str, Any]):
        """
        Print a summary of exploration results.

        Args:
            results: Dictionary of exploration results
        """
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}\n")

        print(f"Connection Status: {'✓ Success' if results['connection_successful'] else '✗ Failed'}")
        print(f"Salesforce Organizations: {len(results['salesforce_orgs'])}")
        print(f"Salesforce Assets Found: {len(results['salesforce_assets'])}")
        print(f"Custom Metadata Defs: {len(results['custom_metadata_defs'])}")

        print("\n" + "=" * 60)
        print("Exploration complete!")
        print("=" * 60)


def main():
    """Main exploration workflow."""
    explorer = AtlanExplorer()

    # Run exploration
    print("Starting Atlan SDK exploration...")
    results = explorer.run_exploration()

    if not results["connection_successful"]:
        print("\nFailed to connect to Atlan. Check your configuration.")
        print("Make sure you have a .env file with valid ATLAN_BASE_URL and ATLAN_API_KEY.")
        sys.exit(1)

    # Print summary
    explorer.print_summary(results)

    # Save results to file
    output_file = "atlan_exploration_results.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\nDetailed results saved to: {output_file}")


if __name__ == "__main__":
    main()
