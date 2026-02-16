#!/usr/bin/env python3
"""
Salesforce API Exploration Script

This script authenticates with Salesforce and explores the history objects
to validate field availability, data shapes, and record counts. Use this
to understand what's available in a target Salesforce org before building
extractors.

Usage:
    python -m salesforce_temporal.exploration.explore_salesforce
"""

import json
import sys
from typing import Any, Dict, List

from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceError

from salesforce_temporal.config.settings import get_settings


class SalesforceExplorer:
    """Explore Salesforce API and validate data access patterns."""

    def __init__(self):
        """Initialize Salesforce connection."""
        self.settings = get_settings()
        self.sf: Salesforce | None = None

    def connect(self) -> bool:
        """
        Establish connection to Salesforce.

        Returns:
            bool: True if connection successful
        """
        try:
            auth_config = self.settings.get_salesforce_auth_config()
            print(f"Connecting to Salesforce ({self.settings.salesforce_domain})...")

            self.sf = Salesforce(
                username=auth_config["username"],
                password=auth_config.get("password"),
                security_token=auth_config.get("security_token", ""),
                domain=auth_config["domain"],
                version=self.settings.salesforce_api_version,
            )

            # Test the connection
            org_info = self.sf.query("SELECT Name, OrganizationType FROM Organization LIMIT 1")
            org_name = org_info["records"][0]["Name"] if org_info["records"] else "Unknown"
            org_type = (
                org_info["records"][0]["OrganizationType"] if org_info["records"] else "Unknown"
            )

            print(f"✓ Connected to: {org_name} ({org_type})")
            print(f"✓ API Version: {self.settings.salesforce_api_version}")
            return True

        except Exception as e:
            print(f"✗ Failed to connect: {e}")
            return False

    def describe_object(self, object_name: str) -> Dict[str, Any] | None:
        """
        Describe a Salesforce object to understand its structure.

        Args:
            object_name: Name of the Salesforce object

        Returns:
            Dictionary with object metadata, or None if error
        """
        try:
            obj = getattr(self.sf, object_name)
            metadata = obj.describe()
            return metadata
        except Exception as e:
            print(f"✗ Failed to describe {object_name}: {e}")
            return None

    def get_field_names(self, object_name: str) -> List[str]:
        """
        Get all field names for an object.

        Args:
            object_name: Name of the Salesforce object

        Returns:
            List of field names
        """
        metadata = self.describe_object(object_name)
        if not metadata:
            return []

        return [field["name"] for field in metadata.get("fields", [])]

    def query_sample(self, object_name: str, limit: int = 5) -> Dict[str, Any]:
        """
        Query sample records from an object.

        Args:
            object_name: Name of the Salesforce object
            limit: Number of records to retrieve

        Returns:
            Query results dictionary
        """
        try:
            fields = self.get_field_names(object_name)
            if not fields:
                return {"totalSize": 0, "records": []}

            # Limit fields for readability
            display_fields = fields[:20]
            field_list = ", ".join(display_fields)

            query = f"SELECT {field_list} FROM {object_name} ORDER BY CreatedDate DESC LIMIT {limit}"
            results = self.sf.query(query)
            return results

        except SalesforceError as e:
            print(f"✗ Query failed for {object_name}: {e}")
            return {"totalSize": 0, "records": []}

    def get_record_count(self, object_name: str) -> int:
        """
        Get total record count for an object.

        Args:
            object_name: Name of the Salesforce object

        Returns:
            Total record count
        """
        try:
            result = self.sf.query(f"SELECT COUNT() FROM {object_name}")
            return result["totalSize"]
        except SalesforceError:
            return 0

    def explore_history_object(self, object_name: str) -> Dict[str, Any]:
        """
        Comprehensive exploration of a history object.

        Args:
            object_name: Name of the history object

        Returns:
            Dictionary with exploration results
        """
        print(f"\n{'='*60}")
        print(f"Exploring: {object_name}")
        print(f"{'='*60}")

        result = {
            "object_name": object_name,
            "exists": False,
            "record_count": 0,
            "fields": [],
            "sample_records": [],
        }

        # Check if object exists and get field list
        fields = self.get_field_names(object_name)
        if not fields:
            print(f"✗ Object not found or not accessible")
            return result

        result["exists"] = True
        result["fields"] = fields

        print(f"✓ Object exists")
        print(f"✓ Fields available: {len(fields)}")
        print(f"  Key fields: {', '.join(fields[:10])}...")

        # Get record count
        count = self.get_record_count(object_name)
        result["record_count"] = count
        print(f"✓ Total records: {count:,}")

        if count > 0:
            # Get sample records
            sample = self.query_sample(object_name, limit=3)
            result["sample_records"] = sample.get("records", [])
            print(f"✓ Retrieved {len(result['sample_records'])} sample records")

        return result

    def explore_all_history_objects(self) -> Dict[str, Any]:
        """
        Explore all relevant history and audit objects.

        Returns:
            Dictionary with all exploration results
        """
        history_objects = [
            "OpportunityHistory",
            "OpportunityFieldHistory",
            "AccountHistory",
            "CaseHistory",
            "LeadHistory",
            "ContactHistory",
            "Task",
            "Event",
            "ProcessInstance",
            "ProcessInstanceStep",
            "ProcessInstanceWorkitem",
            "SetupAuditTrail",
        ]

        results = {}
        for obj_name in history_objects:
            results[obj_name] = self.explore_history_object(obj_name)

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

        print(f"{'Object':<30} {'Exists':<10} {'Records':<15}")
        print("-" * 60)

        for obj_name, data in results.items():
            exists = "Yes" if data["exists"] else "No"
            count = f"{data['record_count']:,}" if data["exists"] else "N/A"
            print(f"{obj_name:<30} {exists:<10} {count:<15}")

        print("\n" + "=" * 60)
        print("Exploration complete!")
        print("=" * 60)


def main():
    """Main exploration workflow."""
    explorer = SalesforceExplorer()

    # Connect to Salesforce
    if not explorer.connect():
        print("\nFailed to connect to Salesforce. Check your configuration.")
        print("Make sure you have a .env file with valid credentials.")
        sys.exit(1)

    # Explore all history objects
    print("\nStarting exploration of Salesforce history objects...")
    results = explorer.explore_all_history_objects()

    # Print summary
    explorer.print_summary(results)

    # Save results to file
    output_file = "salesforce_exploration_results.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\nDetailed results saved to: {output_file}")


if __name__ == "__main__":
    main()
