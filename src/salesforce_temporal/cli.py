#!/usr/bin/env python3
"""
Command-line interface for Salesforce Temporal Data Extractor.

Usage:
    sf-temporal explore-salesforce
    sf-temporal explore-atlan
    sf-temporal extract opportunity-history [--incremental]
    sf-temporal extract field-history <object> [--field <name>]
    sf-temporal extract approval-history
    sf-temporal extract activity
    sf-temporal extract setup-audit-trail
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List

from salesforce_temporal.config.settings import get_settings
from salesforce_temporal.extractors.activity import ActivityExtractor
from salesforce_temporal.extractors.approval_history import ApprovalHistoryExtractor
from salesforce_temporal.extractors.field_history import FieldHistoryExtractor
from salesforce_temporal.extractors.opportunity_history import OpportunityHistoryExtractor
from salesforce_temporal.extractors.setup_audit_trail import SetupAuditTrailExtractor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def explore_salesforce_command(args):
    """Run Salesforce exploration."""
    from salesforce_temporal.exploration.explore_salesforce import main as explore_main

    explore_main()


def explore_atlan_command(args):
    """Run Atlan exploration."""
    from salesforce_temporal.exploration.explore_atlan import main as explore_main

    explore_main()


def extract_opportunity_history(args):
    """Extract opportunity history."""
    logger.info("Extracting OpportunityHistory...")

    extractor = OpportunityHistoryExtractor()
    events = []

    try:
        for event in extractor.extract_events(incremental=args.incremental):
            events.append(event.to_dict())

            if args.limit and len(events) >= args.limit:
                break

        # Save to output file
        output_file = args.output or f"opportunity_history_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl"
        save_events(events, output_file, args.format)

        logger.info(f"Extracted {len(events)} events to {output_file}")

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)


def extract_field_history(args):
    """Extract field history for a specific object."""
    logger.info(f"Extracting {args.object}...")

    # Map common names to actual history object names
    object_map = {
        "opportunity": ("OpportunityFieldHistory", "Opportunity"),
        "account": ("AccountHistory", "Account"),
        "case": ("CaseHistory", "Case"),
        "lead": ("LeadHistory", "Lead"),
        "contact": ("ContactHistory", "Contact"),
    }

    if args.object.lower() in object_map:
        history_obj, parent_obj = object_map[args.object.lower()]
    else:
        # Assume custom object format
        history_obj = args.object
        parent_obj = args.object.replace("History", "").replace("FieldHistory", "")

    extractor = FieldHistoryExtractor(history_obj, parent_obj)
    events = []

    try:
        if args.field:
            # Extract specific field only
            event_iter = extractor.extract_by_field_name(args.field, incremental=args.incremental)
        else:
            # Extract all fields
            event_iter = extractor.extract_events(incremental=args.incremental)

        for event in event_iter:
            events.append(event.to_dict())

            if args.limit and len(events) >= args.limit:
                break

        # Save to output file
        output_file = args.output or f"{history_obj}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl"
        save_events(events, output_file, args.format)

        logger.info(f"Extracted {len(events)} events to {output_file}")

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)


def extract_approval_history(args):
    """Extract approval history."""
    logger.info("Extracting ApprovalHistory...")

    extractor = ApprovalHistoryExtractor()
    events = []

    try:
        for event in extractor.extract_events(incremental=args.incremental):
            events.append(event.to_dict())

            if args.limit and len(events) >= args.limit:
                break

        # Save to output file
        output_file = args.output or f"approval_history_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl"
        save_events(events, output_file, args.format)

        logger.info(f"Extracted {len(events)} events to {output_file}")

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)


def extract_activity(args):
    """Extract activity (Task and Event) records."""
    logger.info("Extracting Activity...")

    extractor = ActivityExtractor()
    events = []

    try:
        for event in extractor.extract_events(
            incremental=args.incremental,
            extract_tasks=not args.events_only,
            extract_events=not args.tasks_only,
        ):
            events.append(event.to_dict())

            if args.limit and len(events) >= args.limit:
                break

        # Save to output file
        output_file = args.output or f"activity_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl"
        save_events(events, output_file, args.format)

        logger.info(f"Extracted {len(events)} events to {output_file}")

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)


def extract_setup_audit_trail(args):
    """Extract setup audit trail."""
    logger.info("Extracting SetupAuditTrail...")

    extractor = SetupAuditTrailExtractor()
    events = []

    try:
        for event in extractor.extract_events(
            incremental=args.incremental,
            lookback_days=args.lookback_days,
        ):
            events.append(event.to_dict())

            if args.limit and len(events) >= args.limit:
                break

        # Save to output file
        output_file = args.output or f"setup_audit_trail_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl"
        save_events(events, output_file, args.format)

        logger.info(f"Extracted {len(events)} events to {output_file}")

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)


def save_events(events: List[dict], output_file: str, format: str = "jsonl"):
    """
    Save events to file in specified format.

    Args:
        events: List of event dictionaries
        output_file: Output file path
        format: Output format (jsonl, json, csv)
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if format == "jsonl":
        with open(output_path, "w") as f:
            for event in events:
                f.write(json.dumps(event, default=str) + "\n")

    elif format == "json":
        with open(output_path, "w") as f:
            json.dump(events, f, indent=2, default=str)

    elif format == "csv":
        import pandas as pd

        df = pd.DataFrame(events)
        df.to_csv(output_path, index=False)

    else:
        raise ValueError(f"Unsupported format: {format}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Salesforce Temporal Data Extractor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Explore commands
    explore_sf = subparsers.add_parser("explore-salesforce", help="Explore Salesforce API")
    explore_sf.set_defaults(func=explore_salesforce_command)

    explore_atlan = subparsers.add_parser("explore-atlan", help="Explore Atlan SDK")
    explore_atlan.set_defaults(func=explore_atlan_command)

    # Extract subcommand
    extract = subparsers.add_parser("extract", help="Extract temporal data")
    extract_subparsers = extract.add_subparsers(dest="extractor", help="Extractor to use")

    # Common extraction arguments
    def add_common_args(parser):
        parser.add_argument("--incremental", action="store_true", help="Incremental extraction")
        parser.add_argument("--output", "-o", help="Output file path")
        parser.add_argument("--format", choices=["jsonl", "json", "csv"], default="jsonl", help="Output format")
        parser.add_argument("--limit", type=int, help="Maximum number of events to extract")

    # OpportunityHistory
    opp_parser = extract_subparsers.add_parser("opportunity-history", help="Extract opportunity history")
    add_common_args(opp_parser)
    opp_parser.set_defaults(func=extract_opportunity_history)

    # FieldHistory
    field_parser = extract_subparsers.add_parser("field-history", help="Extract field history")
    field_parser.add_argument("object", help="Object name (e.g., opportunity, account, case)")
    field_parser.add_argument("--field", help="Extract specific field only")
    add_common_args(field_parser)
    field_parser.set_defaults(func=extract_field_history)

    # ApprovalHistory
    approval_parser = extract_subparsers.add_parser("approval-history", help="Extract approval history")
    add_common_args(approval_parser)
    approval_parser.set_defaults(func=extract_approval_history)

    # Activity
    activity_parser = extract_subparsers.add_parser("activity", help="Extract activity (Task/Event)")
    activity_parser.add_argument("--tasks-only", action="store_true", help="Extract only Tasks")
    activity_parser.add_argument("--events-only", action="store_true", help="Extract only Events")
    add_common_args(activity_parser)
    activity_parser.set_defaults(func=extract_activity)

    # SetupAuditTrail
    setup_parser = extract_subparsers.add_parser("setup-audit-trail", help="Extract setup audit trail")
    setup_parser.add_argument("--lookback-days", type=int, help="Days to look back (max 180)")
    add_common_args(setup_parser)
    setup_parser.set_defaults(func=extract_setup_audit_trail)

    # Parse and execute
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Run the command
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
