#!/usr/bin/env python3
"""
Slack Metadata Gap Analyzer - Main Application

Analyze Slack channels to identify metadata gaps and extract context
for Atlan data catalog enrichment.

Usage:
    # Analyze a channel
    python src/app.py analyze --channel data-questions

    # Analyze with custom options
    python src/app.py analyze --channel data-questions --days 60 --output ./my-report

    # Test Slack connection
    python src/app.py test-connection

    # Analyze from exported JSON file
    python src/app.py analyze-file --input data/messages.json
"""

import argparse
import json
import os
import sys
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from slack_client import SlackClient, SlackClientError, test_connection
from slack_metadata_analyzer import SlackMetadataAnalyzer, format_markdown_report
from transcript_parser import parse_transcript_file


def print_banner():
    """Print application banner."""
    banner = """
╔═══════════════════════════════════════════════════════════════╗
║       Slack Metadata Gap Analyzer for Atlan                   ║
║       Discover what your data catalog is missing              ║
╚═══════════════════════════════════════════════════════════════╝
"""
    print(banner)


def print_summary(results: dict):
    """Print a summary of the analysis to console."""
    print("\n" + "=" * 60)
    print("ANALYSIS SUMMARY")
    print("=" * 60)

    summary = results.get("summary", {})
    print(f"\n📊 Threads analyzed: {summary.get('total_threads_analyzed', 0)}")
    print(f"📦 Unique assets identified: {summary.get('unique_assets_identified', 0)}")
    print(f"❓ Assets with questions: {summary.get('assets_with_questions', 0)}")

    # Top priority assets
    priority_assets = results.get("priority_assets", [])[:5]
    if priority_assets:
        print("\n🎯 TOP PRIORITY ASSETS FOR CURATION:")
        print("-" * 40)
        for i, asset_data in enumerate(priority_assets, 1):
            score = asset_data.get("priority_score", 0)
            asset_name = asset_data.get("asset", "Unknown")
            q_count = asset_data.get("questions", 0)
            print(f"  {i}. {asset_name}")
            print(f"     Score: {score}/10 | Questions: {q_count}")

    # Question type distribution
    dist = results.get("question_type_distribution", {})
    top_types = sorted(dist.items(), key=lambda x: x[1], reverse=True)[:3]
    if top_types:
        print("\n📈 TOP QUESTION TYPES:")
        print("-" * 40)
        for q_type, pct in top_types:
            if pct > 0:
                bar = "█" * int(pct / 5)
                print(f"  {q_type}: {pct}% {bar}")

    # Metadata gaps
    gaps = results.get("metadata_gaps", [])
    high_severity = [g for g in gaps if g.get("severity") == "High"]
    if high_severity:
        print("\n⚠️  HIGH SEVERITY GAPS:")
        print("-" * 40)
        for gap in high_severity:
            print(f"  • {gap.get('gap_type')}: {gap.get('description')}")

    # Agent recommendations summary
    recs = results.get("agent_recommendations", {})
    print("\n🤖 AGENT OPPORTUNITIES:")
    print("-" * 40)
    print(f"  • Description Agent: {len(recs.get('description_agent', []))} assets ready")
    print(f"  • Ownership Agent: {len(recs.get('ownership_agent', []))} assets with owners identified")
    print(f"  • Quality Context Agent: {len(recs.get('quality_context_agent', []))} assets with caveats")
    print(f"  • Glossary Linkage Agent: {len(recs.get('glossary_linkage_agent', []))} assets with terms")

    print("\n" + "=" * 60)


def analyze_channel(args):
    """Analyze a Slack channel for metadata gaps."""
    print_banner()

    channel = args.channel
    days = args.days
    output_dir = args.output
    limit = args.limit

    print(f"🔍 Analyzing channel: #{channel.lstrip('#')}")
    print(f"📅 Looking back: {days} days")
    print(f"📁 Output directory: {output_dir}")
    print()

    # Fetch messages from Slack
    try:
        client = SlackClient()
        messages_data = client.get_channel_messages(
            channel=channel,
            days_back=days,
            limit=limit
        )
    except SlackClientError as e:
        print(f"\n❌ Error fetching from Slack: {e}")
        print("\nTroubleshooting:")
        print("  1. Make sure SLACK_BOT_TOKEN is set")
        print("  2. Ensure the bot is invited to the channel")
        print("  3. Check that the token has required scopes")
        sys.exit(1)

    if not messages_data.get("messages"):
        print("\n⚠️  No question messages found in the channel.")
        print("This could mean:")
        print("  - The channel has no data-related questions")
        print("  - The time range is too short")
        print("  - The bot doesn't have access to message history")
        sys.exit(0)

    print(f"\n✓ Fetched {len(messages_data['messages'])} question threads")

    # Run analysis
    print("\n🔬 Running analysis...")
    analyzer = SlackMetadataAnalyzer(messages_data)
    results = analyzer.analyze()

    # Save results
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    channel_safe = channel.lstrip("#").replace("-", "_")

    # Save JSON
    json_path = os.path.join(output_dir, f"{channel_safe}_analysis_{timestamp}.json")
    json_filename = os.path.basename(json_path)
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"✓ JSON report: {json_filename}")

    # Save Markdown
    md_report = format_markdown_report(results)
    md_path = os.path.join(output_dir, f"{channel_safe}_analysis_{timestamp}.md")
    md_filename = os.path.basename(md_path)
    with open(md_path, "w") as f:
        f.write(md_report)
    print(f"✓ Markdown report: {md_filename}")

    # Print summary
    print_summary(results)


def analyze_file(args):
    """Analyze from an exported JSON file."""
    print_banner()

    input_file = args.input
    output_dir = args.output

    print(f"📂 Reading from: {input_file}")

    # Load messages
    try:
        with open(input_file, "r") as f:
            messages_data = json.load(f)
    except FileNotFoundError:
        print(f"❌ File not found: {input_file}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON: {e}")
        sys.exit(1)

    print(f"✓ Loaded {len(messages_data.get('messages', []))} messages")

    # Run analysis
    print("\n🔬 Running analysis...")
    analyzer = SlackMetadataAnalyzer(messages_data)
    results = analyzer.analyze()

    # Save results
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save JSON
    json_path = os.path.join(output_dir, f"analysis_{timestamp}.json")
    json_filename = os.path.basename(json_path)
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"✓ JSON report: {json_filename}")

    # Save Markdown
    md_report = format_markdown_report(results)
    md_path = os.path.join(output_dir, f"analysis_{timestamp}.md")
    md_filename = os.path.basename(md_path)
    with open(md_path, "w") as f:
        f.write(md_report)
    print(f"✓ Markdown report: {md_filename}")

    # Print summary
    print_summary(results)


def analyze_transcript(args):
    """Analyze from a call transcript file."""
    print_banner()

    input_file = args.input
    output_dir = args.output
    title = args.title if hasattr(args, 'title') and args.title else None

    print(f"📞 Reading transcript: {os.path.basename(input_file)}")

    # Parse transcript
    try:
        messages_data = parse_transcript_file(input_file, title)
    except FileNotFoundError:
        print(f"❌ File not found: {input_file}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error parsing transcript: {e}")
        sys.exit(1)

    print(f"✓ Parsed {len(messages_data.get('messages', []))} conversation threads")

    # Run analysis
    print("\n🔬 Running analysis...")
    analyzer = SlackMetadataAnalyzer(messages_data)
    results = analyzer.analyze()

    # Save results
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save JSON
    json_path = os.path.join(output_dir, f"transcript_analysis_{timestamp}.json")
    json_filename = os.path.basename(json_path)
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"✓ JSON report: {json_filename}")

    # Save Markdown
    md_report = format_markdown_report(results)
    md_path = os.path.join(output_dir, f"transcript_analysis_{timestamp}.md")
    md_filename = os.path.basename(md_path)
    with open(md_path, "w") as f:
        f.write(md_report)
    print(f"✓ Markdown report: {md_filename}")

    # Print summary
    print_summary(results)


def cmd_test_connection(args):
    """Test Slack API connection."""
    print_banner()
    print("Testing Slack connection...\n")

    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        print("❌ SLACK_BOT_TOKEN environment variable not set")
        print("\nTo set it:")
        print("  export SLACK_BOT_TOKEN='xoxb-your-token-here'")
        sys.exit(1)

    if test_connection():
        print("\n✓ Your Slack token is valid and working!")
        print("\nNext steps:")
        print("  1. Invite the bot to your channel: /invite @your-bot-name")
        print("  2. Run: python src/app.py analyze --channel your-channel-name")
    else:
        print("\n❌ Connection failed. Please check your token.")
        sys.exit(1)


def interactive_mode():
    """Run in interactive mode."""
    print_banner()
    print("Welcome! Let's analyze your Slack channel for metadata gaps.\n")

    # Check for token
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        print("❌ SLACK_BOT_TOKEN not found in environment.")
        print("\nTo get started:")
        print("  1. Create a Slack app at https://api.slack.com/apps")
        print("  2. Add these Bot Token Scopes:")
        print("     - channels:history")
        print("     - channels:read")
        print("     - users:read")
        print("  3. Install the app to your workspace")
        print("  4. Copy the Bot Token and run:")
        print("     export SLACK_BOT_TOKEN='xoxb-your-token'")
        print("  5. Run this script again")
        return

    # Test connection
    print("Testing Slack connection...")
    if not test_connection():
        return

    # Get channel name
    print()
    channel = input("Enter channel name (e.g., data-questions): ").strip()
    if not channel:
        print("No channel provided. Exiting.")
        return

    # Get days back
    days_input = input("How many days back to analyze? [90]: ").strip()
    days = int(days_input) if days_input else 90

    # Get output directory
    output = input("Output directory [./reports]: ").strip() or "./reports"

    print()

    # Create args object
    class Args:
        pass

    args = Args()
    args.channel = channel
    args.days = days
    args.output = output
    args.limit = 500

    analyze_channel(args)


def main():
    parser = argparse.ArgumentParser(
        description="Analyze Slack channels for metadata gaps",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze a Slack channel
  python src/app.py analyze --channel data-questions

  # Analyze with custom options
  python src/app.py analyze --channel data-questions --days 60 --output ./my-report

  # Analyze a call transcript
  python src/app.py analyze-transcript --input transcript.txt

  # Analyze a call transcript with custom title
  python src/app.py analyze-transcript --input call_notes.txt --title "Team Sync Call"

  # Test your Slack connection
  python src/app.py test-connection

  # Analyze from an exported JSON file
  python src/app.py analyze-file --input data/messages.json

  # Run in interactive mode
  python src/app.py

Environment Variables:
  SLACK_BOT_TOKEN    Your Slack Bot Token (required for live analysis)
"""
    )

    subparsers = parser.add_subparsers(dest="command")

    # analyze command
    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Analyze a Slack channel"
    )
    analyze_parser.add_argument(
        "--channel", "-c",
        required=True,
        help="Slack channel name (with or without #)"
    )
    analyze_parser.add_argument(
        "--days", "-d",
        type=int,
        default=90,
        help="Number of days to look back (default: 90)"
    )
    analyze_parser.add_argument(
        "--output", "-o",
        default="./reports",
        help="Output directory for reports (default: ./reports)"
    )
    analyze_parser.add_argument(
        "--limit", "-l",
        type=int,
        default=500,
        help="Maximum messages to analyze (default: 500)"
    )

    # analyze-file command
    file_parser = subparsers.add_parser(
        "analyze-file",
        help="Analyze from an exported JSON file"
    )
    file_parser.add_argument(
        "--input", "-i",
        required=True,
        help="Path to JSON file with messages"
    )
    file_parser.add_argument(
        "--output", "-o",
        default="./reports",
        help="Output directory for reports (default: ./reports)"
    )

    # analyze-transcript command
    transcript_parser = subparsers.add_parser(
        "analyze-transcript",
        help="Analyze from a call transcript file"
    )
    transcript_parser.add_argument(
        "--input", "-i",
        required=True,
        help="Path to transcript file (plain text with timestamps)"
    )
    transcript_parser.add_argument(
        "--output", "-o",
        default="./reports",
        help="Output directory for reports (default: ./reports)"
    )
    transcript_parser.add_argument(
        "--title", "-t",
        help="Title for the transcript (default: filename)"
    )

    # test-connection command
    subparsers.add_parser(
        "test-connection",
        help="Test Slack API connection"
    )

    args = parser.parse_args()

    if args.command == "analyze":
        analyze_channel(args)
    elif args.command == "analyze-file":
        analyze_file(args)
    elif args.command == "analyze-transcript":
        analyze_transcript(args)
    elif args.command == "test-connection":
        cmd_test_connection(args)
    else:
        # No command provided - run interactive mode
        interactive_mode()


if __name__ == "__main__":
    main()
