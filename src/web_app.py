"""
Slack Metadata Gap Analyzer - Web Interface

A Streamlit-based web interface for analyzing Slack channels.

Usage:
    streamlit run src/web_app.py
"""

import json
import os
import sys
from datetime import datetime

import streamlit as st

# Add src to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from slack_client import SlackClient, SlackClientError
from slack_metadata_analyzer import SlackMetadataAnalyzer, format_markdown_report
from transcript_parser import parse_transcript


# Page configuration
st.set_page_config(
    page_title="Slack Metadata Gap Analyzer",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        margin-bottom: 0;
    }
    .sub-header {
        color: #666;
        font-size: 1.1rem;
        margin-top: 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    .priority-high {
        color: #d63031;
        font-weight: bold;
    }
    .priority-medium {
        color: #fdcb6e;
        font-weight: bold;
    }
    .priority-low {
        color: #00b894;
    }
</style>
""", unsafe_allow_html=True)


def main():
    # Header
    st.markdown('<p class="main-header">🔍 Slack Metadata Gap Analyzer</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Discover what your data catalog is missing by analyzing Slack conversations</p>', unsafe_allow_html=True)
    st.markdown("---")

    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")

        # Token input
        token = st.text_input(
            "Slack Bot Token",
            type="password",
            help="Your Slack Bot Token (xoxb-...)",
            value=os.environ.get("SLACK_BOT_TOKEN", "")
        )

        st.markdown("---")

        # Analysis options
        st.subheader("Analysis Options")

        channel_name = st.text_input(
            "Channel Name",
            placeholder="data-questions",
            help="Enter the channel name without #"
        )

        days_back = st.slider(
            "Days to analyze",
            min_value=7,
            max_value=365,
            value=90,
            help="How far back to look for messages"
        )

        max_messages = st.slider(
            "Max messages",
            min_value=50,
            max_value=1000,
            value=500,
            step=50,
            help="Maximum number of messages to analyze"
        )

        st.markdown("---")

        # Alternative: File upload
        st.subheader("Or Upload File")
        uploaded_file = st.file_uploader(
            "Upload Slack export or transcript",
            type=["json", "txt"],
            help="Upload a JSON file (Slack export) or TXT file (call transcript)"
        )

        st.markdown("---")

        # Action button
        analyze_button = st.button(
            "🚀 Run Analysis",
            type="primary",
            use_container_width=True
        )

        st.markdown("---")
        st.markdown("""
        **Required Slack Bot Scopes:**
        - `channels:history`
        - `channels:read`
        - `users:read`

        [Create Slack App →](https://api.slack.com/apps)
        """)

    # Main content area
    if analyze_button:
        if uploaded_file:
            run_file_analysis(uploaded_file)
        elif channel_name and token:
            run_channel_analysis(token, channel_name, days_back, max_messages)
        else:
            st.error("Please provide either a Slack token and channel name, or upload a JSON file.")

    # Show instructions if no analysis has been run
    if "results" not in st.session_state:
        show_instructions()
    else:
        display_results(st.session_state.results)


def show_instructions():
    """Display getting started instructions."""
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
        ### 🚀 Getting Started

        **Option 1: Live Slack Analysis**
        1. Enter your Slack Bot Token in the sidebar
        2. Enter the channel name you want to analyze
        3. Click "Run Analysis"

        **Option 2: Upload Slack JSON**
        1. Export your Slack messages to JSON
        2. Upload the file using the sidebar
        3. Click "Run Analysis"

        **Option 3: Upload Call Transcript**
        1. Prepare transcript in format: `[HH:MM:SS] Speaker: Message`
        2. Upload the .txt file using the sidebar
        3. Click "Run Analysis"
        """)

    with col2:
        st.markdown("""
        ### 📊 What You'll Get

        - **Priority Assets**: Data assets that need documentation most
        - **Extracted Context**: Descriptions, ownership, and caveats mined from answers
        - **Question Patterns**: What types of questions are being asked
        - **Agent Recommendations**: Actions for Atlan enrichment agents
        """)

    st.markdown("---")

    # Sample output preview
    st.markdown("### 📋 Sample Analysis Preview")

    sample_assets = [
        {"name": "revenue_daily_v2", "score": 10.0, "questions": 5, "type": "Definitional, Usage"},
        {"name": "dim_customer", "score": 9.5, "questions": 4, "type": "Ownership, Lineage"},
        {"name": "analytics.events", "score": 8.0, "questions": 3, "type": "Definitional"},
    ]

    for asset in sample_assets:
        with st.container():
            cols = st.columns([3, 1, 1, 2])
            cols[0].write(f"**{asset['name']}**")
            cols[1].write(f"Score: {asset['score']}")
            cols[2].write(f"Q's: {asset['questions']}")
            cols[3].write(f"Types: {asset['type']}")


def run_channel_analysis(token: str, channel: str, days: int, limit: int):
    """Run analysis on a Slack channel."""
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # Step 1: Connect to Slack
        status_text.text("🔗 Connecting to Slack...")
        progress_bar.progress(10)
        client = SlackClient(token)

        # Step 2: Fetch messages
        status_text.text(f"📥 Fetching messages from #{channel}...")
        progress_bar.progress(30)
        messages_data = client.get_channel_messages(
            channel=channel,
            days_back=days,
            limit=limit
        )

        if not messages_data.get("messages"):
            st.warning("No question messages found in this channel.")
            progress_bar.empty()
            status_text.empty()
            return

        progress_bar.progress(60)
        status_text.text(f"🔬 Analyzing {len(messages_data['messages'])} messages...")

        # Step 3: Run analysis
        analyzer = SlackMetadataAnalyzer(messages_data)
        results = analyzer.analyze()

        progress_bar.progress(90)
        status_text.text("📊 Generating reports...")

        # Store results in session state
        st.session_state.results = results
        st.session_state.messages_data = messages_data

        progress_bar.progress(100)
        status_text.text("✅ Analysis complete!")

        # Refresh to show results
        st.rerun()

    except SlackClientError as e:
        st.error(f"Slack API Error: {e}")
        progress_bar.empty()
        status_text.empty()
    except Exception as e:
        st.error(f"Error: {e}")
        progress_bar.empty()
        status_text.empty()


def run_file_analysis(uploaded_file):
    """Run analysis on an uploaded JSON or TXT file."""
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # Determine file type
        file_name = uploaded_file.name
        is_transcript = file_name.endswith('.txt')

        # Step 1: Parse file
        status_text.text("📂 Reading file...")
        progress_bar.progress(20)

        if is_transcript:
            # Parse transcript file
            transcript_text = uploaded_file.read().decode('utf-8')
            title = file_name.replace('.txt', '').replace('_', ' ').title()
            messages_data = parse_transcript(transcript_text, title)
        else:
            # Parse JSON file
            messages_data = json.load(uploaded_file)

        if not messages_data.get("messages"):
            st.warning("No messages found in the uploaded file.")
            progress_bar.empty()
            status_text.empty()
            return

        progress_bar.progress(50)
        status_text.text(f"🔬 Analyzing {len(messages_data['messages'])} messages...")

        # Step 2: Run analysis
        analyzer = SlackMetadataAnalyzer(messages_data)
        results = analyzer.analyze()

        progress_bar.progress(90)
        status_text.text("📊 Generating reports...")

        # Store results
        st.session_state.results = results
        st.session_state.messages_data = messages_data

        progress_bar.progress(100)
        status_text.text("✅ Analysis complete!")

        st.rerun()

    except json.JSONDecodeError as e:
        st.error(f"Invalid JSON file: {e}")
        progress_bar.empty()
        status_text.empty()
    except Exception as e:
        st.error(f"Error: {e}")
        progress_bar.empty()
        status_text.empty()


def display_results(results: dict):
    """Display analysis results."""
    # Summary metrics
    st.markdown("## 📊 Analysis Summary")

    summary = results.get("summary", {})
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="Threads Analyzed",
            value=summary.get("total_threads_analyzed", 0)
        )

    with col2:
        st.metric(
            label="Assets Identified",
            value=summary.get("unique_assets_identified", 0)
        )

    with col3:
        st.metric(
            label="Assets with Questions",
            value=summary.get("assets_with_questions", 0)
        )

    st.markdown("---")

    # Tabs for different views
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🎯 Priority Assets",
        "📈 Question Patterns",
        "⚠️ Metadata Gaps",
        "🤖 Agent Recommendations",
        "📄 Full Report"
    ])

    with tab1:
        display_priority_assets(results.get("priority_assets", []))

    with tab2:
        display_question_patterns(results.get("question_type_distribution", {}))

    with tab3:
        display_metadata_gaps(results.get("metadata_gaps", []))

    with tab4:
        display_agent_recommendations(results.get("agent_recommendations", {}))

    with tab5:
        display_full_report(results)


def display_priority_assets(assets: list):
    """Display priority assets section."""
    st.markdown("### Priority Assets for Curation")
    st.markdown("Assets ranked by demand signals from Slack questions.")

    if not assets:
        st.info("No priority assets identified.")
        return

    for asset in assets[:10]:
        with st.expander(f"**{asset['name']}** (Score: {asset['priority_score']}/10)", expanded=False):
            col1, col2 = st.columns([1, 1])

            with col1:
                st.markdown("**Demand Signals:**")
                signals = asset.get("demand_signals", {})
                st.write(f"- Questions: {signals.get('num_questions', 0)}")
                st.write(f"- Unique questioners: {signals.get('unique_questioners', 0)}")
                st.write(f"- Complexity: {signals.get('question_complexity', 'Unknown')}")
                st.write(f"- Types: {', '.join(signals.get('common_question_types', []))}")

            with col2:
                ctx = asset.get("extracted_context", {})
                if any([ctx.get("description"), ctx.get("ownership"), ctx.get("gotchas")]):
                    st.markdown("**Extracted Context:**")
                    if ctx.get("description"):
                        st.write(f"📝 {ctx['description'][:200]}...")
                    if ctx.get("ownership"):
                        st.write(f"👤 Owners: {', '.join(ctx['ownership'])}")
                    if ctx.get("gotchas"):
                        st.write(f"⚠️ {len(ctx['gotchas'])} caveats noted")

            st.markdown("**Sample Questions:**")
            for q in asset.get("sample_questions", [])[:3]:
                st.markdown(f"> {q[:150]}{'...' if len(q) > 150 else ''}")


def display_question_patterns(distribution: dict):
    """Display question type distribution."""
    st.markdown("### Question Type Distribution")
    st.markdown("What types of questions are being asked in Slack?")

    if not distribution:
        st.info("No question patterns identified.")
        return

    import pandas as pd

    # Create DataFrame for chart
    data = [(k, v) for k, v in distribution.items() if v > 0]
    df = pd.DataFrame(data, columns=["Question Type", "Percentage"])
    df = df.sort_values("Percentage", ascending=True)

    st.bar_chart(df.set_index("Question Type"))

    # Insights
    top_type = max(distribution.items(), key=lambda x: x[1])
    st.info(f"💡 **Insight**: {top_type[0]} questions are most common ({top_type[1]}%). "
            "This suggests users frequently need this type of metadata.")


def display_metadata_gaps(gaps: list):
    """Display identified metadata gaps."""
    st.markdown("### Identified Metadata Gaps")
    st.markdown("Systemic issues causing repeated questions.")

    if not gaps:
        st.success("No significant metadata gaps identified!")
        return

    for gap in gaps:
        severity = gap.get("severity", "Unknown")
        icon = "🔴" if severity == "High" else "🟡" if severity == "Medium" else "🟢"

        with st.container():
            st.markdown(f"#### {icon} {gap.get('gap_type', 'Unknown Gap')}")
            st.write(gap.get("description", ""))
            st.write(f"**Affected assets:** {', '.join(gap.get('affected_assets', []))}")
            st.markdown("---")


def display_agent_recommendations(recommendations: dict):
    """Display agent recommendations."""
    st.markdown("### Atlan Agent Recommendations")
    st.markdown("Actions for automated metadata enrichment.")

    # Description Agent
    desc_recs = recommendations.get("description_agent", [])
    if desc_recs:
        st.markdown("#### 📝 Description Agent")
        st.write(f"**{len(desc_recs)} assets** have descriptions extracted from Slack that can be applied.")
        with st.expander("View details"):
            for item in desc_recs[:5]:
                st.write(f"- **{item['asset']}**: \"{item['suggested_description'][:100]}...\"")

    # Ownership Agent
    owner_recs = recommendations.get("ownership_agent", [])
    if owner_recs:
        st.markdown("#### 👤 Ownership Agent")
        st.write(f"**{len(owner_recs)} assets** have identified owners that can be linked.")
        with st.expander("View details"):
            for item in owner_recs[:5]:
                owners = ", ".join(item.get("identified_owners", []))
                st.write(f"- **{item['asset']}**: {owners}")

    # Quality Context Agent
    quality_recs = recommendations.get("quality_context_agent", [])
    if quality_recs:
        st.markdown("#### ⚠️ Quality Context Agent")
        st.write(f"**{len(quality_recs)} assets** have quality notes that should be documented.")
        with st.expander("View details"):
            for item in quality_recs[:5]:
                st.write(f"- **{item['asset']}**: {len(item.get('quality_notes', []))} notes")

    # Glossary Linkage Agent
    glossary_recs = recommendations.get("glossary_linkage_agent", [])
    if glossary_recs:
        st.markdown("#### 📚 Glossary Linkage Agent")
        st.write(f"**{len(glossary_recs)} assets** have business terms that should be linked.")
        with st.expander("View details"):
            for item in sorted(glossary_recs, key=lambda x: x.get("term_count", 0), reverse=True)[:5]:
                terms = ", ".join(item.get("terms_to_link", [])[:5])
                st.write(f"- **{item['asset']}**: {terms}")


def display_full_report(results: dict):
    """Display full report with download options."""
    st.markdown("### Full Report")

    col1, col2 = st.columns(2)

    with col1:
        # JSON download
        json_str = json.dumps(results, indent=2)
        st.download_button(
            label="📥 Download JSON Report",
            data=json_str,
            file_name=f"slack_analysis_{datetime.now().strftime('%Y%m%d')}.json",
            mime="application/json"
        )

    with col2:
        # Markdown download
        md_report = format_markdown_report(results)
        st.download_button(
            label="📥 Download Markdown Report",
            data=md_report,
            file_name=f"slack_analysis_{datetime.now().strftime('%Y%m%d')}.md",
            mime="text/markdown"
        )

    # Show markdown preview
    st.markdown("---")
    st.markdown("#### Report Preview")
    st.markdown(format_markdown_report(results))


if __name__ == "__main__":
    main()
