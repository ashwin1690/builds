#!/usr/bin/env python3
"""
Transcript Parser for Call Transcripts

Parses plain text call transcripts with timestamps and converts them
to a format compatible with the Slack metadata analyzer.

Expected transcript format:
    [HH:MM:SS] Speaker Name: Message text
    [HH:MM:SS] Speaker Name: Another message

Example:
    [00:01:23] John Smith: What does the revenue_daily_v2 table contain?
    [00:01:45] Jane Doe: It has daily revenue aggregated by product.
"""

import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional


def parse_transcript(transcript_text: str, title: str = "Call Transcript") -> dict:
    """
    Parse a plain text transcript into Slack-compatible message format.

    Args:
        transcript_text: Plain text transcript with timestamps
        title: Title for the transcript (used as channel name)

    Returns:
        Dictionary in Slack messages format with questions and replies
    """
    lines = transcript_text.strip().split('\n')

    # Pattern to match: [HH:MM:SS] Speaker: Message or [HH:MM] Speaker: Message
    pattern = r'\[(\d{1,2}:\d{2}(?::\d{2})?)\]\s*([^:]+):\s*(.+)'

    messages = []
    current_thread = None
    base_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    for line in lines:
        line = line.strip()
        if not line:
            continue

        match = re.match(pattern, line)
        if not match:
            # If current thread exists, append as continuation
            if current_thread and messages:
                messages[-1]['message'] += ' ' + line
            continue

        timestamp_str, speaker, message = match.groups()

        # Parse timestamp
        time_parts = timestamp_str.split(':')
        if len(time_parts) == 3:
            hours, minutes, seconds = map(int, time_parts)
        else:
            hours, minutes = map(int, time_parts)
            seconds = 0

        message_time = base_time + timedelta(hours=hours, minutes=minutes, seconds=seconds)
        timestamp = message_time.isoformat()

        # Check if this is a question
        is_question = _is_question(message)

        if is_question:
            # Start a new thread
            thread_id = f"thread_{len([m for m in messages if 'replies' in m]) + 1}"
            current_thread = thread_id

            messages.append({
                'thread_id': thread_id,
                'timestamp': timestamp,
                'user': speaker.strip(),
                'message': message.strip(),
                'replies': []
            })
        elif current_thread and messages:
            # Add as reply to current thread
            messages[-1]['replies'].append({
                'user': speaker.strip(),
                'message': message.strip(),
                'timestamp': timestamp
            })
        else:
            # Standalone message (not a question, no thread)
            # Skip or create thread anyway
            thread_id = f"thread_{len([m for m in messages if 'replies' in m]) + 1}"
            current_thread = thread_id

            messages.append({
                'thread_id': thread_id,
                'timestamp': timestamp,
                'user': speaker.strip(),
                'message': message.strip(),
                'replies': []
            })

    # Calculate date range
    if messages:
        first_time = datetime.fromisoformat(messages[0]['timestamp'])
        last_time = datetime.fromisoformat(messages[-1]['timestamp'])
        date_range = f"{first_time.strftime('%Y-%m-%d')} to {last_time.strftime('%Y-%m-%d')}"
    else:
        date_range = datetime.now().strftime('%Y-%m-%d')

    return {
        'channel_name': title,
        'date_range': date_range,
        'messages': messages
    }


def _is_question(text: str) -> bool:
    """
    Determine if a message is likely a question.

    Args:
        text: Message text

    Returns:
        True if message appears to be a question
    """
    text_lower = text.lower().strip()

    # Check for question mark
    if '?' in text:
        return True

    # Check for question words
    question_words = [
        'what', 'where', 'when', 'why', 'how', 'who', 'which',
        'can', 'could', 'would', 'should', 'is', 'are', 'does',
        'do', 'did', 'has', 'have', 'will'
    ]

    # Question usually starts with these words
    first_word = text_lower.split()[0] if text_lower.split() else ''
    if first_word in question_words:
        return True

    # Check for data-related question patterns
    data_question_patterns = [
        'tell me about',
        'explain',
        'clarify',
        'confused about',
        'not sure',
        'wondering',
        'need to know',
        'can someone',
        'does anyone know'
    ]

    for pattern in data_question_patterns:
        if pattern in text_lower:
            return True

    return False


def parse_transcript_file(file_path: str, title: Optional[str] = None) -> dict:
    """
    Parse a transcript from a file.

    Args:
        file_path: Path to transcript file
        title: Optional title (defaults to filename)

    Returns:
        Dictionary in Slack messages format
    """
    import os

    if title is None:
        title = os.path.basename(file_path).replace('.txt', '').replace('_', ' ').title()

    with open(file_path, 'r', encoding='utf-8') as f:
        transcript_text = f.read()

    return parse_transcript(transcript_text, title)


def main():
    """Test the transcript parser with a sample."""
    sample_transcript = """
[00:01:23] John Smith: What does the revenue_daily_v2 table contain?
[00:01:45] Jane Doe: It has daily revenue aggregated by product.
[00:02:10] John Smith: And what about dim_customer?
[00:02:30] Jane Doe: That's the customer dimension table with all customer attributes.
[00:03:15] Bob Wilson: Is fct_orders reliable for reporting?
[00:03:40] Jane Doe: Yes, but be careful with the status column - values aren't well documented.
[00:04:20] Bob Wilson: Who owns the analytics.events table?
[00:04:45] Alice Chen: The data platform team owns that one.
"""

    result = parse_transcript(sample_transcript, "Team Data Call")

    import json
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
