"""
Slack API Client for fetching channel messages and threads.

Requires a Slack Bot Token with the following scopes:
- channels:history
- channels:read
- groups:history (for private channels)
- groups:read (for private channels)
- users:read
"""

import os
import time
from datetime import datetime, timedelta
from typing import Optional
import requests


class SlackClientError(Exception):
    """Custom exception for Slack API errors."""
    pass


class SlackClient:
    """Client for interacting with Slack API to fetch channel messages."""

    BASE_URL = "https://slack.com/api"

    def __init__(self, token: Optional[str] = None):
        """
        Initialize the Slack client.

        Args:
            token: Slack Bot Token. If not provided, reads from SLACK_BOT_TOKEN env var.
        """
        self.token = token or os.environ.get("SLACK_BOT_TOKEN")
        if not self.token:
            raise SlackClientError(
                "Slack token required. Set SLACK_BOT_TOKEN environment variable "
                "or pass token to constructor."
            )
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self._user_cache = {}

    def _make_request(self, endpoint: str, params: dict = None) -> dict:
        """Make a request to the Slack API with rate limiting."""
        url = f"{self.BASE_URL}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params or {})

        if response.status_code == 429:
            # Rate limited - wait and retry
            retry_after = int(response.headers.get("Retry-After", 5))
            print(f"Rate limited. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
            return self._make_request(endpoint, params)

        data = response.json()
        if not data.get("ok"):
            error = data.get("error", "Unknown error")
            raise SlackClientError(f"Slack API error: {error}")

        return data

    def get_channel_id(self, channel_name: str) -> str:
        """
        Get channel ID from channel name.

        Args:
            channel_name: Channel name (with or without #)

        Returns:
            Channel ID
        """
        # Remove # prefix if present
        channel_name = channel_name.lstrip("#")

        # Try public channels first
        cursor = None
        while True:
            params = {"limit": 200}
            if cursor:
                params["cursor"] = cursor

            data = self._make_request("conversations.list", params)

            for channel in data.get("channels", []):
                if channel["name"] == channel_name:
                    return channel["id"]

            cursor = data.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

        raise SlackClientError(f"Channel '{channel_name}' not found. "
                               "Make sure the bot is invited to the channel.")

    def get_user_info(self, user_id: str) -> dict:
        """Get user information (cached)."""
        if user_id in self._user_cache:
            return self._user_cache[user_id]

        try:
            data = self._make_request("users.info", {"user": user_id})
            user = data.get("user", {})
            user_info = {
                "name": user.get("real_name") or user.get("name", "Unknown"),
                "role": user.get("profile", {}).get("title", ""),
                "username": user.get("name", "")
            }
            self._user_cache[user_id] = user_info
            return user_info
        except SlackClientError:
            return {"name": "Unknown", "role": "", "username": user_id}

    def get_channel_messages(
        self,
        channel: str,
        days_back: int = 90,
        limit: int = 1000
    ) -> dict:
        """
        Fetch messages from a Slack channel.

        Args:
            channel: Channel name or ID
            days_back: Number of days to look back
            limit: Maximum number of messages to fetch

        Returns:
            Dictionary with channel info and messages in analyzer format
        """
        # Get channel ID if name was provided
        if not channel.startswith("C") and not channel.startswith("G"):
            channel_id = self.get_channel_id(channel)
            channel_name = channel.lstrip("#")
        else:
            channel_id = channel
            channel_name = channel_id

        # Calculate time range
        oldest = datetime.now() - timedelta(days=days_back)
        oldest_ts = str(oldest.timestamp())

        print(f"Fetching messages from #{channel_name} (last {days_back} days)...")

        # Fetch messages
        messages = []
        cursor = None
        fetched = 0

        while fetched < limit:
            params = {
                "channel": channel_id,
                "oldest": oldest_ts,
                "limit": min(200, limit - fetched)
            }
            if cursor:
                params["cursor"] = cursor

            data = self._make_request("conversations.history", params)

            for msg in data.get("messages", []):
                # Skip non-user messages (bots, system messages)
                if msg.get("subtype") or not msg.get("user"):
                    continue

                # Only include messages that look like questions
                text = msg.get("text", "")
                if not self._is_likely_question(text):
                    continue

                messages.append(msg)
                fetched += 1

                if fetched >= limit:
                    break

            cursor = data.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

            # Small delay to avoid rate limiting
            time.sleep(0.5)

        print(f"Found {len(messages)} potential question messages")

        # Fetch thread replies for messages with replies
        print("Fetching thread replies...")
        formatted_messages = []

        for i, msg in enumerate(messages):
            if i % 10 == 0:
                print(f"  Processing message {i + 1}/{len(messages)}...")

            thread_ts = msg.get("thread_ts") or msg.get("ts")
            reply_count = msg.get("reply_count", 0)

            # Get user info
            user_info = self.get_user_info(msg.get("user", ""))

            formatted_msg = {
                "thread_id": f"T{thread_ts.replace('.', '')}",
                "timestamp": self._ts_to_iso(msg.get("ts")),
                "user": user_info["username"],
                "user_role": user_info["role"],
                "message": msg.get("text", ""),
                "replies": []
            }

            # Fetch replies if there are any
            if reply_count > 0:
                formatted_msg["replies"] = self._get_thread_replies(
                    channel_id, thread_ts, msg.get("ts")
                )

            formatted_messages.append(formatted_msg)
            time.sleep(0.3)  # Rate limit protection

        # Calculate date range
        if formatted_messages:
            timestamps = [m["timestamp"] for m in formatted_messages]
            start_date = min(timestamps)[:10]
            end_date = max(timestamps)[:10]
            date_range = f"{start_date} to {end_date}"
        else:
            date_range = f"Last {days_back} days"

        return {
            "channel_name": f"#{channel_name}",
            "date_range": date_range,
            "messages": formatted_messages
        }

    def _get_thread_replies(
        self,
        channel_id: str,
        thread_ts: str,
        parent_ts: str
    ) -> list:
        """Fetch replies to a thread."""
        try:
            data = self._make_request("conversations.replies", {
                "channel": channel_id,
                "ts": thread_ts,
                "limit": 100
            })

            replies = []
            for msg in data.get("messages", []):
                # Skip the parent message
                if msg.get("ts") == parent_ts:
                    continue

                # Skip bot messages
                if msg.get("subtype") or not msg.get("user"):
                    continue

                user_info = self.get_user_info(msg.get("user", ""))
                replies.append({
                    "timestamp": self._ts_to_iso(msg.get("ts")),
                    "user": user_info["username"],
                    "user_role": user_info["role"],
                    "message": msg.get("text", "")
                })

            return replies

        except SlackClientError:
            return []

    def _is_likely_question(self, text: str) -> bool:
        """Check if a message is likely a question about data."""
        text_lower = text.lower()

        # Must contain a question indicator
        question_indicators = [
            "?",
            "what ", "what's", "whats",
            "where ", "where's",
            "who ", "who's",
            "how ", "how's",
            "can someone", "can anyone",
            "does anyone", "does someone",
            "is there", "is this",
            "which ", "why ",
            "help me", "i need",
            "looking for", "trying to find",
            "not sure", "confused"
        ]

        has_question = any(ind in text_lower for ind in question_indicators)
        if not has_question:
            return False

        # Should mention data-related terms
        data_terms = [
            "table", "column", "field", "data", "database",
            "dashboard", "report", "metric", "kpi",
            "query", "sql", "schema",
            "source", "upstream", "downstream",
            "definition", "mean", "value",
            "access", "permission",
            "owner", "owns", "team",
            "reliable", "trust", "quality",
            "etl", "pipeline", "dbt",
            "warehouse", "snowflake", "bigquery", "redshift"
        ]

        has_data_term = any(term in text_lower for term in data_terms)

        return has_data_term

    def _ts_to_iso(self, ts: str) -> str:
        """Convert Slack timestamp to ISO format."""
        try:
            dt = datetime.fromtimestamp(float(ts))
            return dt.isoformat() + "Z"
        except (ValueError, TypeError):
            return datetime.now().isoformat() + "Z"


def test_connection(token: Optional[str] = None) -> bool:
    """Test if the Slack token is valid."""
    try:
        client = SlackClient(token)
        client._make_request("auth.test")
        print("✓ Slack connection successful!")
        return True
    except SlackClientError as e:
        print(f"✗ Slack connection failed: {e}")
        return False
