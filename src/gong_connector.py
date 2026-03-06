"""
Gong Call Intelligence Connector

Integrates with Gong's API to extract call recordings, transcripts, and deal
context. Correlates advisory signals with Gong call data to understand which
data assets are discussed in customer/prospect calls and what sentiment surrounds them.

Gong API capabilities used:
- /v2/calls: List calls with filters (date range, workspace)
- /v2/calls/transcript: Get call transcripts with speaker identification
- /v2/calls/extensive: Get detailed call data (trackers, topics, deal info)
- /v2/deals: Get associated deal information

Environment Variables:
    GONG_API_KEY: Gong API access key
    GONG_API_SECRET: Gong API secret (used together for Basic Auth)
    GONG_BASE_URL: Gong API base URL (default: https://api.gong.io)
"""

import base64
import json
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from enriched_signals import EnrichedAdvisorySignal, GongCallContext
from connector_base import SignalEnricher

logger = logging.getLogger(__name__)

# Default Gong API base URL
DEFAULT_GONG_BASE_URL = "https://api.gong.io"


class GongClient:
    """
    Client for the Gong API.

    Handles authentication, pagination, and rate limiting for Gong REST API calls.
    Uses Basic Auth with access_key:access_key_secret encoded in Base64.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        self.api_key = api_key or os.environ.get("GONG_API_KEY", "")
        self.api_secret = api_secret or os.environ.get("GONG_API_SECRET", "")
        self.base_url = (base_url or os.environ.get("GONG_BASE_URL", DEFAULT_GONG_BASE_URL)).rstrip("/")
        self._session = None

    @property
    def is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def _get_auth_header(self) -> str:
        credentials = f"{self.api_key}:{self.api_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"

    def _get_session(self):
        if self._session is None:
            try:
                import requests
                self._session = requests.Session()
                self._session.headers.update({
                    "Authorization": self._get_auth_header(),
                    "Content-Type": "application/json",
                })
            except ImportError:
                raise ImportError("requests library required for Gong integration")
        return self._session

    def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make an authenticated request to the Gong API."""
        session = self._get_session()
        url = f"{self.base_url}{endpoint}"
        response = session.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()

    def _paginate(self, method: str, endpoint: str, body: dict, results_key: str) -> List[dict]:
        """Handle Gong API cursor-based pagination."""
        all_results = []
        cursor = None

        while True:
            if cursor:
                body["cursor"] = cursor

            data = self._request(method, endpoint, json=body)
            records = data.get(results_key, [])
            all_results.extend(records)

            # Gong uses records.currentPageNumber / records.totalRecords for paging
            page_info = data.get("records", {})
            cursor = page_info.get("cursor")
            if not cursor:
                break

        return all_results

    def list_calls(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        workspace_id: Optional[str] = None,
    ) -> List[dict]:
        """
        List calls within a date range.

        Returns list of call metadata objects with id, title, started, duration, etc.
        """
        body: Dict[str, Any] = {}

        if from_date:
            body["filter"] = body.get("filter", {})
            body["filter"]["fromDateTime"] = from_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        if to_date:
            body["filter"] = body.get("filter", {})
            body["filter"]["toDateTime"] = to_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        if workspace_id:
            body["filter"] = body.get("filter", {})
            body["filter"]["workspaceId"] = workspace_id

        return self._paginate("POST", "/v2/calls", body, "calls")

    def get_call_transcripts(self, call_ids: List[str]) -> Dict[str, List[dict]]:
        """
        Get transcripts for a list of calls.

        Returns dict mapping call_id -> list of transcript sentences.
        Each sentence has: speakerId, topic, sentences[{start, end, text}]
        """
        body = {"filter": {"callIds": call_ids}}
        data = self._request("POST", "/v2/calls/transcript", json=body)

        result = {}
        for call_transcript in data.get("callTranscripts", []):
            call_id = call_transcript.get("callId")
            result[call_id] = call_transcript.get("transcript", [])

        return result

    def get_calls_extensive(self, call_ids: List[str]) -> List[dict]:
        """
        Get extensive call data including trackers, topics, and deal associations.

        Returns list of detailed call objects with:
        - metaData: title, started, duration, primaryUserId
        - parties: list of call participants
        - content: trackers, topics, pointsOfInterest
        - interaction: talkRatio, interactivity, patience
        - collaboration: deals, contacts
        """
        body = {
            "filter": {"callIds": call_ids},
            "contentSelector": {
                "exposedFields": {
                    "content": {
                        "trackers": True,
                        "topics": True,
                        "pointsOfInterest": True,
                    },
                    "interaction": {
                        "interactionStats": True,
                    },
                    "collaboration": {
                        "publicComments": True,
                    },
                    "parties": True,
                },
            },
        }
        data = self._request("POST", "/v2/calls/extensive", json=body)
        return data.get("calls", [])

    def search_calls_by_keyword(
        self,
        keywords: List[str],
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
    ) -> List[dict]:
        """
        Search for calls that mention specific keywords/asset names.

        Uses Gong's keyword tracker functionality to find calls where
        specific data assets or topics were discussed.
        """
        # Get all calls in date range first, then filter by transcript content
        calls = self.list_calls(from_date=from_date, to_date=to_date)

        if not calls:
            return []

        # Get transcripts in batches of 20
        matching_calls = []
        call_ids = [c["id"] for c in calls if "id" in c]

        for i in range(0, len(call_ids), 20):
            batch_ids = call_ids[i:i + 20]
            transcripts = self.get_call_transcripts(batch_ids)

            for call_id, transcript_parts in transcripts.items():
                full_text = " ".join(
                    sentence.get("text", "")
                    for part in transcript_parts
                    for sentence in part.get("sentences", [])
                ).lower()

                for keyword in keywords:
                    if keyword.lower() in full_text:
                        # Find the matching call metadata
                        call_meta = next((c for c in calls if c.get("id") == call_id), None)
                        if call_meta:
                            call_meta["_matched_keywords"] = [
                                kw for kw in keywords if kw.lower() in full_text
                            ]
                            call_meta["_transcript"] = transcript_parts
                            matching_calls.append(call_meta)
                        break

        return matching_calls


class GongCallEnricher(SignalEnricher):
    """
    Enriches advisory signals with Gong call recording intelligence.

    For each asset referenced in advisory signals, searches Gong for calls
    where that asset was discussed. Extracts:
    - How often the asset is discussed in customer calls
    - Sentiment around those discussions
    - Associated deals and their values
    - Key phrases and context surrounding mentions
    - Talk ratios indicating customer confusion vs. rep explanation
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: Optional[str] = None,
        lookback_days: int = 90,
    ):
        self.client = GongClient(api_key=api_key, api_secret=api_secret, base_url=base_url)
        self.lookback_days = lookback_days

    def is_available(self) -> bool:
        return self.client.is_configured

    def enrich(self, signals: List[EnrichedAdvisorySignal]) -> List[EnrichedAdvisorySignal]:
        """Enrich signals with Gong call data."""
        # Collect unique asset names across all signals
        all_assets = set()
        for signal in signals:
            all_assets.update(signal.base_signal.assets_referenced)

        if not all_assets:
            return signals

        # Search Gong for calls mentioning these assets
        from_date = datetime.utcnow() - timedelta(days=self.lookback_days)
        matching_calls = self.client.search_calls_by_keyword(
            keywords=list(all_assets),
            from_date=from_date,
        )

        if not matching_calls:
            return signals

        # Get extensive data for matching calls
        call_ids = [c["id"] for c in matching_calls if "id" in c]
        extensive_data = {}
        for i in range(0, len(call_ids), 20):
            batch = call_ids[i:i + 20]
            for call_detail in self.client.get_calls_extensive(batch):
                cid = call_detail.get("metaData", {}).get("id")
                if cid:
                    extensive_data[cid] = call_detail

        # Build asset -> call context index
        asset_call_index = self._build_asset_call_index(
            all_assets, matching_calls, extensive_data
        )

        # Apply Gong context to each signal
        for signal in signals:
            gong_ctx = self._aggregate_gong_context(
                signal.base_signal.assets_referenced, asset_call_index
            )
            if gong_ctx.call_count > 0:
                signal.gong_context = gong_ctx
                if "gong" not in signal.sources:
                    signal.sources.append("gong")

        return signals

    def _build_asset_call_index(
        self,
        assets: set,
        matching_calls: List[dict],
        extensive_data: Dict[str, dict],
    ) -> Dict[str, List[dict]]:
        """Build index mapping asset names to their Gong call contexts."""
        index: Dict[str, List[dict]] = {asset: [] for asset in assets}

        for call in matching_calls:
            call_id = call.get("id")
            matched_keywords = call.get("_matched_keywords", [])
            transcript_parts = call.get("_transcript", [])
            extensive = extensive_data.get(call_id, {})

            # Extract deal info from extensive data
            deals = []
            collaboration = extensive.get("collaboration", {})
            if collaboration:
                for deal_ref in collaboration.get("deals", []):
                    deals.append({
                        "name": deal_ref.get("title", ""),
                        "stage": deal_ref.get("stage", ""),
                        "value": deal_ref.get("amount", 0),
                    })

            # Extract talk ratio from interaction stats
            interaction = extensive.get("interaction", {})
            talk_ratio = 0.0
            if interaction:
                stats = interaction.get("interactionStats", [])
                for stat in stats:
                    if stat.get("name") == "talkRatio":
                        talk_ratio = stat.get("value", 0.0)

            # Extract call duration
            meta = extensive.get("metaData", call)
            duration = meta.get("duration", 0)  # seconds

            # Extract speaker info
            parties = extensive.get("parties", [])
            speakers = [
                p.get("name", "Unknown")
                for p in parties
                if p.get("spoke", False)
            ]

            # For each matched asset, extract surrounding phrases
            for asset_name in matched_keywords:
                context_phrases = self._extract_context_phrases(
                    transcript_parts, asset_name
                )
                sentiment = self._estimate_sentiment(context_phrases)

                call_context = {
                    "call_id": call_id,
                    "duration_minutes": duration / 60.0 if duration else 0,
                    "talk_ratio": talk_ratio,
                    "deals": deals,
                    "context_phrases": context_phrases,
                    "sentiment": sentiment,
                    "speakers": speakers,
                }

                if asset_name.lower() in index:
                    index[asset_name.lower()].append(call_context)

        return index

    def _extract_context_phrases(
        self, transcript_parts: List[dict], keyword: str
    ) -> List[str]:
        """Extract phrases surrounding a keyword mention in transcript."""
        phrases = []
        keyword_lower = keyword.lower()

        for part in transcript_parts:
            sentences = part.get("sentences", [])
            for i, sentence in enumerate(sentences):
                text = sentence.get("text", "")
                if keyword_lower in text.lower():
                    # Get surrounding context (1 sentence before and after)
                    context_parts = []
                    if i > 0:
                        context_parts.append(sentences[i - 1].get("text", ""))
                    context_parts.append(text)
                    if i < len(sentences) - 1:
                        context_parts.append(sentences[i + 1].get("text", ""))
                    phrases.append(" ".join(context_parts))

        return phrases[:10]  # Cap at 10 phrases

    def _estimate_sentiment(self, phrases: List[str]) -> float:
        """
        Estimate sentiment of phrases mentioning an asset.

        Returns float from -1.0 (very negative) to 1.0 (very positive).
        Uses keyword-based heuristic (no external NLP dependency).
        """
        if not phrases:
            return 0.0

        positive_words = {
            "great", "good", "excellent", "helpful", "reliable", "accurate",
            "trust", "love", "perfect", "clear", "easy", "works well",
            "solid", "impressed", "valuable", "useful",
        }
        negative_words = {
            "broken", "wrong", "confusing", "confused", "issue", "problem",
            "missing", "gaps", "weird", "unreliable", "slow", "frustrating",
            "unclear", "difficult", "bad", "concern", "worried", "inaccurate",
            "stale", "outdated", "deprecated",
        }
        question_words = {
            "what", "how", "why", "where", "when", "which", "can you explain",
            "what does", "confused about", "not sure",
        }

        total_score = 0.0
        for phrase in phrases:
            phrase_lower = phrase.lower()
            pos = sum(1 for w in positive_words if w in phrase_lower)
            neg = sum(1 for w in negative_words if w in phrase_lower)
            quest = sum(1 for w in question_words if w in phrase_lower)

            # Questions indicate confusion (mildly negative for advisory purposes)
            phrase_score = (pos - neg - quest * 0.5) / max(pos + neg + quest, 1)
            total_score += phrase_score

        return round(total_score / len(phrases), 3)

    def _aggregate_gong_context(
        self, asset_names: List[str], index: Dict[str, List[dict]]
    ) -> GongCallContext:
        """Aggregate Gong call contexts across multiple assets for a single signal."""
        ctx = GongCallContext()
        seen_call_ids = set()

        for asset_name in asset_names:
            call_contexts = index.get(asset_name.lower(), [])
            for cc in call_contexts:
                call_id = cc["call_id"]
                if call_id not in seen_call_ids:
                    seen_call_ids.add(call_id)
                    ctx.call_ids.append(call_id)
                    ctx.total_duration_minutes += cc["duration_minutes"]
                    ctx.avg_talk_ratio = (
                        (ctx.avg_talk_ratio * (ctx.call_count) + cc["talk_ratio"])
                        / (ctx.call_count + 1)
                        if ctx.call_count > 0
                        else cc["talk_ratio"]
                    )
                    ctx.call_count += 1

                    for deal in cc["deals"]:
                        if deal["name"] and deal["name"] not in ctx.deal_names:
                            ctx.deal_names.append(deal["name"])
                            ctx.deal_stages.append(deal.get("stage", ""))
                            ctx.deal_values.append(deal.get("value", 0))

                    ctx.speakers.extend(cc["speakers"])

                ctx.mention_count += len(cc["context_phrases"])
                ctx.key_phrases.extend(cc["context_phrases"])
                ctx.sentiment_scores.append(cc["sentiment"])

        # Dedupe speakers
        ctx.speakers = list(set(ctx.speakers))
        # Cap key phrases
        ctx.key_phrases = ctx.key_phrases[:20]

        return ctx


def parse_gong_transcript_to_messages(
    call_data: dict,
    transcript: List[dict],
) -> dict:
    """
    Convert a Gong call transcript into the standard messages_data format
    used by AdvisorySignalClassifier.

    This allows Gong transcripts to be analyzed as a signal source alongside
    Slack messages and plain text transcripts.
    """
    meta = call_data.get("metaData", call_data)
    title = meta.get("title", "Gong Call")
    started = meta.get("started", datetime.now().isoformat())

    # Map speaker IDs to names
    parties = call_data.get("parties", [])
    speaker_map = {}
    for party in parties:
        speaker_id = party.get("speakerId")
        name = party.get("name", f"Speaker_{speaker_id}")
        speaker_map[speaker_id] = name

    messages = []
    current_thread = None

    for part in transcript:
        speaker_id = part.get("speakerId")
        speaker_name = speaker_map.get(speaker_id, f"Speaker_{speaker_id}")
        sentences = part.get("sentences", [])

        for sentence in sentences:
            text = sentence.get("text", "").strip()
            if not text:
                continue

            timestamp = sentence.get("start", 0)  # milliseconds
            ts_str = f"{started}+{timestamp}ms"

            # Check if this is a question
            is_question = (
                "?" in text
                or text.lower().startswith(("what", "how", "why", "where", "who", "which", "can", "does", "is"))
            )

            if is_question:
                thread_id = f"gong_thread_{len(messages) + 1}"
                current_thread = thread_id
                messages.append({
                    "thread_id": thread_id,
                    "timestamp": ts_str,
                    "user": speaker_name,
                    "user_role": "Customer" if not _is_internal_speaker(speaker_name, parties) else "Rep",
                    "message": text,
                    "replies": [],
                })
            elif current_thread and messages:
                messages[-1]["replies"].append({
                    "user": speaker_name,
                    "user_role": "Rep" if _is_internal_speaker(speaker_name, parties) else "Customer",
                    "message": text,
                    "timestamp": ts_str,
                })

    return {
        "channel_name": f"Gong: {title}",
        "date_range": started[:10] if isinstance(started, str) else str(started),
        "messages": messages,
    }


def _is_internal_speaker(name: str, parties: List[dict]) -> bool:
    """Check if a speaker is an internal participant (rep) vs external (customer)."""
    for party in parties:
        if party.get("name") == name:
            return party.get("affiliation", "").lower() == "internal"
    return False
