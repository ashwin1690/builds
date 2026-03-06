"""
Abstract base class for signal enrichers.

Each enricher wraps an external connector (Atlan, Salesforce, Gong, Tableau)
and enriches advisory signals with context from that system. Enrichers are
designed for graceful degradation - if a connector is unavailable, the signal
passes through unchanged.
"""

import logging
from abc import ABC, abstractmethod
from typing import List

from enriched_signals import EnrichedAdvisorySignal

logger = logging.getLogger(__name__)


class SignalEnricher(ABC):
    """Abstract base for all advisory signal enrichers."""

    @abstractmethod
    def is_available(self) -> bool:
        """Check if this enricher's backing service is configured and reachable."""
        pass

    @abstractmethod
    def enrich(self, signals: List[EnrichedAdvisorySignal]) -> List[EnrichedAdvisorySignal]:
        """Enrich signals with context from the backing service."""
        pass

    def enrich_safe(self, signals: List[EnrichedAdvisorySignal]) -> List[EnrichedAdvisorySignal]:
        """Wrapper that catches exceptions for graceful degradation."""
        if not self.is_available():
            logger.info(f"{self.__class__.__name__} not available, skipping enrichment.")
            return signals
        try:
            logger.info(f"Running {self.__class__.__name__} on {len(signals)} signals...")
            result = self.enrich(signals)
            logger.info(f"{self.__class__.__name__} enrichment complete.")
            return result
        except Exception as e:
            logger.warning(f"{self.__class__.__name__} failed: {e}", exc_info=True)
            return signals
