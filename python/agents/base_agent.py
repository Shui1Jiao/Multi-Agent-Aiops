"""Abstract agent base using the template method pattern."""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional

from core.event_bus import EventBus
from models.events import AgentType, IncidentState

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    def __init__(
        self,
        agent_type: AgentType,
        event_bus: EventBus,
        name: Optional[str] = None,
    ):
        self.agent_type = agent_type
        self.name = name or agent_type.value
        self.event_bus = event_bus
        self._metrics = {
            "processed_count": 0,
            "error_count": 0,
            "avg_latency_ms": 0.0,
        }

    async def handle(self, state: IncidentState) -> IncidentState:
        """Template method shared by every agent."""
        start_time = datetime.utcnow()
        logger.info("[%s] Processing incident %s", self.name, state.incident_id)

        try:
            state.current_agent = self.agent_type
            state.updated_at = datetime.utcnow()
            state = await self.process(state)

            self._metrics["processed_count"] += 1
            elapsed = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_avg_latency(elapsed)
            logger.info("[%s] Completed in %.1fms", self.name, elapsed)

        except Exception as exc:
            self._metrics["error_count"] += 1
            state.error_message = f"[{self.name}] Error: {str(exc)}"
            logger.error("[%s] Failed: %s", self.name, exc, exc_info=True)

        return state

    @abstractmethod
    async def process(self, state: IncidentState) -> IncidentState:
        """Subclasses implement the core business logic."""

    def _update_avg_latency(self, new_latency: float) -> None:
        count = self._metrics["processed_count"]
        old_avg = self._metrics["avg_latency_ms"]
        self._metrics["avg_latency_ms"] = old_avg + (new_latency - old_avg) / count

    def get_metrics(self) -> dict[str, Any]:
        return {
            "agent": self.name,
            "type": self.agent_type.value,
            **self._metrics,
        }
