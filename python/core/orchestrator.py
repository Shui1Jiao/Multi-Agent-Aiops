"""Orchestrator: a small state machine that runs agents in order.

The workflow is Monitor -> RCA -> Heal -> Change. Each node has a condition
and can be retried; completed states are saved as checkpoints.
"""

import asyncio
import logging
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional

from agents.base_agent import BaseAgent
from agents.change_agent import ChangeAgent
from agents.heal_agent import HealAgent
from agents.monitor_agent import MonitorAgent
from agents.rca_agent import RCAAgent
from core.event_bus import EventBus
from models.events import AgentType, IncidentState

logger = logging.getLogger(__name__)


class NodeStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class WorkflowNode:
    def __init__(
        self,
        name: str,
        agent: BaseAgent,
        condition: Optional[Callable[[IncidentState], bool]] = None,
        max_retries: int = 3,
    ):
        self.name = name
        self.agent = agent
        self.condition = condition
        self.max_retries = max_retries
        self.status = NodeStatus.PENDING
        self.retry_count = 0

    def should_execute(self, state: IncidentState) -> bool:
        if self.condition is None:
            return True
        return self.condition(state)


class Orchestrator:
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self._nodes: list[WorkflowNode] = []
        self._checkpoints: dict[str, IncidentState] = {}
        self._build_default_workflow()

    def _build_default_workflow(self) -> None:
        monitor = MonitorAgent(self.event_bus)
        rca = RCAAgent(self.event_bus)
        heal = HealAgent(self.event_bus, dry_run=True)
        change = ChangeAgent(self.event_bus)

        self._nodes = [
            WorkflowNode(name="monitor", agent=monitor, condition=None),
            WorkflowNode(
                name="rca",
                agent=rca,
                condition=lambda s: s.alert_event is not None,
            ),
            WorkflowNode(
                name="heal",
                agent=heal,
                condition=lambda s: (
                    s.rca_event is not None and s.rca_event.confidence >= 0.3
                ),
            ),
            WorkflowNode(
                name="change",
                agent=change,
                condition=lambda s: s.heal_event is not None,
            ),
        ]

    async def run(
        self,
        initial_state: Optional[IncidentState] = None,
        metadata: Optional[dict] = None,
    ) -> IncidentState:
        state = initial_state or IncidentState()
        if metadata:
            state.metadata = metadata

        logger.info("[Orchestrator] Starting workflow for incident %s", state.incident_id)

        for node in self._nodes:
            if not node.should_execute(state):
                node.status = NodeStatus.SKIPPED
                logger.info("[Orchestrator] Skipping node: %s", node.name)
                continue

            node.status = NodeStatus.RUNNING
            success = False

            while node.retry_count <= node.max_retries:
                try:
                    state = await node.agent.handle(state)

                    if state.error_message and node.retry_count < node.max_retries:
                        node.retry_count += 1
                        logger.warning(
                            "[Orchestrator] Retrying %s (%d/%d)",
                            node.name,
                            node.retry_count,
                            node.max_retries,
                        )
                        state.error_message = None
                        continue

                    node.status = NodeStatus.COMPLETED
                    success = True
                    break
                except Exception as exc:
                    node.retry_count += 1
                    logger.error(
                        "[Orchestrator] Node %s failed: %s (retry %d/%d)",
                        node.name,
                        exc,
                        node.retry_count,
                        node.max_retries,
                    )
                    if node.retry_count > node.max_retries:
                        break

            if not success:
                node.status = NodeStatus.FAILED
                logger.error(
                    "[Orchestrator] Node %s failed after %d retries",
                    node.name,
                    node.max_retries,
                )

            self._save_checkpoint(state)

        state.updated_at = datetime.utcnow()
        logger.info(
            "[Orchestrator] Workflow completed: incident=%s status=%s",
            state.incident_id,
            state.status,
        )
        return state

    def _save_checkpoint(self, state: IncidentState) -> None:
        self._checkpoints[state.incident_id] = state.model_copy(deep=True)

    def get_checkpoint(self, incident_id: str) -> Optional[IncidentState]:
        return self._checkpoints.get(incident_id)

    def get_workflow_status(self) -> list[dict[str, Any]]:
        return [
            {
                "name": node.name,
                "status": node.status.value,
                "agent_type": node.agent.agent_type.value,
                "retry_count": node.retry_count,
                "metrics": node.agent.get_metrics(),
            }
            for node in self._nodes
        ]


async def run_demo():
    from core.event_bus import InMemoryEventBus

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    event_bus = InMemoryEventBus()
    await event_bus.start()

    orchestrator = Orchestrator(event_bus)
    state = await orchestrator.run()

    print("\n" + "=" * 60)
    print("  故障处理结果")
    print("=" * 60)
    print(f"  事件 ID:    {state.incident_id}")
    print(f"  状态:       {state.status}")

    if state.alert_event:
        print("\n  [告警]")
        print(f"    名称:     {state.alert_event.alert_name}")
        print(f"    严重度:   {state.alert_event.severity.value}")
        print(f"    服务:     {state.alert_event.target_service}")
        print(f"    指标值:   {state.alert_event.metric_value}")

    if state.rca_event:
        print("\n  [根因分析]")
        print(f"    根因:     {state.rca_event.root_cause}")
        print(f"    置信度:   {state.rca_event.confidence}")
        print(f"    影响链:   {' -> '.join(state.rca_event.affected_services[:5])}")
        print(f"    建议动作: {', '.join(state.rca_event.suggested_actions)}")

    if state.heal_event:
        print("\n  [自愈]")
        print(f"    操作:     {state.heal_event.action_type}")
        print(f"    级别:     {state.heal_event.heal_level.value}")
        print(f"    爆炸半径: {state.heal_event.blast_radius:.2f}")
        print(f"    Dry-run:  {state.heal_event.dry_run_result}")

    if state.change_event:
        print("\n  [审批]")
        print(f"    状态:     {state.change_event.approval_status}")
        print(f"    风险分:   {state.change_event.risk_score}")
        print(f"    审批人:   {state.change_event.approver}")
        print(f"    原因:     {state.change_event.reason}")

    print("\n  [工作流节点状态]")
    for node_info in orchestrator.get_workflow_status():
        print(f"    {node_info['name']:12s} -> {node_info['status']}")

    print("=" * 60)

    await event_bus.stop()
    return state


if __name__ == "__main__":
    asyncio.run(run_demo())
