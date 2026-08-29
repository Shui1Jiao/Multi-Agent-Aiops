"""Workflow tests for the replicated AIOps system."""

import asyncio

import pytest

from core.event_bus import InMemoryEventBus
from core.knowledge_graph import create_demo_knowledge_graph
from core.orchestrator import Orchestrator
from models.events import AgentType
from models.time_series import EnsembleDetector, generate_demo_metrics


@pytest.mark.asyncio
async def test_demo_workflow_resolves_incident():
    event_bus = InMemoryEventBus()
    await event_bus.start()
    orchestrator = Orchestrator(event_bus)

    state = await orchestrator.run()

    assert state.alert_event is not None
    assert state.alert_event.target_service == "order-service"
    assert state.rca_event is not None
    assert state.rca_event.confidence >= 0.3
    assert state.heal_event is not None
    assert state.change_event is not None
    assert state.change_event.approval_status == "approved"
    assert state.status == "resolved"

    node_names = [n["name"] for n in orchestrator.get_workflow_status()]
    assert node_names == ["monitor", "rca", "heal", "change"]

    await event_bus.stop()


@pytest.mark.asyncio
async def test_custom_metric_trigger_keeps_metadata():
    event_bus = InMemoryEventBus()
    await event_bus.start()
    orchestrator = Orchestrator(event_bus)

    metadata = {"metric_data": {"metric_name": "cpu", "value": 95.0, "service": "order-service"}}
    state = await orchestrator.run(metadata=metadata)

    assert state.metadata["metric_data"]["value"] == 95.0

    await event_bus.stop()


def test_knowledge_graph_traces_dependencies():
    kg = create_demo_knowledge_graph()
    deps = kg.get_dependencies("order-service")
    assert "payment-service" in deps
    assert len(kg.bfs_trace("order-service")) > 0
    assert kg.get_topology_summary()["total_nodes"] > 10


def test_ensemble_detector_finds_injected_anomalies():
    values, labels = generate_demo_metrics(200, inject_anomaly=True)
    detector = EnsembleDetector(min_votes=2)

    found = []
    for i in range(50, len(values)):
        is_anomaly, _, _ = detector.detect(values[:i], values[i])
        if is_anomaly:
            found.append(i)

    assert any(labels[i] == 1.0 for i in found)
