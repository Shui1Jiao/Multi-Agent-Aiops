"""Event-driven data models shared by all agents.

Every agent communicates through these events. The `correlation_id` field ties
all events belonging to one incident together.
"""

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class Severity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class AlertStatus(str, Enum):
    FIRING = "firing"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


class AgentType(str, Enum):
    MONITOR = "monitor"
    RCA = "rca"
    HEAL = "heal"
    CHANGE = "change"
    ORCHESTRATOR = "orchestrator"


class EventType(str, Enum):
    ALERT_FIRED = "alert.fired"
    ALERT_RESOLVED = "alert.resolved"
    RCA_STARTED = "rca.started"
    RCA_COMPLETED = "rca.completed"
    HEAL_PROPOSED = "heal.proposed"
    HEAL_EXECUTING = "heal.executing"
    HEAL_COMPLETED = "heal.completed"
    HEAL_FAILED = "heal.failed"
    CHANGE_REQUESTED = "change.requested"
    CHANGE_APPROVED = "change.approved"
    CHANGE_REJECTED = "change.rejected"


class HealLevel(str, Enum):
    L0_AUTO = "L0"
    L1_SEMI = "L1"
    L2_MANUAL = "L2"


class BaseEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source_agent: AgentType
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict[str, Any] = Field(default_factory=dict)


class AlertEvent(BaseEvent):
    event_type: EventType = EventType.ALERT_FIRED
    source_agent: AgentType = AgentType.MONITOR

    alert_name: str
    severity: Severity
    status: AlertStatus = AlertStatus.FIRING
    source: str
    target_service: str
    metric_name: Optional[str] = None
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    description: str = ""
    labels: dict[str, str] = Field(default_factory=dict)


class RCAEvent(BaseEvent):
    event_type: EventType = EventType.RCA_COMPLETED
    source_agent: AgentType = AgentType.RCA

    alert_event_id: str
    root_cause: str
    confidence: float
    affected_services: list[str]
    evidence: list[dict[str, Any]]
    suggested_actions: list[str]


class HealEvent(BaseEvent):
    event_type: EventType = EventType.HEAL_PROPOSED
    source_agent: AgentType = AgentType.HEAL

    rca_event_id: str
    heal_level: HealLevel
    action_type: str
    action_params: dict[str, Any]
    target_service: str
    estimated_impact: str
    blast_radius: float
    dry_run_result: Optional[str] = None
    execution_result: Optional[str] = None


class ChangeEvent(BaseEvent):
    event_type: EventType = EventType.CHANGE_REQUESTED
    source_agent: AgentType = AgentType.CHANGE

    heal_event_id: str
    risk_score: float
    approval_status: str = "pending"
    approver: str = ""
    reason: str = ""
    audit_log: list[dict[str, Any]] = Field(default_factory=list)


class IncidentState(BaseModel):
    """Global state that flows through the orchestrator."""

    incident_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    status: str = "open"

    alert_event: Optional[AlertEvent] = None
    rca_event: Optional[RCAEvent] = None
    heal_event: Optional[HealEvent] = None
    change_event: Optional[ChangeEvent] = None

    current_agent: AgentType = AgentType.MONITOR
    retry_count: int = 0
    error_message: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
