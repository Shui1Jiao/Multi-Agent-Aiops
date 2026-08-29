"""Event bus abstraction with in-memory and Kafka implementations.

The in-memory bus lets the demo and tests run without external services.
The Kafka implementation is the production-style path with retries and DLQ.
"""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class EventBus(ABC):
    @abstractmethod
    async def publish(self, topic: str, event: BaseModel) -> None:
        """Publish an event to a topic."""

    @abstractmethod
    async def subscribe(self, topic: str, group_id: str, handler: Callable) -> None:
        """Subscribe a consumer group to a topic."""

    @abstractmethod
    async def start(self) -> None:
        """Start the bus."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the bus."""


class InMemoryEventBus(EventBus):
    def __init__(self):
        self._subscribers: dict[str, list[tuple[str, Callable]]] = defaultdict(list)
        self._running = False
        self._event_log: list[dict[str, Any]] = []

    async def publish(self, topic: str, event: BaseModel) -> None:
        event_data = event.model_dump(mode="json")
        self._event_log.append(
            {
                "topic": topic,
                "event": event_data,
                "timestamp": datetime.utcnow().isoformat(),
            }
        )
        logger.info(
            "[EventBus] Published to %s: %s",
            topic,
            event_data.get("event_type", "unknown"),
        )

        for group_id, handler in self._subscribers.get(topic, []):
            try:
                await handler(event_data)
            except Exception as exc:
                logger.error("[EventBus] Handler error in group %s: %s", group_id, exc)

    async def subscribe(self, topic: str, group_id: str, handler: Callable) -> None:
        self._subscribers[topic].append((group_id, handler))
        logger.info("[EventBus] Subscribed %s to %s", group_id, topic)

    async def start(self) -> None:
        self._running = True
        logger.info("[EventBus] InMemory event bus started")

    async def stop(self) -> None:
        self._running = False
        logger.info("[EventBus] InMemory event bus stopped")

    def get_event_log(self) -> list[dict[str, Any]]:
        return self._event_log.copy()


class KafkaEventBus(EventBus):
    def __init__(self, bootstrap_servers: str = "localhost:9092", max_retries: int = 3):
        self._bootstrap_servers = bootstrap_servers
        self._max_retries = max_retries
        self._producer = None
        self._consumers: list = []
        self._running = False

    async def publish(self, topic: str, event: BaseModel) -> None:
        from confluent_kafka import Producer

        if self._producer is None:
            self._producer = Producer(
                {
                    "bootstrap.servers": self._bootstrap_servers,
                    "acks": "all",
                    "retries": self._max_retries,
                    "retry.backoff.ms": 100,
                }
            )

        event_data = event.model_dump_json()
        self._producer.produce(
            topic=topic,
            value=event_data.encode("utf-8"),
            callback=self._delivery_callback,
        )
        self._producer.flush(timeout=5)
        logger.info("[KafkaEventBus] Published to %s", topic)

    async def subscribe(self, topic: str, group_id: str, handler: Callable) -> None:
        from confluent_kafka import Consumer

        consumer = Consumer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([topic])

        async def consume_loop():
            while self._running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                if msg.error():
                    logger.error("[KafkaEventBus] Consumer error: %s", msg.error())
                    continue

                try:
                    event_data = json.loads(msg.value().decode("utf-8"))
                    await handler(event_data)
                    consumer.commit(asynchronous=False)
                except Exception as exc:
                    logger.error("[KafkaEventBus] Handler error: %s", exc)
                    await self._send_to_dlq(topic, msg.value(), str(exc))

        self._consumers.append((consume_loop, consumer))

    async def start(self) -> None:
        self._running = True
        for consume_loop, _ in self._consumers:
            asyncio.create_task(consume_loop())
        logger.info("[KafkaEventBus] Kafka event bus started")

    async def stop(self) -> None:
        self._running = False
        for _, consumer in self._consumers:
            consumer.close()
        if self._producer:
            self._producer.flush(timeout=10)
        logger.info("[KafkaEventBus] Kafka event bus stopped")

    @staticmethod
    def _delivery_callback(err, msg):
        if err:
            logger.error("[KafkaEventBus] Delivery failed: %s", err)
        else:
            logger.debug(
                "[KafkaEventBus] Delivered to %s [%s]",
                msg.topic(),
                msg.partition(),
            )

    async def _send_to_dlq(self, original_topic: str, value: bytes, error: str) -> None:
        dlq_topic = f"{original_topic}.dlq"
        if self._producer:
            self._producer.produce(topic=dlq_topic, value=value)
            self._producer.flush(timeout=5)
            logger.warning("[KafkaEventBus] Sent to DLQ %s: %s", dlq_topic, error)


def create_event_bus(use_kafka: bool = False, **kwargs) -> EventBus:
    if use_kafka:
        return KafkaEventBus(**kwargs)
    return InMemoryEventBus()
