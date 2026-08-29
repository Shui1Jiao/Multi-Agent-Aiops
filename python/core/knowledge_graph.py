"""Knowledge graph with in-memory and Neo4j implementations.

The in-memory graph stores service dependencies and supports BFS traversal for
root-cause analysis. The Neo4j implementation shows the production query path.
"""

import logging
from collections import defaultdict, deque
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)


class ServiceNode:
    def __init__(
        self,
        name: str,
        service_type: str = "microservice",
        namespace: str = "production",
        labels: Optional[dict] = None,
    ):
        self.name = name
        self.service_type = service_type
        self.namespace = namespace
        self.labels = labels or {}


class Relationship:
    def __init__(
        self,
        source: str,
        target: str,
        rel_type: str,
        properties: Optional[dict] = None,
    ):
        self.source = source
        self.target = target
        self.rel_type = rel_type
        self.properties = properties or {}


class InMemoryKnowledgeGraph:
    def __init__(self):
        self._nodes: dict[str, dict[str, Any]] = {}
        self._edges: list[Relationship] = []
        self._adjacency: dict[str, list[tuple[str, str, dict]]] = defaultdict(list)
        self._reverse_adjacency: dict[str, list[tuple[str, str, dict]]] = defaultdict(list)

    def add_node(
        self, name: str, node_type: str, properties: Optional[dict] = None
    ) -> None:
        self._nodes[name] = {
            "name": name,
            "type": node_type,
            "properties": properties or {},
            "created_at": datetime.utcnow().isoformat(),
        }

    def add_relationship(
        self,
        source: str,
        target: str,
        rel_type: str,
        properties: Optional[dict] = None,
    ) -> None:
        rel = Relationship(source, target, rel_type, properties)
        self._edges.append(rel)
        self._adjacency[source].append((target, rel_type, properties or {}))
        self._reverse_adjacency[target].append((source, rel_type, properties or {}))

    def get_node(self, name: str) -> Optional[dict[str, Any]]:
        return self._nodes.get(name)

    def get_dependencies(self, service: str) -> list[str]:
        return [
            target
            for target, rel_type, _ in self._adjacency.get(service, [])
            if rel_type == "DEPENDS_ON"
        ]

    def get_dependents(self, service: str) -> list[str]:
        return [
            source
            for source, rel_type, _ in self._reverse_adjacency.get(service, [])
            if rel_type == "DEPENDS_ON"
        ]

    def bfs_trace(
        self, start: str, rel_type: str = "DEPENDS_ON", max_depth: int = 5
    ) -> list[list[str]]:
        paths: list[list[str]] = []
        queue: deque[tuple[str, list[str], int]] = deque()
        queue.append((start, [start], 0))
        visited = {start}

        while queue:
            current, path, depth = queue.popleft()
            if depth >= max_depth:
                continue

            neighbors = [
                target
                for target, rt, _ in self._adjacency.get(current, [])
                if rt == rel_type
            ]

            if not neighbors:
                paths.append(path)
                continue

            for neighbor in neighbors:
                if neighbor not in visited:
                    visited.add(neighbor)
                    new_path = path + [neighbor]
                    queue.append((neighbor, new_path, depth + 1))
                    paths.append(new_path)

        return paths

    def reverse_bfs_trace(
        self, start: str, rel_type: str = "DEPENDS_ON", max_depth: int = 5
    ) -> list[str]:
        """Walk dependencies backwards to find candidate root causes."""
        result: list[str] = []
        queue: deque[tuple[str, int]] = deque()
        queue.append((start, 0))
        visited = {start}

        while queue:
            current, depth = queue.popleft()
            if depth >= max_depth:
                continue

            deps = self.get_dependencies(current)
            if not deps:
                result.append(current)
                continue

            for dep in deps:
                if dep not in visited:
                    visited.add(dep)
                    result.append(dep)
                    queue.append((dep, depth + 1))

        return result

    def find_recent_changes(self, service: str, within_hours: int = 24) -> list[dict]:
        changes = []
        affected = [service] + self.get_dependencies(service)
        for svc in affected:
            node = self._nodes.get(svc, {})
            for change in node.get("properties", {}).get("recent_changes", []):
                changes.append({"service": svc, "change": change})
        return changes

    def compute_impact_score(self, service: str) -> float:
        dependents = self.get_dependents(service)
        total_services = max(len(self._nodes), 1)
        return len(dependents) / total_services

    def get_topology_summary(self) -> dict[str, Any]:
        node_types: dict[str, int] = defaultdict(int)
        for node in self._nodes.values():
            node_types[node["type"]] += 1
        return {
            "total_nodes": len(self._nodes),
            "total_edges": len(self._edges),
            "node_types": dict(node_types),
            "nodes": list(self._nodes.keys()),
        }


class Neo4jKnowledgeGraph:
    def __init__(self, uri: str, user: str, password: str):
        from neo4j import GraphDatabase

        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        self._driver.close()

    def init_schema(self) -> None:
        with self._driver.session() as session:
            session.run(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Service) REQUIRE s.name IS UNIQUE"
            )
            session.run(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Pod) REQUIRE p.name IS UNIQUE"
            )
            session.run(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Node) REQUIRE n.name IS UNIQUE"
            )
            session.run("CREATE INDEX IF NOT EXISTS FOR (a:Alert) ON (a.timestamp)")

    def add_service(self, name: str, properties: Optional[dict] = None) -> None:
        props = properties or {}
        with self._driver.session() as session:
            session.run(
                "MERGE (s:Service {name: $name}) SET s += $props",
                name=name,
                props=props,
            )

    def add_dependency(self, source: str, target: str) -> None:
        with self._driver.session() as session:
            session.run(
                """
                MATCH (s:Service {name: $source})
                MATCH (t:Service {name: $target})
                MERGE (s)-[:DEPENDS_ON]->(t)
                """,
                source=source,
                target=target,
            )

    def find_root_causes(self, service: str, max_depth: int = 5) -> list[dict]:
        with self._driver.session() as session:
            result = session.run(
                """
                MATCH path = (s:Service {name: $service})-[:DEPENDS_ON*1..$max_depth]->(root)
                WHERE NOT (root)-[:DEPENDS_ON]->()
                RETURN root.name AS root_cause,
                       length(path) AS distance,
                       [n IN nodes(path) | n.name] AS path
                ORDER BY distance ASC
                """,
                service=service,
                max_depth=max_depth,
            )
            return [dict(record) for record in result]

    def find_recent_changes(self, service: str, hours: int = 24) -> list[dict]:
        with self._driver.session() as session:
            result = session.run(
                """
                MATCH (s:Service {name: $service})-[:DEPENDS_ON*0..3]->(dep)
                MATCH (c:Change)-[:AFFECTS]->(dep)
                WHERE c.timestamp > datetime() - duration({hours: $hours})
                RETURN c.description AS change, dep.name AS service, c.timestamp AS time
                ORDER BY c.timestamp DESC
                """,
                service=service,
                hours=hours,
            )
            return [dict(record) for record in result]


def create_demo_knowledge_graph() -> InMemoryKnowledgeGraph:
    kg = InMemoryKnowledgeGraph()

    services = {
        "api-gateway": {"type": "gateway", "tier": "frontend"},
        "order-service": {
            "type": "microservice",
            "tier": "backend",
            "recent_changes": ["deploy v2.3.1"],
        },
        "payment-service": {"type": "microservice", "tier": "backend"},
        "inventory-service": {
            "type": "microservice",
            "tier": "backend",
            "recent_changes": ["config: max_conn 100->200"],
        },
        "user-service": {"type": "microservice", "tier": "backend"},
        "notification-service": {"type": "microservice", "tier": "backend"},
        "mysql-primary": {"type": "database", "tier": "data"},
        "mysql-replica": {"type": "database", "tier": "data"},
        "redis-cache": {"type": "cache", "tier": "data"},
        "elasticsearch": {"type": "search", "tier": "data"},
        "kafka-broker": {"type": "messaging", "tier": "infra"},
    }

    for name, props in services.items():
        kg.add_node(name, node_type=props["type"], properties=props)

    dependencies = [
        ("api-gateway", "order-service"),
        ("api-gateway", "user-service"),
        ("api-gateway", "inventory-service"),
        ("order-service", "payment-service"),
        ("order-service", "inventory-service"),
        ("order-service", "user-service"),
        ("order-service", "kafka-broker"),
        ("payment-service", "mysql-primary"),
        ("payment-service", "redis-cache"),
        ("inventory-service", "mysql-primary"),
        ("inventory-service", "elasticsearch"),
        ("user-service", "mysql-replica"),
        ("user-service", "redis-cache"),
        ("notification-service", "kafka-broker"),
        ("mysql-replica", "mysql-primary"),
    ]

    for source, target in dependencies:
        kg.add_relationship(source, target, "DEPENDS_ON")

    node_deployments = [
        ("order-service", "node-1"),
        ("payment-service", "node-2"),
        ("inventory-service", "node-1"),
        ("user-service", "node-3"),
        ("mysql-primary", "node-2"),
        ("redis-cache", "node-3"),
    ]

    for svc, node in node_deployments:
        kg.add_node(node, "host")
        kg.add_relationship(svc, node, "RUNS_ON")

    return kg
