"""Global configuration loaded from environment variables with AIOPS_ prefix."""

from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Multi-Agent AIOps"
    debug: bool = False

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_group_id: str = "aiops-agents"

    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "aiops_password"

    openai_api_key: Optional[str] = None
    openai_model: str = "gpt-4o-mini"

    prometheus_url: str = "http://localhost:9090"

    monitor_check_interval: int = 30
    rca_max_depth: int = 5
    heal_dry_run: bool = True
    heal_max_retries: int = 3
    change_auto_approve_threshold: float = 0.3

    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 60
    rate_limit_per_minute: int = 30
    blast_radius_max_percent: float = 0.2

    model_config = {"env_prefix": "AIOPS_", "env_file": ".env"}


settings = Settings()
