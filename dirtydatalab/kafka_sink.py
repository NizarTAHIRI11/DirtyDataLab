"""Optional Kafka integration (pip install kafka-python)."""
from __future__ import annotations

import json


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, topic: str):
        if not bootstrap_servers.strip() or not topic.strip():
            raise ValueError("Le serveur Kafka et le topic sont obligatoires.")
        try:
            from kafka import KafkaProducer
        except ImportError as exc:
            raise RuntimeError("Module Kafka manquant : pip install kafka-python") from exc
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=[x.strip() for x in bootstrap_servers.split(",")],
            value_serializer=lambda row: json.dumps(row, ensure_ascii=False).encode("utf-8"),
            api_version_auto_timeout_ms=5000,
            request_timeout_ms=15000,
        )

    def send(self, row: dict) -> None:
        self.producer.send(self.topic, row)

    def flush(self) -> None:
        self.producer.flush(timeout=15)

    def close(self) -> None:
        self.producer.close(timeout=15)
