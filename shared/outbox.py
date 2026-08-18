import json
import uuid

from opentelemetry import propagate


def write_event(cur, aggregate_type: str, aggregate_id: str, event_type: str, payload: dict) -> None:
    event_id = str(uuid.uuid4())

    # Capture the current trace context so the trace survives DB - Debezium - Kafka.\
    carrier: dict = {}
    propagate.inject(carrier)

    body = {"id": event_id, "type": event_type, **payload}

    cur.execute(
        """INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload, tracingspancontext)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (event_id, aggregate_type, aggregate_id, event_type, json.dumps(body), carrier.get("traceparent")),
    )
