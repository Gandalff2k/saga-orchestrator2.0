import json
import os
import time

from confluent_kafka import Consumer, Producer
from opentelemetry import propagate, trace
from opentelemetry.trace import Status, StatusCode

_tracer = trace.get_tracer("kafka")
MAX_ATTEMPTS = 3

_producer = None


def _producer_():
    global _producer
    if _producer is None:
        _producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]})
    return _producer


def _to_dlt(msg, error) -> None:
    """Park a bad message on <topic>.DLT so it stops blocking the partition."""
    record = {
        "error": str(error),
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "value": msg.value().decode() if msg.value() else None,
    }
    p = _producer_()
    p.produce(f"{msg.topic()}.DLT", key=msg.key(), value=json.dumps(record).encode())
    p.flush(5)


def consume(topics: list[str], group_id: str, handler) -> None:
    c = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,     # we commit manually, AFTER handling
    })
    c.subscribe(topics)

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("kafka error:", msg.error())
                continue

            # Pull trace context out of the Kafka header Debezium set.
            headers = dict(msg.headers() or [])
            tp = headers.get("traceparent")
            carrier = {"traceparent": tp.decode() if isinstance(tp, bytes) else tp} if tp else {}
            ctx = propagate.extract(carrier)

            with _tracer.start_as_current_span("handle", context=ctx) as span:
                for attempt in range(1, MAX_ATTEMPTS + 1):
                    try:
                        event = json.loads(msg.value())
                        if isinstance(event, str):
                            event = json.loads(event)   # Debezium ships jsonb as a JSON string
                        span.update_name(f"handle {event.get('type', '?')}")
                        handler(event)
                        break                                    # success
                    except Exception as e:
                        if attempt < MAX_ATTEMPTS:
                            time.sleep(2 ** (attempt - 1))       #  backoff
                            continue
                        # retries exhausted - dead-letter it and move on
                        print(f"DLQ: {msg.topic()}@{msg.offset()} -> {e}")
                        span.record_exception(e)
                        span.set_status(Status(StatusCode.ERROR))
                        _to_dlt(msg, e)
                c.commit(msg)   # advance past this message whether it succeeded or goes to  DLQ
    finally:
        c.close()               # graceful: commit offsets + leave the group cleanly
