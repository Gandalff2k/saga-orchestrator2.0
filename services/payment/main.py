
import os

import psycopg2

from shared import kafka
from shared.outbox import write_event
from shared.tracing import init_tracing

COMMANDS_TOPIC = "payment.commands"
GROUP = "payment"
REPLIES = "orchestrator.replies"
FAIL_MODE = os.environ.get("FAIL_MODE", "off")


def db():
    return psycopg2.connect(os.environ["PAYMENT_DSN"])


def handle(conn, event: dict) -> None:
    saga_id = event["saga_id"]
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO inbox (event_id) VALUES (%s) ON CONFLICT DO NOTHING", (event["id"],))
        if cur.rowcount == 0:
            conn.commit()
            return

        if event["type"] == "charge":
            if FAIL_MODE == "always" or float(event["amount"]) < 0:
                write_event(cur, REPLIES, saga_id, "payment_failed", {"saga_id": saga_id})
            else:
                cur.execute(
                    "INSERT INTO payments (saga_id, amount, status) VALUES (%s,%s,'charged') "
                    "ON CONFLICT (saga_id) DO NOTHING",
                    (saga_id, event["amount"]),
                )
                write_event(cur, REPLIES, saga_id, "payment_charged", {"saga_id": saga_id})

        elif event["type"] == "refund":
            cur.execute(
                "UPDATE payments SET status='refunded' WHERE saga_id=%s AND status='charged'", (saga_id,)
            )
            write_event(cur, REPLIES, saga_id, "payment_refunded", {"saga_id": saga_id})

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def main() -> None:
    init_tracing("payment")
    conn = db()
    kafka.consume([COMMANDS_TOPIC], GROUP, lambda e: handle(conn, e))


if __name__ == "__main__":
    main()
