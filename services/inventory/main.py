import os

import psycopg2

from shared import kafka
from shared.outbox import write_event
from shared.tracing import init_tracing

COMMANDS_TOPIC = "inventory.commands"
GROUP = "inventory"
REPLIES = "orchestrator.replies"


def db():
    return psycopg2.connect(os.environ["INVENTORY_DSN"])


def handle(conn, event: dict) -> None:
    saga_id, seat_id = event["saga_id"], event.get("seat_id")
    cur = conn.cursor()
    try:
        # 1. dedup: if we've seen this event id, skip 
        cur.execute("INSERT INTO inbox (event_id) VALUES (%s) ON CONFLICT DO NOTHING", (event["id"],))
        if cur.rowcount == 0:
            conn.commit()
            return

        if event["type"] == "reserve_seat":
            cur.execute(
                "UPDATE seats SET status='reserved' WHERE seat_id=%s AND status='free'", (seat_id,)
            )
            if cur.rowcount == 1:
                cur.execute(
                    "INSERT INTO reservations (saga_id, seat_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (saga_id, seat_id),
                )
                write_event(cur, REPLIES, saga_id, "seat_reserved", {"saga_id": saga_id})
            else:
                # seat wasn't free — was it us (idempotent retry) or someone else?
                cur.execute(
                    "SELECT 1 FROM reservations WHERE saga_id=%s AND seat_id=%s", (saga_id, seat_id)
                )
                kind = "seat_reserved" if cur.fetchone() else "seat_failed"
                write_event(cur, REPLIES, saga_id, kind, {"saga_id": saga_id})

        elif event["type"] == "release_seat":
            cur.execute("UPDATE seats SET status='free' WHERE seat_id=%s", (seat_id,))
            cur.execute(
                "UPDATE reservations SET status='released' WHERE saga_id=%s AND seat_id=%s",
                (saga_id, seat_id),
            )
            write_event(cur, REPLIES, saga_id, "seat_released", {"saga_id": saga_id})

        conn.commit()   # inbox + business change + outbox reply commit together
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def main() -> None:
    init_tracing("inventory")
    conn = db()
    kafka.consume([COMMANDS_TOPIC], GROUP, lambda e: handle(conn, e))


if __name__ == "__main__":
    main()
