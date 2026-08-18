import os
import uuid

import psycopg2

from shared.outbox import write_event

INVENTORY_Q = "inventory.commands"
PAYMENT_Q = "payment.commands"

RESERVING, CHARGING, COMPENSATING, DONE, ABORTED, CONFIRMED = (
    "RESERVING", "CHARGING", "COMPENSATING", "DONE", "ABORTED", "CONFIRMED")


def db():
    return psycopg2.connect(os.environ["ORCHESTRATOR_DSN"])


def start(conn, seat_id: int, amount: float) -> str:
    order_id = str(uuid.uuid4())
    saga_id = str(uuid.uuid4())
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO orders (order_id, seat_id, amount, status) VALUES (%s,%s,%s,'PENDING')",
            (order_id, seat_id, amount),
        )
        cur.execute(
            "INSERT INTO saga_instance (saga_id, order_id, state, seat_id, amount) VALUES (%s,%s,%s,%s,%s)",
            (saga_id, order_id, RESERVING, seat_id, amount),
        )
        write_event(cur, INVENTORY_Q, saga_id, "reserve_seat", {"saga_id": saga_id, "seat_id": seat_id})
        conn.commit()   # order + saga + first command commit together 
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
    return order_id


def advance(conn, event: dict) -> None:
    cur = conn.cursor()
    try:
        # dedup this reply 
        cur.execute("INSERT INTO inbox (event_id) VALUES (%s) ON CONFLICT DO NOTHING", (event["id"],))
        if cur.rowcount == 0:
            conn.commit()
            return

        cur.execute(
            "SELECT state, order_id, seat_id, amount FROM saga_instance WHERE saga_id=%s",
            (event["saga_id"],),
        )
        row = cur.fetchone()
        if not row:
            conn.commit()
            return
        state, order_id, seat_id, amount = row
        etype = event["type"]

        if state == RESERVING and etype == "seat_reserved":
            write_event(cur, PAYMENT_Q, event["saga_id"], "charge",
                        {"saga_id": event["saga_id"], "amount": float(amount)})
            cur.execute("UPDATE saga_instance SET state=%s WHERE saga_id=%s", (CHARGING, event["saga_id"]))
        elif state == RESERVING and etype == "seat_failed":
            cur.execute("UPDATE orders SET status=%s WHERE order_id=%s", (ABORTED, order_id))
            cur.execute("UPDATE saga_instance SET state=%s WHERE saga_id=%s", (ABORTED, event["saga_id"]))
        elif state == CHARGING and etype == "payment_charged":
            cur.execute("UPDATE orders SET status=%s WHERE order_id=%s", (CONFIRMED, order_id))
            cur.execute("UPDATE saga_instance SET state=%s WHERE saga_id=%s", (DONE, event["saga_id"]))
        elif state == CHARGING and etype == "payment_failed":
            write_event(cur, INVENTORY_Q, event["saga_id"], "release_seat",
                        {"saga_id": event["saga_id"], "seat_id": seat_id})
            cur.execute("UPDATE saga_instance SET state=%s WHERE saga_id=%s", (COMPENSATING, event["saga_id"]))
        elif state == COMPENSATING and etype == "seat_released":
            cur.execute("UPDATE orders SET status=%s WHERE order_id=%s", (ABORTED, order_id))
            cur.execute("UPDATE saga_instance SET state=%s WHERE saga_id=%s", (ABORTED, event["saga_id"]))

        conn.commit()   # inbox + state change + emitted command commit together
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
