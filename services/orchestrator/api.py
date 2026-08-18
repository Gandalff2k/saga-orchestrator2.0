from fastapi import FastAPI, HTTPException
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from pydantic import BaseModel

from shared.tracing import init_tracing
from services.orchestrator import saga

init_tracing("orchestrator-api")
app = FastAPI()
FastAPIInstrumentor.instrument_app(app)


class OrderIn(BaseModel):
    seat_id: int
    amount: float


@app.post("/orders")
def create_order(body: OrderIn):
    conn = saga.db()
    try:
        order_id = saga.start(conn, body.seat_id, body.amount)
    finally:
        conn.close()
    return {"order_id": order_id}


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    conn = saga.db()
    try:
        with conn.cursor() as c:
            c.execute("SELECT status FROM orders WHERE order_id = %s", (order_id,))
            row = c.fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="order not found")
    return {"order_id": order_id, "status": row[0]}
