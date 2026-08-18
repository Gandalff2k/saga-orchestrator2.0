from shared import kafka
from services.orchestrator import saga

REPLIES_TOPIC = "orchestrator.replies"
GROUP = "orchestrator"


def main() -> None:
    from shared.tracing import init_tracing
    init_tracing("orchestrator")
    conn = saga.db()
    kafka.consume([REPLIES_TOPIC], GROUP, lambda e: saga.advance(conn, e))


if __name__ == "__main__":
    main()
