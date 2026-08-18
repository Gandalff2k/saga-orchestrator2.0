-- v2: outbox pattern. Each service DB has:
--   business tables + an OUTBOX table (Debezium tails it) + an INBOX table (dedup).
-- The outbox columns match Debezium's Outbox Event Router defaults.

CREATE DATABASE orchestrator;
CREATE DATABASE inventory;
CREATE DATABASE payment;

-- ============================================================
\connect orchestrator
-- ============================================================
CREATE TABLE orders (
    order_id   uuid PRIMARY KEY,
    seat_id    int NOT NULL,
    amount     numeric(12,2) NOT NULL,
    status     text NOT NULL DEFAULT 'PENDING',   -- PENDING | CONFIRMED | ABORTED
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE saga_instance (
    saga_id    uuid PRIMARY KEY,
    order_id   uuid NOT NULL REFERENCES orders(order_id),
    state      text NOT NULL,
    seat_id    int NOT NULL,
    amount     numeric(12,2) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- OUTBOX: an event written in the SAME tx as the business change. Debezium ships it.
CREATE TABLE outbox (
    id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregatetype      text NOT NULL,   -- Kafka topic (e.g. "inventory.commands")
    aggregateid        text NOT NULL,   -- Kafka message KEY (saga_id => per-saga ordering)
    type               text NOT NULL,   -- command/event name 
    payload            jsonb NOT NULL,  -- the message body
    tracingspancontext text,            -- W3C traceparent, forwarded to a Kafka header
    created_at         timestamptz NOT NULL DEFAULT now()
);

-- INBOX: dedup consumed events
CREATE TABLE inbox (
    event_id   uuid PRIMARY KEY,
    handled_at timestamptz NOT NULL DEFAULT now()
);

-- ============================================================
\connect inventory
-- ============================================================
CREATE TABLE seats (
    seat_id int PRIMARY KEY,
    status  text NOT NULL DEFAULT 'free'
);
CREATE TABLE reservations (
    reservation_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    seat_id        int NOT NULL REFERENCES seats(seat_id),
    saga_id        uuid NOT NULL UNIQUE,
    status         text NOT NULL DEFAULT 'held'
);
INSERT INTO seats (seat_id, status) SELECT g, 'free' FROM generate_series(1, 20) AS g;

CREATE TABLE outbox (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregatetype text NOT NULL, aggregateid text NOT NULL, type text NOT NULL,
    payload jsonb NOT NULL, tracingspancontext text,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE inbox (event_id uuid PRIMARY KEY, handled_at timestamptz NOT NULL DEFAULT now());

-- ============================================================
\connect payment
-- ============================================================
CREATE TABLE payments (
    payment_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    saga_id    uuid NOT NULL UNIQUE,
    amount     numeric(12,2) NOT NULL,
    status     text NOT NULL
);
CREATE TABLE outbox (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregatetype text NOT NULL, aggregateid text NOT NULL, type text NOT NULL,
    payload jsonb NOT NULL, tracingspancontext text,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE inbox (event_id uuid PRIMARY KEY, handled_at timestamptz NOT NULL DEFAULT now());
