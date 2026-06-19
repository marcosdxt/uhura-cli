-- Schema base do Uhura no PostgreSQL (ver SPEC.md §10/§12/§13).
-- Idempotente: pode ser aplicado repetidamente (uhura db init).

-- Outbox de eventos de domínio (append-only). Lido pelo dispatcher.
CREATE TABLE IF NOT EXISTS uhura_outbox (
    id            BIGSERIAL    PRIMARY KEY,
    domain        TEXT         NOT NULL,
    event         TEXT         NOT NULL,
    partitionkey  TEXT,
    envelope      JSONB        NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

-- Índice parcial: varredura barata das linhas ainda não publicadas, em ordem.
CREATE INDEX IF NOT EXISTS uhura_outbox_unpublished
    ON uhura_outbox (id) WHERE published_at IS NULL;

-- Inbox de deduplicação (idempotência). PK = envelope.id.
CREATE TABLE IF NOT EXISTS uhura_inbox (
    envelope_id   TEXT         PRIMARY KEY,
    domain        TEXT         NOT NULL,
    partitionkey  TEXT,
    processed_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Acorda o dispatcher via LISTEN/NOTIFY a cada novo evento (fallback: polling).
CREATE OR REPLACE FUNCTION uhura_outbox_notify() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('uhura_outbox', NEW.domain);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS uhura_outbox_notify ON uhura_outbox;
CREATE TRIGGER uhura_outbox_notify
    AFTER INSERT ON uhura_outbox
    FOR EACH ROW EXECUTE FUNCTION uhura_outbox_notify();
