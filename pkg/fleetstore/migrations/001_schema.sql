-- FleetStore schema: Postgres-backed CR store for hyperfleet-operator.
-- See docs/design/fleetstore-design.md §4.
-- Idempotent: safe to run on every operator startup.

BEGIN;

-- ── resources: all CR kinds in a single table ──────────────────────────────────

CREATE TABLE IF NOT EXISTS resources (
  -- CR identity (maps 1:1 to ObjectMeta)
  kind        text        NOT NULL,
  namespace   text        NOT NULL,
  name        text        NOT NULL,
  uid         uuid        NOT NULL,
  generation  bigint      NOT NULL DEFAULT 1,

  -- CR payload (codec: direct JSON marshal of kubebuilder Go types)
  labels      jsonb       NOT NULL DEFAULT '{}',
  annotations jsonb       NOT NULL DEFAULT '{}',
  owner_refs  jsonb       NOT NULL DEFAULT '[]',
  finalizers  text[]      NOT NULL DEFAULT '{}',
  spec        jsonb       NOT NULL,
  status      jsonb       NOT NULL DEFAULT '{"conditions": []}',
  created_at  timestamptz NOT NULL DEFAULT now(),
  deletion_timestamp timestamptz,

  -- Store metadata (managed by triggers; codec strips on decode)
  seq         bigint      NOT NULL,
  aws_account_id text,
  updated_at  timestamptz NOT NULL,
  deleted_at  timestamptz,

  PRIMARY KEY (kind, namespace, name)
);

CREATE UNIQUE INDEX IF NOT EXISTS resources_seq ON resources (seq);
CREATE INDEX IF NOT EXISTS resources_account ON resources (aws_account_id) WHERE aws_account_id IS NOT NULL;

-- ── global_seq: the serialization point (§5) ──────────────────────────────────

CREATE TABLE IF NOT EXISTS global_seq (
  singleton bool   PRIMARY KEY DEFAULT true CHECK (singleton),
  seq       bigint NOT NULL DEFAULT 0
) WITH (fillfactor = 50, autovacuum_vacuum_scale_factor = 0.01);

INSERT INTO global_seq (singleton, seq) VALUES (true, 0) ON CONFLICT DO NOTHING;

-- ── leader: single-row leadership lease (§9) ──────────────────────────────────

CREATE TABLE IF NOT EXISTS leader (
  singleton  bool PRIMARY KEY DEFAULT true CHECK (singleton),
  holder     text NOT NULL,
  expires_at timestamptz NOT NULL
);

INSERT INTO leader (singleton, holder, expires_at)
VALUES (true, '', now() - interval '1 hour') ON CONFLICT DO NOTHING;

-- ── stamp trigger: enforces commit-ordered seq + no-op suppression (§4) ───────

CREATE OR REPLACE FUNCTION fleetstore_stamp() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'UPDATE'
     AND to_jsonb(NEW) - 'seq' - 'updated_at'
       = to_jsonb(OLD) - 'seq' - 'updated_at' THEN
    RETURN NULL;
  END IF;
  UPDATE global_seq SET seq = seq + 1 RETURNING seq INTO NEW.seq;
  NEW.updated_at := clock_timestamp();
  PERFORM pg_notify('fleetstore', '');
  RETURN NEW;
END $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS fleetstore_stamp ON resources;
CREATE TRIGGER fleetstore_stamp BEFORE INSERT OR UPDATE ON resources
FOR EACH ROW EXECUTE FUNCTION fleetstore_stamp();

COMMIT;
