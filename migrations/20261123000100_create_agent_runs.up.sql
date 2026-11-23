BEGIN;

CREATE TABLE agent_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    public_id uuid NOT NULL UNIQUE,
    principal text NOT NULL,
    status text NOT NULL CHECK (status IN ('running','responding','awaiting_tool_results','completed','incomplete','failed','cancelled')),
    revision bigint NOT NULL CHECK (revision > 0),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    state jsonb NOT NULL CHECK (jsonb_typeof(state) = 'object'),
    operation_id uuid,
    deadline_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (expires_at >= deadline_at),
    CHECK ((status IN ('running','responding')) = (operation_id IS NOT NULL))
);
CREATE INDEX agent_runs_deadline_idx ON agent_runs(deadline_at) WHERE status IN ('running','responding','awaiting_tool_results');
CREATE INDEX agent_runs_expiry_idx ON agent_runs(expires_at) WHERE status IN ('completed','incomplete','failed','cancelled');

CREATE TABLE agent_steps (
    run_id bigint NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    attempt integer NOT NULL CHECK (attempt > 0),
    step_number integer NOT NULL CHECK (step_number > 0),
    phase text NOT NULL CHECK (phase IN ('decision','work','respond')),
    model text NOT NULL,
    status text NOT NULL,
    usage jsonb NOT NULL,
    started_at timestamptz NOT NULL,
    ended_at timestamptz,
    PRIMARY KEY (run_id, attempt)
);

CREATE TABLE agent_tool_calls (
    run_id bigint NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    call_id text NOT NULL,
    provider_id text NOT NULL,
    step_number integer NOT NULL CHECK (step_number > 0),
    tool_name text NOT NULL,
    executor text NOT NULL CHECK (executor IN ('server','client')),
    arguments jsonb NOT NULL CHECK (jsonb_typeof(arguments) = 'object'),
    status text NOT NULL CHECK (status IN ('planned','running','waiting','succeeded','failed','outcome_unknown')),
    result jsonb,
    PRIMARY KEY (run_id, call_id)
);

CREATE TABLE request_idempotency (
    principal text NOT NULL,
    scope text NOT NULL,
    key_digest text NOT NULL CHECK (length(key_digest) = 64),
    request_digest text NOT NULL CHECK (length(request_digest) = 64),
    operation_id uuid NOT NULL,
    checkpoint_revision bigint NOT NULL CHECK (checkpoint_revision > 0),
    run_id bigint NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (principal, scope, key_digest)
);
CREATE INDEX request_idempotency_run_idx ON request_idempotency(run_id);

COMMIT;
