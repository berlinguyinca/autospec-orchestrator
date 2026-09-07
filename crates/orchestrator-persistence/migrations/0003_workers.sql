CREATE TABLE workers (
    id TEXT PRIMARY KEY,
    capabilities JSONB NOT NULL,
    capability_proof JSONB NOT NULL,
    state TEXT NOT NULL,
    running_executions INTEGER NOT NULL DEFAULT 0 CHECK (running_executions >= 0),
    last_heartbeat TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX workers_state_heartbeat_idx ON workers (state, last_heartbeat);
