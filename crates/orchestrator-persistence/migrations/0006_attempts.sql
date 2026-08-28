CREATE TABLE execution_attempts (
    attempt_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL REFERENCES workers(id) ON DELETE RESTRICT,
    state TEXT NOT NULL,
    worktree_path TEXT,
    session_id TEXT,
    result JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX one_active_attempt_per_execution
    ON execution_attempts (execution_id)
    WHERE finished_at IS NULL;
