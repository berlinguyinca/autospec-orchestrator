-- Expand phase only: current controllers omit this legacy duplicate while
-- controllers from the prior rollout may continue writing it. A later,
-- separately deployed contract migration may drop the column only after every
-- old controller is stopped and rollback no longer needs its request rows.
ALTER TABLE execution_requests ALTER COLUMN manifest DROP NOT NULL;

COMMENT ON COLUMN execution_requests.manifest IS
    'Legacy mixed-version compatibility only. New controllers leave this NULL and replay executions.manifest.';
