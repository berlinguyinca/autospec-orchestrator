UPDATE cleanup_authorities AS authority
SET phase = CASE authority.phase
    WHEN 'RESERVED' THEN 'ACTIVE:RESERVED'
    WHEN 'STORAGE_ALLOCATED' THEN 'ACTIVE:STORAGE'
    WHEN 'WORKTREE_CREATED' THEN 'ACTIVE:WORKTREE'
    WHEN 'RUNTIME_CREATED' THEN 'ACTIVE:RUNTIME'
    WHEN 'PI_STARTED' THEN 'ACTIVE:PI_STARTED'
    WHEN 'RUNNING' THEN 'ACTIVE:RUNNING'
    WHEN 'REVIEW_READY' THEN CASE
        WHEN execution.manifest ->> 'persistence' = 'resumable' THEN 'RETAIN_REQUESTED'
        ELSE 'CLEANUP_PENDING'
    END
    ELSE authority.phase
END
FROM executions AS execution
WHERE execution.id = authority.execution_id;

ALTER TABLE cleanup_authorities
    ADD CONSTRAINT cleanup_authorities_phase_check CHECK (phase IN (
        'ACTIVE:RESERVED',
        'ACTIVE:STORAGE',
        'ACTIVE:WORKTREE',
        'ACTIVE:RUNTIME',
        'ACTIVE:PI_STARTED',
        'ACTIVE:RUNNING',
        'ACTIVE:POST_PI_BEFORE_EVENT',
        'RETAIN_REQUESTED',
        'RETAINED',
        'CLEANUP_PENDING',
        'RUNTIME_STOPPED',
        'RUNTIME_DESTROYED',
        'GIT_RECOVERED_CLEANED',
        'STORAGE_RELEASED',
        'RESERVATION_RELEASED',
        'RESOLVED'
    ));
