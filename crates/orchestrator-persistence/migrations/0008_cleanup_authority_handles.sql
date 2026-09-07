ALTER TABLE cleanup_authorities
    ADD COLUMN handles JSONB NOT NULL DEFAULT '{}'::jsonb;
