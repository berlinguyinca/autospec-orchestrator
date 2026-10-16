-- Additive and forward-only: existing reservation rows read as 0 GiB reserved.
ALTER TABLE reservations
    ADD COLUMN disk_gib BIGINT NOT NULL DEFAULT 0 CHECK (disk_gib >= 0);
