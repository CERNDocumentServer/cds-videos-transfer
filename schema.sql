CREATE TABLE migration_state (
    recid_video TEXT PRIMARY KEY,
    migration_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uploaded BOOLEAN NOT NULL,
    migration_message TEXT NOT NULL CHECK(LENGTH(migration_message) <= 200)
);
