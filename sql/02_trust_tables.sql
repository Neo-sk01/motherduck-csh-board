CREATE TABLE IF NOT EXISTS trust_state (
    id              INTEGER PRIMARY KEY DEFAULT 1,
    state           VARCHAR NOT NULL DEFAULT 'blocked',
    counting_model  VARCHAR,
    approved_week   VARCHAR,
    last_signoff    TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS validation_days (
    call_date                DATE PRIMARY KEY,
    invariant_pass           BOOLEAN,
    drift_pct                DOUBLE,
    drift_status             VARCHAR,
    explanation              TEXT,
    evidence                 TEXT,
    tech_approver            VARCHAR,
    ops_approver             VARCHAR,
    tech_signoff_at          TIMESTAMP,
    ops_signoff_at           TIMESTAMP,
    final_decision           VARCHAR
);
