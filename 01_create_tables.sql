-- ============================================================
-- METADATA STORE — Full DDL
-- Schema separation: config.* and audit.*
-- Run order: schemas → config tables → audit tables → indexes
-- ============================================================
 
-- ============================================================
-- CREATE SCHEMAS
-- ============================================================
DROP SCHEMA IF EXISTS config CASCADE;
DROP SCHEMA IF EXISTS audit  CASCADE;
CREATE SCHEMA IF NOT EXISTS config;
CREATE SCHEMA IF NOT EXISTS audit;
 
-- ============================================================
-- CONFIG SCHEMA — setup tables (written once, rarely changed)
-- ============================================================
 
-- 1. config.job — master job registry (schedule merged in)
CREATE TABLE IF NOT EXISTS config.job (
    job_id          SERIAL          PRIMARY KEY,
    client_id       VARCHAR(100),
    job_name        VARCHAR(255)    NOT NULL,
    job_description TEXT,
    source_system   VARCHAR(100),
    secret_scope    VARCHAR(255),
    secret_key_name VARCHAR(255),
    cron_expression VARCHAR(100),                       -- e.g. 0 2 * * *
    frequency       VARCHAR(50),                        -- Daily / Hourly / Weekly
    timezone        VARCHAR(100)    DEFAULT 'UTC',
    schedule_type   VARCHAR(50),                        -- Manual / Scheduled / Dependency
    next_run        TIMESTAMP,
    last_run        TIMESTAMP,
    is_active       BOOLEAN         DEFAULT TRUE,
    created_by      VARCHAR(100),
    created_date    TIMESTAMP       DEFAULT NOW(),
    updated_by      VARCHAR(100),
    updated_date    TIMESTAMP
);
 
-- 3. config.job_step — step definitions per job (step 1 to step N)
CREATE TABLE IF NOT EXISTS config.job_step (
    step_id         SERIAL          PRIMARY KEY,
    job_id          INT             NOT NULL REFERENCES config.job(job_id) ON DELETE CASCADE,
    step_name       VARCHAR(255)    NOT NULL,
    step_type       VARCHAR(50)     NOT NULL,           -- READ_CSV / REGISTER_VIEW / TRANSFORM_SQL / WRITE_PARQUET
    step_action     TEXT,                       -- e.g. Points to SQL file, python script, stored code etc.
    source_path     VARCHAR(500),
    target_path     VARCHAR(500),
    sequence        INT             NOT NULL,
    is_active       boolean         DEFAULT TRUE,
    created_by      VARCHAR(100),
    created_date    TIMESTAMP       DEFAULT NOW(),
    updated_by      VARCHAR(100),
    updated_date    TIMESTAMP
);
 
-- 4. config.step_param — parameters substituted into each step at runtime
CREATE TABLE IF NOT EXISTS config.step_param (
    param_id            SERIAL          PRIMARY KEY,
    step_id             INT             NOT NULL REFERENCES config.job_step(step_id) ON DELETE CASCADE,
    param_name          VARCHAR(255)    NOT NULL,
    param_value         TEXT,
    param_data_type TEXT CHECK (param_data_type IN ('STRING', 'INT', 'JSON', 'SQL')),                     -- e.g. STRING / INT / JSON / SQL
    resource_group      VARCHAR(255),
    fields              TEXT,                           -- comma-separated field list
    filters             TEXT,                           -- optional JSON filters
    delta_to_pull       VARCHAR(100),                   -- e.g. Current_Date -1 / full_refresh
    masked_fields       TEXT,
    validation_rules    TEXT,                           -- optional JSON
    is_active           boolean         DEFAULT TRUE,
    created_by          VARCHAR(100),
    created_date        TIMESTAMP       DEFAULT NOW(),
    updated_by          VARCHAR(100),
    updated_date        TIMESTAMP
);
 
-- ============================================================
-- AUDIT SCHEMA — runtime tables (written every execution)
-- ============================================================
 
-- 5. audit.job_run — one row per job execution
CREATE TABLE IF NOT EXISTS audit.job_run (
    job_run_id      SERIAL          PRIMARY KEY,
    job_id          INT             NOT NULL REFERENCES config.job(job_id),
    run_status      VARCHAR(20),                        -- RUNNING / SUCCESS / FAILED / PARTIAL
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    triggered_by    VARCHAR(50),                        -- Manual / Schedule / Dependency
    rows_processed  BIGINT          DEFAULT 0,
    error_count     INT             DEFAULT 0,
    runtime_seconds INT
);
 
-- 6. audit.step_run — one row per step per execution
CREATE TABLE IF NOT EXISTS audit.step_run (
    step_run_id     SERIAL          PRIMARY KEY,
    job_run_id      INT             NOT NULL REFERENCES audit.job_run(job_run_id),
    step_id         INT             NOT NULL REFERENCES config.job_step(step_id),
    step_status     VARCHAR(20),                        -- RUNNING / SUCCESS / FAILED
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    rows_in         BIGINT          DEFAULT 0,
    rows_out        BIGINT          DEFAULT 0,
    runtime_seconds INT
);
 
-- 7. audit.batch — delta window detail per job run
CREATE TABLE IF NOT EXISTS audit.batch (
    batch_id        SERIAL          PRIMARY KEY,
    job_run_id      INT             NOT NULL REFERENCES audit.job_run(job_run_id),
    delta_to_pull   VARCHAR(100),
    batch_status    VARCHAR(20),                        -- SUCCESS / FAILED
    rows_processed  BIGINT          DEFAULT 0,
    rows_failed     BIGINT          DEFAULT 0,
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    runtime_seconds INT
);
 
-- 8. audit.log — free-text log lines (job and step level)
CREATE TABLE IF NOT EXISTS audit.log (
    log_id          SERIAL          PRIMARY KEY,
    job_run_id      INT             NOT NULL REFERENCES audit.job_run(job_run_id),
    step_run_id     INT             REFERENCES audit.step_run(step_run_id),  -- nullable: job-level logs have no step
    log_level       VARCHAR(10)     NOT NULL,           -- INFO / WARN / ERROR / DEBUG
    log_message     TEXT,
    stack_trace     TEXT,
    log_time        TIMESTAMP       DEFAULT NOW()
);
 
-- 9. audit.error — structured error records (job and step level)
CREATE TABLE IF NOT EXISTS audit.error (
    error_id        SERIAL          PRIMARY KEY,
    job_run_id      INT             NOT NULL REFERENCES audit.job_run(job_run_id),
    step_run_id     INT             REFERENCES audit.step_run(step_run_id),  -- nullable: job-level errors
    error_type      VARCHAR(50),                        -- EXTRACTION / TRANSFORMATION / LOAD / NETWORK / SCHEMA / PERMISSION
    error_message   TEXT,
    error_detail    TEXT,
    error_code      VARCHAR(50),
    is_resolved     BOOLEAN         DEFAULT FALSE,
    resolved_date   TIMESTAMP,
    resolved_by     VARCHAR(100)
);
 
-- ============================================================
-- INDEXES
-- ============================================================
 
-- config schema
CREATE INDEX IF NOT EXISTS idx_job_step_job_id        ON config.job_step(job_id);
CREATE INDEX IF NOT EXISTS idx_step_param_step_id     ON config.step_param(step_id);
 
-- audit schema
CREATE INDEX IF NOT EXISTS idx_job_run_job_id         ON audit.job_run(job_id);
CREATE INDEX IF NOT EXISTS idx_job_run_status         ON audit.job_run(run_status);
CREATE INDEX IF NOT EXISTS idx_step_run_job_run_id    ON audit.step_run(job_run_id);
CREATE INDEX IF NOT EXISTS idx_step_run_step_id       ON audit.step_run(step_id);
CREATE INDEX IF NOT EXISTS idx_batch_job_run_id       ON audit.batch(job_run_id);
CREATE INDEX IF NOT EXISTS idx_log_job_run_id         ON audit.log(job_run_id);
CREATE INDEX IF NOT EXISTS idx_log_step_run_id        ON audit.log(step_run_id);
CREATE INDEX IF NOT EXISTS idx_error_job_run_id       ON audit.error(job_run_id);
CREATE INDEX IF NOT EXISTS idx_error_step_run_id      ON audit.error(step_run_id);
 