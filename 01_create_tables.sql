-- ============================================================
-- METADATA STORE - DDL Script
-- DI Accelerator POC
-- Creates all CONFIG, AUDIT, and SCHEDULE tables
-- ============================================================

-- ============================================================
-- CONFIG TABLES
-- ============================================================

-- 1. ETL_JOB_CONFIG: Master job registry
CREATE TABLE IF NOT EXISTS ETL_JOB_CONFIG (
    JOB_ID          SERIAL PRIMARY KEY,
    CLIENT_ID       VARCHAR(100),
    JOB_NAME        VARCHAR(255) NOT NULL,
    JOB_DESCRIPTION TEXT,
    SOURCE_SYSTEM   VARCHAR(100),           -- e.g., ProviderA_CSV, GoogleAds
    SECRET_SCOPE    VARCHAR(255),           -- secret manager scope reference
    SECRET_KEY_NAME VARCHAR(255),           -- secret key identifier
    STATUS          VARCHAR(20)  DEFAULT 'ACTIVE',   -- ACTIVE / INACTIVE
    IS_ACTIVE       CHAR(1)      DEFAULT 'Y',
    CREATED_BY      VARCHAR(100),
    CREATED_DATE    TIMESTAMP    DEFAULT NOW(),
    UPDATED_BY      VARCHAR(100),
    UPDATED_DATE    TIMESTAMP,
    VERSION         INT          DEFAULT 1
);

-- 2. ETL_PARAM_CONFIG: Job execution parameters
CREATE TABLE IF NOT EXISTS ETL_PARAM_CONFIG (
    PARAM_ID         SERIAL PRIMARY KEY,
    JOB_ID           INT REFERENCES ETL_JOB_CONFIG(JOB_ID) ON DELETE CASCADE,
    RESOURCE_GROUP   VARCHAR(255),          -- e.g., customer_master_a, Campaign
    PARAM_NAME       VARCHAR(255),
    FIELDS           TEXT,                  -- comma-separated field list
    FILTERS          TEXT,                  -- optional JSON filters
    DELTA_TO_PULL    VARCHAR(100),          -- e.g., Current_Date -1, full_refresh
    PARAM_TYPE       VARCHAR(50),           -- ETL / DLT / TRANSFORM
    MASKED_FIELDS    TEXT,                  -- sensitive fields to mask
    SEQUENCE         INT DEFAULT 1,         -- execution order
    VALIDATION_RULES TEXT,                  -- optional JSON validation rules
    DEPENDENCIES     TEXT,                  -- references other PARAM_IDs
    DESCRIPTION      TEXT,
    IS_ACTIVE        CHAR(1) DEFAULT 'Y',
    CREATED_BY       VARCHAR(100),
    CREATED_DATE     TIMESTAMP DEFAULT NOW(),
    UPDATED_BY       VARCHAR(100),
    UPDATED_DATE     TIMESTAMP
);

-- ============================================================
-- SCHEDULE TABLES
-- ============================================================

-- 3. ETL_JOB_SCHEDULE: When to run each job
CREATE TABLE IF NOT EXISTS ETL_JOB_SCHEDULE (
    SCHEDULE_ID     SERIAL PRIMARY KEY,
    JOB_ID          INT REFERENCES ETL_JOB_CONFIG(JOB_ID) ON DELETE CASCADE,
    CRON_EXPRESSION VARCHAR(100),           -- e.g., 0 2 * * *
    FREQUENCY       VARCHAR(50),            -- Daily / Hourly / Weekly
    TIMEZONE        VARCHAR(100) DEFAULT 'UTC',
    NEXT_RUN        TIMESTAMP,
    LAST_RUN        TIMESTAMP,
    SCHEDULE_TYPE   VARCHAR(50),            -- Manual / Scheduled / Dependency
    IS_ACTIVE       CHAR(1) DEFAULT 'Y',
    CREATED_BY      VARCHAR(100),
    CREATED_DATE    TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- AUDIT TABLES
-- ============================================================

-- 4. AUDIT_ETL_JOB: Top-level job run record
CREATE TABLE IF NOT EXISTS AUDIT_ETL_JOB (
    JOB_AUDIT_ID     SERIAL PRIMARY KEY,
    JOB_ID           INT REFERENCES ETL_JOB_CONFIG(JOB_ID),
    RUN_STATUS       VARCHAR(20),           -- SUCCESS / FAILED / PARTIAL
    START_TIME       TIMESTAMP,
    END_TIME         TIMESTAMP,
    TRIGGERED_BY     VARCHAR(50),           -- Manual / Schedule / Dependency
    ROWS_PROCESSED   BIGINT  DEFAULT 0,
    ERROR_COUNT      INT     DEFAULT 0,
    RUNTIME_SECONDS  INT
);

-- 5. AUDIT_ETL_BATCH: Batch-level detail
CREATE TABLE IF NOT EXISTS AUDIT_ETL_BATCH (
    BATCH_ID         SERIAL PRIMARY KEY,
    JOB_AUDIT_ID     INT REFERENCES AUDIT_ETL_JOB(JOB_AUDIT_ID),
    DELTA_TO_PULL    VARCHAR(100),
    BATCH_STATUS     VARCHAR(20),           -- SUCCESS / FAILED
    START_TIME       TIMESTAMP,
    END_TIME         TIMESTAMP,
    ROWS_PROCESSED   BIGINT DEFAULT 0,
    ROWS_FAILED      BIGINT DEFAULT 0,
    RUNTIME_SECONDS  INT
);

-- 6. AUDIT_ETL_JOB_LOG: Free-text log lines
CREATE TABLE IF NOT EXISTS AUDIT_ETL_JOB_LOG (
    LOG_ID           SERIAL PRIMARY KEY,
    JOB_AUDIT_ID     INT REFERENCES AUDIT_ETL_JOB(JOB_AUDIT_ID),
    LOG_LEVEL        VARCHAR(10),           -- INFO / WARN / ERROR / DEBUG
    LOG_MESSAGE      TEXT,
    STACK_TRACE      TEXT,
    LOG_TIME         TIMESTAMP DEFAULT NOW()
);

-- 7. AUDIT_ETL_ERROR: Structured error records
CREATE TABLE IF NOT EXISTS AUDIT_ETL_ERROR (
    ERROR_ID         SERIAL PRIMARY KEY,
    JOB_AUDIT_ID     INT REFERENCES AUDIT_ETL_JOB(JOB_AUDIT_ID),
    ERROR_TYPE       VARCHAR(50),           -- EXTRACTION / TRANSFORMATION / LOAD / NETWORK / SCHEMA / PERMISSION
    ERROR_MESSAGE    TEXT,
    ERROR_DETAIL     TEXT,
    IS_RESOLVED      BOOLEAN DEFAULT FALSE,
    RESOLVED_DATE    TIMESTAMP,
    RESOLVED_BY      VARCHAR(100),
    ERROR_CODE       VARCHAR(50)
);

-- ============================================================
-- INDEXES for performance
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_etl_param_job_id    ON ETL_PARAM_CONFIG(JOB_ID);
CREATE INDEX IF NOT EXISTS idx_schedule_job_id     ON ETL_JOB_SCHEDULE(JOB_ID);
CREATE INDEX IF NOT EXISTS idx_audit_job_job_id    ON AUDIT_ETL_JOB(JOB_ID);
CREATE INDEX IF NOT EXISTS idx_audit_batch_audit   ON AUDIT_ETL_BATCH(JOB_AUDIT_ID);
CREATE INDEX IF NOT EXISTS idx_audit_log_audit     ON AUDIT_ETL_JOB_LOG(JOB_AUDIT_ID);
CREATE INDEX IF NOT EXISTS idx_audit_error_audit   ON AUDIT_ETL_ERROR(JOB_AUDIT_ID);

SELECT 'All Metadata Store tables created successfully.' AS status;
