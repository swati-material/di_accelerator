-- ============================================================
-- METADATA STORE - Seed Data
-- DI Accelerator POC: Multi-Source Customer Activity Consolidation
-- ============================================================

-- ============================================================
-- STEP 1: Register the Job
-- ============================================================

INSERT INTO ETL_JOB_CONFIG (
    CLIENT_ID, JOB_NAME, JOB_DESCRIPTION,
    SOURCE_SYSTEM, STATUS, IS_ACTIVE,
    CREATED_BY, CREATED_DATE, VERSION
) VALUES (
    'CLIENT_001',
    'Customer Activity Consolidation',
    'Consolidates customer master data from Provider A and B, joins with activity records, and produces SILVER and GOLD outputs.',
    'CSV_S3',
    'ACTIVE', 'Y',
    'admin', NOW(), 1
);

-- ============================================================
-- STEP 2: Register Execution Parameters (one row per input)
-- SEQUENCE drives execution order in run_etl_job.py
-- ============================================================

-- Param 1: Provider A Customer Master (full refresh)
INSERT INTO ETL_PARAM_CONFIG (
    JOB_ID, RESOURCE_GROUP, PARAM_NAME,
    FIELDS, DELTA_TO_PULL, PARAM_TYPE,
    SEQUENCE, IS_ACTIVE, CREATED_BY, CREATED_DATE
) VALUES (
    1,
    'customer_master_a',
    'Provider A Master Ingest',
    'customer_id,customer_name,email,region,signup_date',
    'full_refresh',
    'ETL',
    1, 'Y', 'admin', NOW()
);

-- Param 2: Provider B Customer Master (full refresh)
INSERT INTO ETL_PARAM_CONFIG (
    JOB_ID, RESOURCE_GROUP, PARAM_NAME,
    FIELDS, DELTA_TO_PULL, PARAM_TYPE,
    SEQUENCE, IS_ACTIVE, CREATED_BY, CREATED_DATE
) VALUES (
    1,
    'customer_master_b',
    'Provider B Master Ingest',
    'customer_id,customer_name,phone,country,created_date',
    'full_refresh',
    'ETL',
    2, 'Y', 'admin', NOW()
);

-- Param 3: Customer Activity (incremental daily delta)
INSERT INTO ETL_PARAM_CONFIG (
    JOB_ID, RESOURCE_GROUP, PARAM_NAME,
    FIELDS, DELTA_TO_PULL, PARAM_TYPE,
    SEQUENCE, IS_ACTIVE, CREATED_BY, CREATED_DATE
) VALUES (
    1,
    'customer_activity',
    'Customer Activity Delta Ingest',
    'activity_id,customer_id,activity_type,activity_date,amount',
    'Current_Date -1',
    'ETL',
    3, 'Y', 'admin', NOW()
);

-- Param 4: Silver Transformation - Curated Customer Activity
INSERT INTO ETL_PARAM_CONFIG (
    JOB_ID, RESOURCE_GROUP, PARAM_NAME,
    FIELDS, DELTA_TO_PULL, PARAM_TYPE,
    SEQUENCE, DESCRIPTION, IS_ACTIVE, CREATED_BY, CREATED_DATE
) VALUES (
    1,
    'curated_customer_activity',
    'Silver Layer Transform',
    'customer_id,customer_name,email,region,activity_type,activity_date,amount',
    'Current_Date -1',
    'TRANSFORM',
    4,
    'Join customer master (A+B) with activity records. Apply dedup and schema validation.',
    'Y', 'admin', NOW()
);

-- Param 5: Gold Aggregation - KPI Summary
INSERT INTO ETL_PARAM_CONFIG (
    JOB_ID, RESOURCE_GROUP, PARAM_NAME,
    FIELDS, DELTA_TO_PULL, PARAM_TYPE,
    SEQUENCE, DESCRIPTION, IS_ACTIVE, CREATED_BY, CREATED_DATE
) VALUES (
    1,
    'activity_kpi_summary',
    'Gold Layer Aggregation',
    'region,activity_type,total_amount,activity_count,report_date',
    'Current_Date -1',
    'TRANSFORM',
    5,
    'Aggregate curated customer activity into KPI summary by region and activity type.',
    'Y', 'admin', NOW()
);

-- ============================================================
-- STEP 3: Register Schedule (Daily at 2 AM UTC)
-- ============================================================

INSERT INTO ETL_JOB_SCHEDULE (
    JOB_ID, CRON_EXPRESSION, FREQUENCY,
    TIMEZONE, SCHEDULE_TYPE, IS_ACTIVE, CREATED_BY, CREATED_DATE
) VALUES (
    1,
    '0 2 * * *',
    'Daily',
    'UTC',
    'Scheduled',
    'Y', 'admin', NOW()
);

SELECT 'Seed data inserted successfully.' AS status;
