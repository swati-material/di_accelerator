-- ============================================================
-- 1. config.job
--    Schedule fields (cron_expression, frequency, timezone,
--    schedule_type, next_run, last_run) are now columns here.
-- ============================================================
INSERT INTO config.job (
    client_id, job_name, job_description, source_system,
    cron_expression, frequency, timezone, schedule_type,
    is_active, created_by, created_date
)
VALUES (
    'CLIENT_001',
    'Customer Activity Consolidation',
    'Consolidates customer master from Provider A and B, joins with activity, produces SILVER and GOLD outputs.',
    'CSV_S3',
    '0 2 * * *', 'Daily', 'UTC', 'Scheduled',
    TRUE, 'admin', NOW()
);

-- ============================================================
-- 2. config.job_step
--    Removed : step_number, target_layer, transform_sql
--    Added   : step_action (stores SQL for TRANSFORM_SQL steps)
--    Changed : is_active -> BOOLEAN (TRUE / FALSE)
-- ============================================================

-- Step 1: Read Provider A CSV -> RAW parquet
INSERT INTO config.job_step (
    job_id, step_name, step_type, source_path, target_path, sequence, is_active, created_by, created_date
)
VALUES (
    1, 'Ingest Provider A master', 'READ_CSV',
    'data/landing/provider_a/customer_master_a.csv', 'data/raw/customer_master_a',
    1, TRUE, 'admin', NOW()
);

-- Step 2: Read Provider B CSV -> RAW parquet
INSERT INTO config.job_step (
    job_id, step_name, step_type, source_path, target_path, sequence, is_active, created_by, created_date
)
VALUES (
    1, 'Ingest Provider B master', 'READ_CSV',
    'data/landing/provider_b/customer_master_b.csv', 'data/raw/customer_master_b',
    2, TRUE, 'admin', NOW()
);

-- Step 3: Read Customer Activity CSV -> RAW parquet
INSERT INTO config.job_step (
    job_id, step_name, step_type, source_path, target_path, sequence, is_active, created_by, created_date
)
VALUES (
    1, 'Ingest customer activity', 'READ_CSV',
    'data/landing/customer_activity.csv', 'data/raw/customer_activity',
    3, TRUE, 'admin', NOW()
);

-- Step 4: Silver transform (SQL stored in step_action)
INSERT INTO config.job_step (
    job_id, step_name, step_type, target_path, sequence, is_active, created_by, created_date, step_action
)
VALUES (
    1, 'Silver curated activity', 'TRANSFORM_SQL',
    'data/silver/curated_customer_activity',
    4, TRUE, 'admin', NOW(),
    'SELECT
         COALESCE(a.customer_id,   b.customer_id)   AS customer_id,
         COALESCE(a.customer_name, b.customer_name) AS customer_name,
         a.email, a.region, b.phone, b.country,
         act.activity_id, act.activity_type, act.activity_date, act.amount,
         current_timestamp() AS transformed_at
     FROM customer_activity act
     LEFT JOIN customer_master_a a ON act.customer_id = a.customer_id
     LEFT JOIN customer_master_b b ON act.customer_id = b.customer_id'
);

-- Step 5: Gold KPI aggregation (SQL stored in step_action)
INSERT INTO config.job_step (
    job_id, step_name, step_type, target_path, sequence, is_active, created_by, created_date, step_action
)
VALUES (
    1, 'Gold KPI summary', 'TRANSFORM_SQL',
    'data/gold/activity_kpi_summary',
    5, TRUE, 'admin', NOW(),
    'SELECT
         region, activity_type,
         COUNT(*)    AS activity_count,
         SUM(amount) AS total_amount,
         current_date() AS report_date
     FROM curated_customer_activity
     GROUP BY region, activity_type'
);

-- ============================================================
-- 3. config.step_param
--    Added   : param_value, param_data_type
--    Changed : is_active -> BOOLEAN (TRUE / FALSE)
-- ============================================================

-- Step 1 params
INSERT INTO config.step_param (step_id, param_name, param_value, param_data_type, fields, delta_to_pull, resource_group, is_active, created_by, created_date)
VALUES (1, 'provider_a_fields', NULL, 'STRING', 'customer_id,customer_name,email,region,signup_date', 'full_refresh', 'customer_master_a', TRUE, 'admin', NOW());

-- Step 2 params
INSERT INTO config.step_param (step_id, param_name, param_value, param_data_type, fields, delta_to_pull, resource_group, is_active, created_by, created_date)
VALUES (2, 'provider_b_fields', NULL, 'STRING', 'customer_id,customer_name,phone,country,created_date', 'full_refresh', 'customer_master_b', TRUE, 'admin', NOW());

-- Step 3 params
INSERT INTO config.step_param (step_id, param_name, param_value, param_data_type, fields, delta_to_pull, resource_group, is_active, created_by, created_date)
VALUES (3, 'activity_fields', NULL, 'STRING', 'activity_id,customer_id,activity_type,activity_date,amount', 'Current_Date -1', 'customer_activity', TRUE, 'admin', NOW());

-- Step 4 params
INSERT INTO config.step_param (step_id, param_name, param_value, param_data_type, delta_to_pull, resource_group, is_active, created_by, created_date)
VALUES (4, 'silver_delta', NULL, 'STRING', 'Current_Date -1', 'curated_customer_activity', TRUE, 'admin', NOW());

-- Step 5 params
INSERT INTO config.step_param (step_id, param_name, param_value, param_data_type, delta_to_pull, resource_group, is_active, created_by, created_date)
VALUES (5, 'gold_delta', NULL, 'STRING', 'Current_Date -1', 'activity_kpi_summary', TRUE, 'admin', NOW());

SELECT 'Seed data inserted successfully.' AS status;
