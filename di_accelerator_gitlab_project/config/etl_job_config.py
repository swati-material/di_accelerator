import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE_DATA_DIR = os.path.join(BASE_DIR, "sample_data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

LOCAL_JOB_CONFIGS = {
    "CCF_JOB_001": {
        "description": "CCF POC - multi-source customer activity consolidation",
        "steps": [
            {
                "name": "step_01_read_csv_to_raw_parquet",
                "params": {
                    "inputs": [
                        {"path": os.path.join(SAMPLE_DATA_DIR, "ccf_customer_master_a.csv"), "source_system": "SRC_A"},
                        {"path": os.path.join(SAMPLE_DATA_DIR, "ccf_customer_master_b.csv"), "source_system": "SRC_B"}
                    ],
                    "output_path": os.path.join(OUTPUT_DIR, "raw", "customer_master"),
                    "write_mode": "overwrite"
                }
            },
            {
                "name": "step_01_read_csv_to_raw_parquet",
                "params": {
                    "inputs": [
                        {"path": os.path.join(SAMPLE_DATA_DIR, "ccf_case_activity.csv"), "source_system": "CCF_ACTIVITY"}
                    ],
                    "output_path": os.path.join(OUTPUT_DIR, "raw", "case_activity"),
                    "write_mode": "overwrite"
                }
            },
            {
                "name": "step_01_read_csv_to_raw_parquet",
                "params": {
                    "inputs": [
                        {"path": os.path.join(SAMPLE_DATA_DIR, "ccf_reference_mapping.csv"), "source_system": "CCF_REF"}
                    ],
                    "output_path": os.path.join(OUTPUT_DIR, "raw", "reference_mapping"),
                    "write_mode": "overwrite"
                }
            },
            {
                "name": "step_02_read_parquet_to_df",
                "params": {
                    "path": os.path.join(OUTPUT_DIR, "raw", "customer_master"),
                    "df_key": "customer_master_df"
                }
            },
            {
                "name": "step_02_read_parquet_to_df",
                "params": {
                    "path": os.path.join(OUTPUT_DIR, "raw", "case_activity"),
                    "df_key": "case_activity_df"
                }
            },
            {
                "name": "step_02_read_parquet_to_df",
                "params": {
                    "path": os.path.join(OUTPUT_DIR, "raw", "reference_mapping"),
                    "df_key": "reference_mapping_df"
                }
            },
            {
                "name": "step_03_df_to_temp_table",
                "params": {
                    "df_key": "customer_master_df",
                    "temp_view": "vw_customer_master"
                }
            },
            {
                "name": "step_03_df_to_temp_table",
                "params": {
                    "df_key": "case_activity_df",
                    "temp_view": "vw_case_activity"
                }
            },
            {
                "name": "step_03_df_to_temp_table",
                "params": {
                    "df_key": "reference_mapping_df",
                    "temp_view": "vw_reference_mapping"
                }
            },
            {
                "name": "step_04_run_sql_on_temp_tables",
                "params": {
                    "output_key": "silver_df",
                    "sql": '''
                        SELECT
                            a.activity_id,
                            a.customer_id,
                            c.customer_name,
                            c.segment,
                            c.city,
                            c.state,
                            a.activity_type,
                            a.activity_date,
                            COALESCE(r.standard_status, a.status) AS standard_status,
                            a.amount
                        FROM vw_case_activity a
                        LEFT JOIN vw_customer_master c
                            ON a.customer_id = c.customer_id
                        LEFT JOIN vw_reference_mapping r
                            ON a.status = r.source_status
                    '''
                }
            },
            {
                "name": "step_05_sql_output_to_df",
                "params": {
                    "input_key": "silver_df",
                    "output_key": "silver_df"
                }
            },
            {
                "name": "step_06_write_df_to_parquet",
                "params": {
                    "df_key": "silver_df",
                    "output_path": os.path.join(OUTPUT_DIR, "silver", "ccf_case_customer_curated"),
                    "write_mode": "overwrite"
                }
            },
            {
                "name": "step_03_df_to_temp_table",
                "params": {
                    "df_key": "silver_df",
                    "temp_view": "vw_ccf_curated"
                }
            },
            {
                "name": "step_04_run_sql_on_temp_tables",
                "params": {
                    "output_key": "gold_df",
                    "sql": '''
                        SELECT
                            standard_status,
                            COUNT(*) AS activity_count,
                            SUM(amount) AS total_amount
                        FROM vw_ccf_curated
                        GROUP BY standard_status
                    '''
                }
            },
            {
                "name": "step_05_sql_output_to_df",
                "params": {
                    "input_key": "gold_df",
                    "output_key": "gold_df"
                }
            },
            {
                "name": "step_06_write_df_to_parquet",
                "params": {
                    "df_key": "gold_df",
                    "output_path": os.path.join(OUTPUT_DIR, "gold", "ccf_status_kpi"),
                    "write_mode": "overwrite"
                }
            }
        ]
    }
}


def get_job_config(job_id: str, env: str = "local"):
    if env != "local":
        raise ValueError("This sample is configured for local mode. Replace paths with S3 paths for EMR.")
    if job_id not in LOCAL_JOB_CONFIGS:
        raise ValueError(f"Unknown job id: {job_id}")
    return LOCAL_JOB_CONFIGS[job_id]
