# DI Accelerator POC - GitLab/IntelliJ Ready Project

This repository is structured so you can:
- import it directly into IntelliJ as a Python project
- push it to GitLab as-is
- run the metadata-driven DI Accelerator POC locally
- extend it later for AWS EMR and S3

## Project Structure

```text
di_accelerator_gitlab_project/
├── README.md
├── requirements.txt
├── .gitignore
├── run_etl_job.py
├── config/
│   └── etl_job_config.py
├── src/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── logger.py
│   │   ├── spark_session.py
│   │   └── audit.py
│   └── steps/
│       ├── __init__.py
│       ├── step_01_read_csv_to_raw_parquet/
│       ├── step_02_read_parquet_to_df/
│       ├── step_03_df_to_temp_table/
│       ├── step_04_run_sql_on_temp_tables/
│       ├── step_05_sql_output_to_df/
│       └── step_06_write_df_to_parquet/
├── sample_data/
└── output/
```

## Local Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python run_etl_job.py --job-id CCF_JOB_001 --env local
```

## IntelliJ
1. Open IntelliJ IDEA
2. Open this folder as a project
3. Configure Python interpreter
4. Install requirements
5. Run `run_etl_job.py`

## GitLab
This package is ready for GitLab, but it is not pushed automatically. After downloading:
1. create a new repo in GitLab
2. extract this zip
3. `git init`
4. `git add .`
5. `git commit -m "Initial DI Accelerator POC"`
6. add remote and push
