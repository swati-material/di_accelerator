from src.steps.step_01_read_csv_to_raw_parquet.step_01_read_csv_to_raw_parquet import step_01_read_csv_to_raw_parquet
from src.steps.step_02_read_parquet_to_df.step_02_read_parquet_to_df import step_02_read_parquet_to_df
from src.steps.step_03_df_to_temp_table.step_03_df_to_temp_table import step_03_df_to_temp_table
from src.steps.step_04_run_sql_on_temp_tables.step_04_run_sql_on_temp_tables import step_04_run_sql_on_temp_tables
from src.steps.step_05_sql_output_to_df.step_05_sql_output_to_df import step_05_sql_output_to_df
from src.steps.step_06_write_df_to_parquet.step_06_write_df_to_parquet import step_06_write_df_to_parquet

STEP_FUNCTION_REGISTRY = {
    "step_01_read_csv_to_raw_parquet": step_01_read_csv_to_raw_parquet,
    "step_02_read_parquet_to_df": step_02_read_parquet_to_df,
    "step_03_df_to_temp_table": step_03_df_to_temp_table,
    "step_04_run_sql_on_temp_tables": step_04_run_sql_on_temp_tables,
    "step_05_sql_output_to_df": step_05_sql_output_to_df,
    "step_06_write_df_to_parquet": step_06_write_df_to_parquet,
}
