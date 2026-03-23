def step_06_write_df_to_parquet(spark, context, params):
    df = context[params["df_key"]]
    df.write.mode(params.get("write_mode", "overwrite")).parquet(params["output_path"])
    return context
