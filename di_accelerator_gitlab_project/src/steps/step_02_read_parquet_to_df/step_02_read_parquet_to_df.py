def step_02_read_parquet_to_df(spark, context, params):
    context[params["df_key"]] = spark.read.parquet(params["path"])
    return context
