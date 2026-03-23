def step_04_run_sql_on_temp_tables(spark, context, params):
    output_key = params.get("output_key", "sql_output_df")
    context[output_key] = spark.sql(params["sql"])
    return context
