def step_03_df_to_temp_table(spark, context, params):
    df = context[params["df_key"]]
    df.createOrReplaceTempView(params["temp_view"])
    return context
