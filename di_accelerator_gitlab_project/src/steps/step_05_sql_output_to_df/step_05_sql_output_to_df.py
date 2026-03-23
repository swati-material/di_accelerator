def step_05_sql_output_to_df(spark, context, params):
    input_key = params.get("input_key", "sql_output_df")
    output_key = params.get("output_key", "transformed_df")
    context[output_key] = context[input_key]
    return context
