from pyspark.sql import functions as F

def step_01_read_csv_to_raw_parquet(spark, context, params):
    dfs = []
    for item in params["inputs"]:
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(item["path"])
            .withColumn("source_file", F.input_file_name())
            .withColumn("source_system", F.lit(item.get("source_system", "UNKNOWN")))
            .withColumn("ingestion_ts", F.current_timestamp())
        )
        dfs.append(df)

    if not dfs:
        raise ValueError("No CSV input files supplied.")

    consolidated_df = dfs[0]
    for df in dfs[1:]:
        consolidated_df = consolidated_df.unionByName(df, allowMissingColumns=True)

    consolidated_df.write.mode(params.get("write_mode", "overwrite")).parquet(params["output_path"])
    context["last_df"] = consolidated_df
    return context
