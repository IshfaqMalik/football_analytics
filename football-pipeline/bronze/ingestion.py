import sys

import pyspark.sql.functions as F

pipeline_id = sys.argv[1]
run_id = sys.argv[2]
task_id = sys.argv[3]
processing_date = sys.argv[4]
catalog = sys.argv[5]


landing_path = f"/Volumes/{catalog}/landing/football_data/"


def ingest_data():
    tables = ["scorers", "standings", "matches", "teams"]
    for table in tables:
        df = spark.read.option("multiline", "true").format("json").load(landing_path + table + "/*/")
        df = df.withColumn(
            "metadata",
            F.create_map(
                F.lit("pipeline_id"),
                F.lit(pipeline_id),
                F.lit("run_id"),
                F.lit(run_id),
                F.lit("task_id"),
                F.lit(task_id),
                F.lit("processed_date"),
                F.lit(processing_date),
            ),
        )
        df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog}.bronze.bronze_{table}_tbl")


ingest_data()
