import pyspark.sql.functions as F
import sys

catalog = sys.argv[1]


class GoldLayer:
    def __init__(self, spark, catalog=catalog):
        self.spark = spark
        self.catalog = catalog

    def create_gold_teams(self):
        df = self.spark.table(f"{self.catalog}.silver.silver_teams_tbl")

        competitions = [row.competition for row in df.select("competition").distinct().collect()]
        for comp in competitions:
            comp_df = df.filter(F.col("competition") == comp)
            table_name = comp.lower().replace(" ", "_")
            comp_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
                f"{self.catalog}.gold.gold_teams_{table_name}_tbl"
            )

    def create_gold_scorers(self):
        df = self.spark.table(f"{self.catalog}.silver.silver_scorers_tbl")

        df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.gold.gold_scorers_tbl")

    def create_gold_standings(self):
        df = self.spark.table(f"{self.catalog}.silver.silver_standings_tbl")
        df = df.select("competition_id", F.col("competition"), F.explode("standing.table").alias("table"))
        df = df.select(
            "competition_id",
            "competition",
            F.col("table.team.id").alias("team_id"),
            F.col("table.team.name").alias("team_name"),
            F.col("table.position").alias("position"),
            F.col("table.points").alias("points"),
            F.col("table.playedGames").alias("games"),
            F.col("table.won").alias("wins"),
            F.col("table.draw").alias("draws"),
            F.col("table.lost").alias("losses"),
            F.col("table.goalsFor").alias("goals_for"),
            F.col("table.goalsAgainst").alias("goals_against"),
            F.col("table.goalDifference").alias("goal_difference"),
        )
        competitions = [row.competition for row in df.select("competition").distinct().collect()]
        for comp in competitions:
            comp_df = df.filter(F.col("competition") == comp)
            table_name = comp.lower().replace(" ", "_")
            comp_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
                f"{self.catalog}.gold.gold_standings_{table_name}_tbl"
            )

    def gold_layer_run(self):
        self.create_gold_teams()
        self.create_gold_scorers()
        self.create_gold_standings()


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    catalog = sys.argv[1]
    gold_layer = GoldLayer(spark)
    gold_layer.gold_layer_run()
