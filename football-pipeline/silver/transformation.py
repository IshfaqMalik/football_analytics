import pyspark.sql.functions as F
import sys

catalog = sys.argv[1]


class Transformation:
    def __init__(self, spark, catalog=catalog):
        self.spark = spark
        self.catalog = catalog

    def transform_matches(self):
        df = self.spark.table(f"{self.catalog}.bronze.bronze_matches_tbl")
        df = df.select(
            F.col("competition.id").alias("competition_id"),
            F.col("competition.name").alias("competition"),
            F.explode("matches").alias("match"),
        )
        df = df.select(
            "competition_id",
            "competition",
            F.col("match.id").alias("match_id"),
            F.col("match.homeTeam.id").alias("home_team_id"),
            F.col("match.homeTeam.name").alias("home_team"),
            F.col("match.awayTeam.id").alias("away_team_id"),
            F.col("match.awayTeam.name").alias("away_team"),
            F.col("match.score.duration").alias("duration"),
            F.col("match.score.extraTime").alias("extra_time"),
            F.col("match.score.fullTime.home").alias("home_team_score"),
            F.col("match.score.fullTime.away").alias("away_team_score"),
            F.col("match.score.winner").alias("winner"),
        )
        df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
            f"{self.catalog}.silver.silver_matches_tbl"
        )

    def transform_scorers(self):
        df = self.spark.table(f"{self.catalog}.bronze.bronze_scorers_tbl")
        df = df.select(
            F.col("competition.id").alias("competition_id"),
            F.col("competition.name").alias("competition"),
            F.explode("scorers").alias("scorer"),
        )
        df = df.select(
            "competition_id",
            "competition",
            F.col("scorer.player.name").alias("player_name"),
            F.col("scorer.player.id").alias("player_id"),
            F.col("scorer.team.name").alias("team_name"),
            F.col("scorer.team.id").alias("team_id"),
            F.col("scorer.player.nationality").alias("nationality"),
            F.col("scorer.player.position").alias("position"),
            F.col("scorer.player.dateOfBirth").alias("date_of_birth"),
            F.col("scorer.assists").alias("assists"),
            F.col("scorer.goals").alias("goals"),
            F.col("scorer.penalties").alias("penalties"),
            F.col("scorer.playedMatches").alias("played_matches"),
        )
        df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
            f"{self.catalog}.silver.silver_scorers_tbl"
        )

    def transform_standings(self):
        df = self.spark.table(f"{self.catalog}.bronze.bronze_standings_tbl")
        df = df.select(
            F.col("competition.id").alias("competition_id"),
            F.col("competition.name").alias("competition"),
            F.explode("standings").alias("standing"),
        )
        df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
            f"{self.catalog}.silver.silver_standings_tbl"
        )

    def transform_teams(self):
        df = self.spark.table(f"{self.catalog}.bronze.bronze_teams_tbl")
        df = df.select(
            F.col("competition.id").alias("competition_id"),
            F.col("competition.name").alias("competition"),
            F.explode("teams").alias("team"),
        )
        df = df.select(
            "competition_id",
            F.col("competition"),
            F.col("team.id").alias("team_id"),
            F.col("team.name").alias("team_name"),
            F.col("team.coach.name").alias("coach"),
            F.col("team.coach.contract.start").alias("contract_start"),
            F.col("team.coach.contract.until").alias("contract_end"),
            F.col("team.lastUpdated").alias("last_updated"),
            F.col("team.squad").alias("squad"),
        )

        df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{self.catalog}.silver.silver_teams_tbl")

    def run_transformations(self):
        self.transform_matches()
        self.transform_scorers()
        self.transform_standings()
        self.transform_teams()


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    catalog = sys.argv[1]

    transformer = Transformation(spark, catalog)
    transformer.run_transformations()
