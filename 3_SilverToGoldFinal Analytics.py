# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.functions import *

# COMMAND ----------

silver_df = spark.table("workspace.default.cricket_silver_current_matches")
display(silver_df)

# COMMAND ----------

# DBTITLE 1,GOLD Analytics 1 :Match type Distribution
match_type_df = silver_df.groupBy("match_type").agg(count('*').alias('Total_Matches'))
display(match_type_df)


# COMMAND ----------

# DBTITLE 1,GOLD Analytics 2 : Venue-Wise Match Count
gold_venue_df=silver_df.groupBy('venue').agg(count('*').alias('Total_Matches'))
display(gold_venue_df)

# COMMAND ----------

# DBTITLE 1,GOLD Analytics 3 : Team Wise Match Count
team_1_df=silver_df.select(col("team_1").alias("team"))
team_2_df=silver_df.select(col("team_2").alias("team"))

all_teams_df=team_1_df.union(team_2_df)
gold_team_df=all_teams_df.groupBy('team')\
    .agg(count('*').alias("matches_played"))

display(gold_team_df)

# COMMAND ----------

# DBTITLE 1,Final Analytics Queries
display(spark.sql("""select count(*) as Total_matches,
count(distinct match_type) AS total_matche_types,
count(distinct venue) as Total_venues
FROM workspace.default.cricket_silver_current_matches"""))
