# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

silver_df = spark.table('workspace.default.cricket_silver_current_matches')
display(silver_df)

# COMMAND ----------

gold_match_type_df = silver_df.groupBy('match_type').agg(count('*').alias('Match_count'))
display(gold_match_type_df)

# COMMAND ----------

gold_venue = silver_df.groupBy('venue').agg(count('*').alias('Total_matches'))
display(gold_venue)

# COMMAND ----------

#Match Overview

display(spark.sql("""
                SELECT count(*) as Total_matches,
                    count(DISTINCT match_type) AS total_matche_types,
                    count(DISTINCT venue) AS Total_venues
                FROM workspace.default.cricket_silver_current_matches
"""))
