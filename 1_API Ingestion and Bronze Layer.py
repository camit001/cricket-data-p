# Databricks notebook source
# DBTITLE 1,Import the required library
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# DBTITLE 1,Creating catalog, schema & volume
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_project")

base_path = '/Volumes/workspace/default/cricket_api_project'

# COMMAND ----------

# DBTITLE 1,Calling cricket API data
API_KEY = 'ec0c4d5c-6f5e-4e87-98e0-02c57bf43e68'
api_url = f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEY}&offset=0"


response = requests.get(api_url)
response.raise_for_status()
print(f"Status code: {response.status_code}")

api_data = response.json()
print(api_data.keys())

print(json.dumps(api_data, indent=4 )[:2000])
# df = spark.createDataFrame(api_data['data'])
# df.write.mode('overwrite').save(f'{base_path}/countries')

# COMMAND ----------

# DBTITLE 1,Save raw data from API
raw_file_path = f'{base_path}/current_matches_raw.json'

with open(raw_file_path, 'w') as file:
    json.dump(api_data, file)

print("RAW API DATA IS CAVE AT : ", raw_file_path)

# df = spark.read.json(raw_file_path)
# df.write.mode('overwrite').save(f'{base_path}/current_matches')
# df

# COMMAND ----------

bronze_data = [{
    "source_api":api_url,
    "raw_json":json.dumps(api_data),
    "ingestion_time":None
}]



bronze_schema = StructType([
    StructField("source_api", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("ingestion_time", TimestampType(), True)
])


bronze_df = spark.createDataFrame(bronze_data, bronze_schema)\
        .withColumn("ingestion_time", current_timestamp())

display(bronze_df)

# COMMAND ----------

# DBTITLE 1,Save the bronze table
bronze_df.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("workspace.default.cricket_bronze_current_matches")


print("BRONZE table created successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.cricket_bronze_current_matches
