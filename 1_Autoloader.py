# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.projectnetflixdatalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.projectnetflixdatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.projectnetflixdatalake.dfs.core.windows.net", "dbf9ea6b-a9df-4c9c-9ea8-8d9fb3f54736")
spark.conf.set("fs.azure.account.oauth2.client.secret.projectnetflixdatalake.dfs.core.windows.net", "Nlb8Q~.eRkNsIihJ1ZH2wEPtQOCiGGw5sfsgWbZb")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.projectnetflixdatalake.dfs.core.windows.net", "https://login.microsoftonline.com/961858b2-e943-4fd8-ada5-279d35aa236c/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Data Loading using AutoLoader

# COMMAND ----------

checkpoint_location = "abfss://silver@projectnetflixdatalake.dfs.core.windows.net/checkpoint"

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
                     .option("cloudFiles.format","csv")\
                     .option("cloudFiles.schemalocation", checkpoint_location)\
                     .load("abfss://raw@projectnetflixdatalake.dfs.core.windows.net")

# COMMAND ----------

display(df)

# COMMAND ----------

df.writeStream.option("checkpointLocation", checkpoint_location)\
              .trigger(processingTime="10 seconds")\
              .start("abfss://bronze@projectnetflixdatalake.dfs.core.windows.net/netflix_titles")