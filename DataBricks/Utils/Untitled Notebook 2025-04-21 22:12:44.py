# Databricks notebook source
# MAGIC %sql
# MAGIC select * from db_projects.dev.audit_table order by startTime

# COMMAND ----------

df=spark.read.format("csv").load("/mnt/devadlsprojects/movies/ratings")

# COMMAND ----------

df.show()
