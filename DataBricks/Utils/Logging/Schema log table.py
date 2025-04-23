# Databricks notebook source
# MAGIC %sql
# MAGIC -- truncate table log_table

# COMMAND ----------

spark.sql("""
CREATE or replace TABLE  db_projects.dev.job_errors (
    logId STRING,
    jobId bigint,
    runId bigint,
    taskRunId bigint,
    error_message STRING,
    startTime TIMESTAMP,
    endTime TIMESTAMP
)
""")
