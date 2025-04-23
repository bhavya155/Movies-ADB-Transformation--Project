# Databricks notebook source
# MAGIC %sql
# MAGIC create database db_projects.dev;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table db_projects.dev.wateremark
# MAGIC (process_name string, watermark_time timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC update db_projects.dev.wateremark
# MAGIC set watermark_time='1999-12-31T00:00:00.313+00:00'
# MAGIC where process_name='Ingest ratings'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into db_projects.dev.wateremark
# MAGIC values('Ingest movies','1999-12-31T00:00:00.313+00:00'),
# MAGIC ('Ingest ratings','1999-12-31T00:00:00.313+00:00'),
# MAGIC ('Ingest tags','1999-12-31T00:00:00.313+00:00');
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_projects.dev.wateremark
