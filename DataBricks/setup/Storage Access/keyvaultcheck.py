# Databricks notebook source
dbutils.secrets.get('sp-adls','client-id')

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('sp')

# COMMAND ----------

dbutils.secrets.get('sp','client-id')
