# Databricks notebook source
# MAGIC %run "/Workspace/DataBricks/setup/0.commonvar"

# COMMAND ----------

# MAGIC %run "/Workspace/DataBricks/Utils/Audit/Audit_to_Table_func_succes"

# COMMAND ----------

# MAGIC %run "/Workspace/DataBricks/Utils/Logging/2.Error Handling Code Table"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

watermark=spark.read.table("db_projects.dev.wateremark").filter("process_name='Ingest ratings'").select("watermark_time").collect()[0][0]

# COMMAND ----------

print(watermark)

# COMMAND ----------

def getSchema():
    schema="""userId integer,
             movieId integer,
            rating double,
            timestamp integer
            """
    return schema

def readFile(file_schema):
    from pyspark.sql.functions import input_file_name
    return (
            spark.read.format("csv")
            .option("header",True)
            .schema(file_schema)
            .option("modifiedAfter", watermark)
            .load(f"{base_data_dir}/ratings")
            .withColumnRenamed("userId","user_id")
            .withColumnRenamed("movieId","movie_id")
            .withColumn("IngestedDate",current_timestamp())
            .withColumn("InputFile", input_file_name())
        )
try:
    def process():
        print(f"\nStarting Silver Stream...", end="")
        file_schema=getSchema()
        RatingsDF = readFile(file_schema)
        RatingsDF.dropDuplicates().write.mode("append").saveAsTable("db_projects.dev.ratings_sv")
        spark.sql("""update db_projects.dev.wateremark
                    set watermark_time=current_timestamp()
                    where process_name='Ingest ratings'""")
        print("Done")
        audit(RatingsDF,'db_projects.dev.ratings_sv')
    process()
except Exception as e:
    error_message=f"Error:{e}"
    error_function(error_message)
