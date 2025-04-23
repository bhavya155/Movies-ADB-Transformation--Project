# Databricks notebook source
# MAGIC %sql
# MAGIC select * from db_projects.dev.ratings_sv

# COMMAND ----------

# MAGIC %md
# MAGIC Ratings per Year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year(from_unixtime(1377476993)) AS rating_year, COUNT(*) AS total_ratings FROM db_projects.dev.ratings_sv GROUP BY rating ORDER BY rating;

# COMMAND ----------

# MAGIC %md
# MAGIC Rating Distribution
# MAGIC

# COMMAND ----------

df=spark.sql("SELECT rating, COUNT(*) AS count FROM db_projects.dev.ratings_sv GROUP BY rating ORDER BY rating")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 18 Movies Tagged but Not Rated
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_projects.dev.tags_sv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT t.movie_id,t.tag FROM db_projects.dev.tags_sv t LEFT JOIN db_projects.dev.ratings_sv r ON t.movie_id = r.movie_id WHERE r.movie_id IS NULL LIMIT 18;

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 Untagged Rated Movies (30+ Ratings)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT movie_id, AVG(rating) AS avg_rating, COUNT(*) AS num_ratings FROM db_projects.dev.ratings_sv WHERE movie_id NOT IN (SELECT DISTINCT movie_id FROM db_projects.dev.tags_sv) GROUP BY movie_id HAVING COUNT(*) > 30 ORDER BY avg_rating DESC, num_ratings DESC LIMIT 10;