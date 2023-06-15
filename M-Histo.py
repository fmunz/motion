# Databricks notebook source
# MAGIC %md
# MAGIC ![Name of the image](https://raw.githubusercontent.com/fmunz/motion/master/img/data-histo.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo_frank.motion;
# MAGIC DESCRIBE sensor;

# COMMAND ----------




# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from pyspark.sql.functions import col, expr, max
# MAGIC from pyspark.sql.window import Window
# MAGIC # Read the sensor stream as a DataFrame
# MAGIC sensor_df = spark.readStream.format("delta").table("demo_frank.motion.sensor")
# MAGIC
# MAGIC # Define the window duration
# MAGIC window_duration = "10 minutes"
# MAGIC
# MAGIC # Apply the filter and aggregation
# MAGIC df = sensor_df \
# MAGIC     .filter(col("time") >= (col("current_timestamp") - expr(f"INTERVAL {window_duration}"))) \
# MAGIC     .groupBy("device") \
# MAGIC     .agg(max("magn").alias("max_magn"))
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC display(df)

# COMMAND ----------


