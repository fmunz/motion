# Databricks notebook source
# MAGIC %md
# MAGIC ![Name of the image](https://raw.githubusercontent.com/fmunz/motion/master/img/magn.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo_frank.motion;
# MAGIC DESCRIBE table sensor;

# COMMAND ----------

from pyspark.sql.functions import window

# set watermark for 1 minute for time column
# set window 1 second ?
# spark.sql.shuffle.partitions (default 200, match number of corese 4 or 8), maybe bigger instance?
spark.conf.set("spark.sql.shuffle.partitions", 16)


display(spark.readStream.format("delta").table("sensor").withWatermark("time", "3 seconds"). \
        groupBy(window("time", "1 second")).avg("magn").orderBy("window",ascending=False).limit(30))
