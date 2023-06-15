-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![Name of the image](https://raw.githubusercontent.com/fmunz/motion/master/img/data-histo.png)

-- COMMAND ----------

USE demo_frank.motion;
DESCRIBE sensor;

-- COMMAND ----------

SELECT device, MAX(magn) AS max_magn
FROM sensor
WHERE time >= (CURRENT_TIMESTAMP() - INTERVAL 10 MINUTES)
GROUP BY device;


-- COMMAND ----------


