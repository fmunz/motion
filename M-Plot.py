# Databricks notebook source
# MAGIC %md
# MAGIC ![Name of the image](https://raw.githubusercontent.com/fmunz/motion/master/img/data_analytics.png)

# COMMAND ----------


!pip install plotly plotly_express
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # read the sensor table as a pandas df

# COMMAND ----------

import pandas as pd


#read UC table
df = spark.read.format("delta").table("demo_frank.motion.sensor")
pt = df.toPandas()

# make device ids smaller 
pt['device'] = pt['device'].str[-3:]

pt.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## stats
# MAGIC

# COMMAND ----------


#print(f"Size rebinned: {pt.shape} vs size before: {p.shape}")
print(f"first timestamp found:    {pt['time'].min()}") 
print(f"last timestamp found:     {pt['time'].max()}")
print(f"info():                   {pt.info()}")


# COMMAND ----------

# MAGIC %md
# MAGIC # resample the data to 10s windows

# COMMAND ----------

pt['device'].unique().size

# COMMAND ----------

# Resample the DataFrame for each 1-second interval, taking the maximum 'magn' value for each device
pt.set_index('time', inplace=True)  # Set 'time' as the index
resampled_pt = pt.groupby('device').resample('5S')['magn'].max().fillna(0)

#print(resampled_pt)

# COMMAND ----------

pt= resampled_pt

# COMMAND ----------

import plotly.express as px

fig = px.scatter_3d(pt.reset_index(), x='device', y='time', z='magn' )
fig.update_layout(autosize=True,width=1000,height=800)
fig.show()


# COMMAND ----------

'''
import pandas as pd
import numpy as np
import random
import string

# Function to generate a random string of fixed length
def get_random_string(length):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))

n = 10  # Number of times to replicate
new_pt_list = []

for device in pt['device'].unique():
    device_df = pt[pt['device'] == device]
    for _ in range(n):
        new_device_df = device_df.copy()
        new_device_df['device'] = device + get_random_string(5)
        new_pt_list.append(new_device_df)

new_pt = pd.concat(new_pt_list)

print(new_pt)
'''
