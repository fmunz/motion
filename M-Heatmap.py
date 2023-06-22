# Databricks notebook source
# MAGIC %md
# MAGIC ![Name of the image](https://raw.githubusercontent.com/fmunz/motion/master/img/data_analytics.png)

# COMMAND ----------


!pip install plotly plotly_express
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### read UC streaming table 

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
# MAGIC
# MAGIC ### sizes and stats

# COMMAND ----------


#print(f"Size rebinned: {pt.shape} vs size before: {p.shape}")
print(f"first timestamp found:    {pt['time'].min()}") 
print(f"last timestamp found:     {pt['time'].max()}")
print(f"info():                   {pt.info()}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### resample to x seconds timed windows
# MAGIC

# COMMAND ----------

# generate more test data...


import pandas as pd
import numpy as np
import random
import string

# Function to generate a random string of fixed length
def get_random_string(length):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))

n = 50  # Number of times to replicate
new_pt_list = []

for device in pt['device'].unique():
    device_df = pt[pt['device'] == device]
    for _ in range(n):
        new_device_df = device_df.copy()
        new_device_df['device'] = device + get_random_string(5)
        new_pt_list.append(new_device_df)

new_pt = pd.concat(new_pt_list)

print(new_pt)


# COMMAND ----------

pt=new_pt

# COMMAND ----------

pt['device'].unique().size

# COMMAND ----------

# MAGIC %md 
# MAGIC ### downsample the time axis to 10 seconds for all data

# COMMAND ----------

pt.set_index('time', inplace=True)  # Set 'time' as the index

# Resample the DataFrame for each 1-second interval, taking the maximum 'magn' value for each device
resampled_pt = pt.groupby('device').resample('10S')['magn'].max().fillna(0)

print(resampled_pt)

# COMMAND ----------

pt= resampled_pt

# COMMAND ----------



# COMMAND ----------

print(len(pt_pivot.index))

# COMMAND ----------

import plotly.graph_objects as go

# Reset the index of your DataFrame
pt_reset = pt.reset_index()

# Pivot the DataFrame
pt_pivot = pt_reset.pivot(index='device', columns='time', values='magn')

# Create the heatmap
# x=pt_pivot.columns[-5:], 

fig = go.Figure(data=go.Heatmap( 
    x=pt_pivot.columns[-10:] ,
    y=pt_pivot.index,
    z=pt_pivot.values,
    colorscale='Magma'  # fire-like colors
    
))

# warning layout limits the number of devices shown
fig.update_layout(autosize=True)
#fig.update_layout(width=1000, height=800) 
fig.show()


# COMMAND ----------


