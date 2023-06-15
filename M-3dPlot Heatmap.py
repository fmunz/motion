# Databricks notebook source


# COMMAND ----------

#pip install plotly

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo_frank.motion
# MAGIC

# COMMAND ----------

import pandas as pd


df = spark.read.format("delta").table("sensor")
p = df.toPandas()
p.columns

# COMMAND ----------

p.columns

# COMMAND ----------

last_time = p['time'].max()
print(last_time)

# COMMAND ----------

pt = p
pt['time'] = pd.to_datetime(pt['time'])
pt.set_index('time', inplace=True)
pt_res = pt.groupby('device').resample('10S').agg({
    'device': 'first',
    'magn': 'sum'
})

# Reset the index of the DataFrame
pt_res.reset_index(drop=True, inplace=True) 

# COMMAND ----------

#pt=p

# COMMAND ----------

import plotly.express as px

# Convert 'device' to category codes
#pt['device_code'] = pt['device'].astype('category').cat.codes
pt['device_code'] = pt['device'].str[-3:]

# Convert 'time' to "HH:MM" format
pt['time_str'] = pt.index.strftime('%H:%M')

fig = px.scatter_3d(pt.reset_index(), x='device_code', y='time_str', z='magn')

fig.update_layout(
    autosize=True,
    width=1000,  # Adjust these values as needed
    height=800
)

fig.show()


# COMMAND ----------

pt=p

# COMMAND ----------

import plotly.graph_objects as go

# Reset the index of your DataFrame
pt_reset = pt.reset_index()

# Pivot the DataFrame
pt_pivot = pt_reset.pivot(index='device', columns='time', values='magn')

# Create the heatmap
fig = go.Figure(data=go.Heatmap(
    z=pt_pivot.values,
    x=pt_pivot.columns[-5:], 
    y=pt_pivot.index,
    colorscale='Hot'  # fire-like colors
))

fig.show()


# COMMAND ----------

pt.size
