from pyspark.sql import functions as F

def split_lat_long(df, lat_long_col):
  return df.withColumn("latitude", F.col(lat_long_col) ) 
