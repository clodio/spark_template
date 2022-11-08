import pyspark.sql.functions as F
from spark_cities.models.cities import Cities
from spark_cities.geospatial import split_lat_long


def main(spark):
  cities_df = spark.read.csv("hdfs:///data/raw/cities/v1/csv/laposte_hexasmal.csv", header=True, sep=";")

  cities_df.write.mode("overwrite").parquet('hdfs:///experiment/cities/v1/parquet')


