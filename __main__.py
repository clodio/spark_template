from pyspark.sql import SparkSession
from spark_cities.main import main

if __name__ == "__main__":
  spark = SparkSession.builder.appName("spark_cities").getOrCreate()
  main(spark)
