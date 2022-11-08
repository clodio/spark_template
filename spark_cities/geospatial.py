from pyspark.sql import functions as F
import pyspark.sql.types as T
from spark_cities.models.cities import Cities

def split_lat_long(df, lat_long_col):

  return df.withColumn('latitude', F.split(df[lat_long_col], ',').getItem(0).cast(T.DoubleType())) \
        .withColumn('longitude', F.split(df[lat_long_col], ',').getItem(1).cast(T.DoubleType()) ) \
        .drop(lat_long_col)

def add_departement_column_from_postal_code(df):
  cities_clean_with_dept = df.withColumn('departement', F.substring(Cities.CODE_POSTAL, 1, 2))
  cities_clean_with_dept_string = cities_clean_with_dept.withColumn('departement', F.col('departement').cast(T.StringType()))
  return cities_clean_with_dept_string