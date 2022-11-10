from tokenize import Double
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType,FloatType
import pyspark.sql.types as T
from spark_cities.models.cities import Cities
from spark_cities.models.departments import Departments
from pyspark.sql.functions import when
from pyspark.sql.window import Window
from pyspark.sql.functions import col
import math
import numpy as np

def split_lat_long(df, lat_long_col):

  return df.withColumn('latitude', F.split(df[lat_long_col], ',').getItem(0).cast(T.DoubleType())) \
        .withColumn('longitude', F.split(df[lat_long_col], ',').getItem(1).cast(T.DoubleType()) ) \
        .drop(lat_long_col)

def add_departement_column_from_postal_code(df):

  cities_clean_with_dept = df.withColumn("departement", \
        when ( ( (df[Cities.CODE_POSTAL].cast('int') >= 20000 ) & (df[Cities.CODE_POSTAL].cast('int') < 20200 )), '2A' ) \
        .when( ((df[Cities.CODE_POSTAL].cast('int') >= 20200) & (df[Cities.CODE_POSTAL].cast('int') < 30000 )),"2B") \
        .otherwise(F.substring(df[Cities.CODE_POSTAL], 1, 2)))

  cities_clean_with_dept_string = cities_clean_with_dept.withColumn('departement', F.col('departement').cast(T.StringType()))
  return cities_clean_with_dept_string

def add_departement_column_from_postal_codeUDF(df):

  @udf('string')
  def get_department_from_postal_codeMyUDF(postal_code_to_analyse):
    return_departement = ""
    postal_code = int(postal_code_to_analyse)
    if postal_code >= 20000 and postal_code < 20200 :
      return_departement= "2A"
    elif postal_code >= 20000 and postal_code < 30000 :
      return_departement= "2B"
    else:
      return_departement = postal_code_to_analyse[0:2]
    return return_departement

  cities_clean_with_dept = df.withColumn(Departments.DEPARTEMENT, get_department_from_postal_codeMyUDF(df[Cities.CODE_POSTAL]))

  cities_clean_with_dept_string = cities_clean_with_dept.withColumn('departement', F.col('departement').cast(T.StringType()))
  return cities_clean_with_dept_string

def add_prefecture_geoloc_and_distance(df):

  window = Window.partitionBy(Departments.DEPARTEMENT).orderBy(Cities.CODE_POSTAL)

  df_with_prefecture_geoloc  = df.withColumn('geoloc_prefecture', F.first(Cities.COORDONNES_GPS, True).over(window))

  df_with_distance_from_prefecture  = df_with_prefecture_geoloc.withColumn('geoloc_prefecture', F.first(Cities.COORDONNES_GPS, True).over(window))
  
  @udf('string')
  def distance(a,b):
    if a is None or b is None:
      # cas de mayotte
      a_lat = 0
      a_lon = 0
      b_lat = 0
      b_lon = 0
    else:
      a_lat = float(a.split(',')[0])
      a_lon = float(a.split(',')[1])
      b_lat = float(b.split(',')[0])
      b_lon = float(b.split(',')[1])
      # Todo : A ameliorer ce n'est pas un calcul de distance parfait du fait de la rotondite de la terre
    return math.sqrt(pow(abs(a_lat - b_lat),2)  + pow(abs(a_lon - b_lon),2))
  
  df_with_distance_from_prefecture=df_with_prefecture_geoloc.withColumn('distance', distance(df_with_prefecture_geoloc[Cities.COORDONNES_GPS],df_with_prefecture_geoloc["geoloc_prefecture"]))
  
  return df_with_distance_from_prefecture


def get_distance_stats_from_prefecture(df):

  # Impossible avec agg et les fonctions par defaut car percentile_approx EXISTE seulement en spark > 3.1
  # df_with_stats = df.groupBy(Departments.DEPARTEMENT) \
  #     .agg(F.percentile_approx("distance", 0.5, 10000).alias("dist_mean"), \
  #         F.avg("distance").alias("dist_avg") \
  #     )
  # F.percentile_approx("distance", 0.5, 10000).alias("dist_mean") --> EXISTE seulement en spark > 3.1
  # df_with_stats.show()

  # avec Window
  window = Window.partitionBy(Departments.DEPARTEMENT)
  df_with_avg = df.withColumn("dist_avg", F.avg("distance").over(window))

  # percentile_approx(distance, 0.5) --> calcule la mediane
  magic_percentile = F.expr('percentile_approx(distance, 0.5)').over(window)
  df_with_avg_and_mean = df_with_avg.withColumn("dist_mean", magic_percentile)

  # Menage
  df_cleaned = df_with_avg_and_mean.select('departement','dist_mean','dist_avg').drop_duplicates()
  df_cleaned.show()

  return df_cleaned
  # return df_with_stats
