from tokenize import Double
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType
import pyspark.sql.types as T
from spark_cities.models.cities import Cities
from spark_cities.models.departments import Departments
from pyspark.sql.functions import when
from pyspark.sql.window import Window
from pyspark.sql.functions import col
import math

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
  df_with_prefecture_geoloc.show()

  df_with_distance_from_prefecture  = df_with_prefecture_geoloc.withColumn('geoloc_prefecture', F.first(Cities.COORDONNES_GPS, True).over(window))
  
  @udf('string')
  def distance(a,b):
    a_lat = float(a.split(',')[0])
    a_lon = float(a.split(',')[1])
    b_lat = float(b.split(',')[0])
    b_lon = float(b.split(',')[1])
    # Todo : ce n'est pas un calcul de distance parfait du fait de la rotondite de la terre
    return math.sqrt(pow(abs(a_lat - b_lat),2)  + pow(abs(a_lon - b_lon),2))

  df_with_distance_from_prefecture=df_with_prefecture_geoloc.withColumn('distance', distance(df_with_prefecture_geoloc["coordonnees_gps"],df_with_prefecture_geoloc["geoloc_prefecture"]))
  df_with_distance_from_prefecture.show()
  
  return df_with_distance_from_prefecture


def get_distance_stats_from_prefecture(df):

  window = Window.partitionBy(Departments.DEPARTEMENT)

  dist_avg = F.avg("distance").over(window)
  dist_mean = F.mean("distance").over(window)
  df_with_avg = df.withColumn("dist_avg", dist_avg)
  df_with_avg_and_mean = df_with_avg.withColumn("dist_mean", dist_mean)
  df_with_avg_and_mean.show()
  
  df_with_stats = df.groupBy(Departments.DEPARTEMENT) \
      .agg(F.mean("distance").alias("dist_mean"), \
          F.avg("distance").alias("dist_avg") \
      )
  return df_with_stats

  # return df_with_avg_and_mean

# group_by departement  partitionBy 
# sort orderBy 
# analytic fisrt value 

# * À l'aide de window function à chaque ville ajouter les coordonnées GPS de la préfecture du département.
# * On la préfecture du département se situe dans la ville ayant le code postal le plus petit dans tout le département. Pour l’exercice on considère également que la Corse est un seul département (on ne sépare pas la haute corse et la corse du sud).
# * Une fois la préfecture trouvée, calculer la distance relative de chaque ville par rapport à la préfecture. On ne cherche pas une distance en km.
# * calculer la distance moyenne et médiane à la préfecture par département sauvegarder le résultat sur HDFS en csv dans le dossier /refined/departement/v3/csv

