import pyspark.sql.functions as F
import pyspark.sql.types as T
from spark_cities.models.cities import Cities
from spark_cities.geospatial import split_lat_long
from spark_cities.geospatial import add_departement_column_from_postal_code
from spark_cities.geospatial import add_departement_column_from_postal_codeUDF
import spark_cities.geospatial as geospatial
from spark_cities.models.departments import Departments
from datetime import datetime


def main(spark):
  cities_df = Cities.read(spark)
  cities_clean = cities_df.select(Cities.CODE_POSTAL, Cities.CODE_INSEE, Cities.NOM_COMMUNE,Cities.COORDONNES_GPS)

  cities_clean_with_split_lat_long = split_lat_long(cities_clean,Cities.COORDONNES_GPS)

  cities_clean_with_split_lat_longforUDF = cities_clean_with_split_lat_long

  # Methode 1 calcul deparement SANS udf 
  start_datetime = datetime.now()
  cities_clean_with_dept_string = add_departement_column_from_postal_code(cities_clean_with_split_lat_long)
  end_datetime = datetime.now()
  delta = end_datetime - start_datetime
  print('Methode 1 calcul departement SANS udf : ', delta)

  # Methode 2 calcul deparement AVEC udf 
  start_datetime = datetime.now()
  cities_clean_with_dept_string2 = add_departement_column_from_postal_codeUDF(cities_clean_with_split_lat_longforUDF)
  end_datetime = datetime.now()
  delta = end_datetime - start_datetime
  print('Methode 2 calcul departement AVEC udf : ', delta)

  cities_clean_with_dept_string.printSchema()

  Cities.write(cities_clean_with_dept_string)
  Cities.write(cities_clean_with_dept_string2)

  departements_count = Departments.group_by_departments(cities_clean_with_dept_string)
  # departements_count.show()

  departements_sort = Departments.sort_by_nb_locality(departements_count)
  # departements_sort.show()

  Departments.write(departements_sort)

  ## calculs stats
  # /!\ les calculs sont complement erronees sur les dom/tom/monaco du fait de codes postaux particuliers
  cities_to_stat = add_departement_column_from_postal_code(cities_clean)
  cities_with_prefecture_geoloc_and_distance = geospatial.add_prefecture_geoloc_and_distance(cities_to_stat)
  departement_distance_to_prefecture_stats = geospatial.get_distance_stats_from_prefecture(cities_with_prefecture_geoloc_and_distance)
  Departments.write_stats(departement_distance_to_prefecture_stats)
