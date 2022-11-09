import pyspark.sql.functions as F
import pyspark.sql.types as T
from spark_cities.models.cities import Cities
from spark_cities.geospatial import split_lat_long
from spark_cities.geospatial import add_departement_column_from_postal_code
from spark_cities.geospatial import add_departement_column_from_postal_codeUDF
from spark_cities.models.departments import Departments
from datetime import datetime

def main(spark):
  cities_df = Cities.read(spark)
  cities_clean = cities_df.select(Cities.CODE_POSTAL, Cities.CODE_INSEE, Cities.NOM_COMMUNE,Cities.COORDONNES_GPS)

  cities_clean_with_split_lat_long = split_lat_long(cities_clean,Cities.COORDONNES_GPS)

  # Methode 2 calcul deparement AVEC udf 
  start_datetime = datetime.now()
  cities_clean_with_dept_string = add_departement_column_from_postal_codeUDF(cities_clean_with_split_lat_long)
  end_datetime = datetime.now()
  delta = end_datetime - start_datetime
  print('Methode 2 calcul departement AVEC udf : ', delta)

  cities_clean_with_dept_string.printSchema()

  Cities.write(cities_clean_with_dept_string)

  # Methode 1 sans udf sur la corse
  start_datetime = datetime.now()
  departements_count = Departments.group_by_departments(cities_clean_with_dept_string)
  end_datetime = datetime.now()
  delta = end_datetime - start_datetime
  print('Methode 1 SANS udf sur la corse: ', delta)

  # departements_count.show()
  departements_sort = Departments.sort_by_nb_locality(departements_count)
  # departements_sort.show()

  # Methode 2 avec udf sur la corse
  start_datetime = datetime.now()
  departements_count_udf = Departments.group_by_departments_with_corse_in_numeric(cities_clean_with_dept_string)
  end_datetime = datetime.now()
  delta = end_datetime - start_datetime
  print('Methode 2 AVEC udf sur la corse: ', delta)

  Departments.write(departements_sort)


# * sauvegarder votre dataframe nettoyer sur HDFS dans le dossier 
