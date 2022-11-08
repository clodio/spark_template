import pyspark.sql.functions as F
import pyspark.sql.types as T
from spark_cities.models.cities import Cities
from spark_cities.geospatial import split_lat_long
from spark_cities.geospatial import add_departement_column_from_postal_code

def main(spark):
  cities_df = Cities.read(spark)
  cities_clean = cities_df.select(Cities.CODE_POSTAL, Cities.CODE_INSEE, Cities.NOM_COMMUNE,Cities.COORDONNES_GPS)

  cities_clean_with_split_lat_long = split_lat_long(cities_clean,Cities.COORDONNES_GPS)
  cities_clean_with_dept_string = add_departement_column_from_postal_code(cities_clean_with_split_lat_long)

  cities_clean_with_dept_string.printSchema()

  Cities.write(cities_clean_with_dept_string)

# * sauvegarder votre dataframe nettoyer sur HDFS dans le dossier 
