from pyspark.sql import functions as F
import pyspark.sql.types as T
from spark_cities.models.cities import Cities
from spark_cities.models.departments import Departments
from pyspark.sql.functions import when
from pyspark.sql.functions import expr
from pyspark.sql.functions import udf

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
  def get_department_from_postal_codeMyUDF(postal_code):
    return_departement = ""
    if postal_code >= 20000 & postal_code < 20200 :
      return_departement= "2A"
    elif postal_code >= 20000 & postal_code < 20200 :
      return_departement= "2B"
    else:
      return_departement = postal_code[0:2]
    return return_departement

  cities_clean_with_dept = df.withColumn(Departments.DEPARTEMENT, get_department_from_postal_codeMyUDF(df[Cities.CODE_POSTAL]))

  cities_clean_with_dept_string = cities_clean_with_dept.withColumn('departement', F.col('departement').cast(T.StringType()))
  return cities_clean_with_dept_string
