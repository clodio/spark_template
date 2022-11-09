from pyspark.sql import functions as F
from pyspark.sql.functions import udf


class Departments:
  
  DEPARTEMENT = "departement"
  NB_LOCALITY = "nb_localites"
  
  @staticmethod
  def read(spark):
    return spark.read.parquet("/refined/departement/v1/csv")

  @staticmethod
  def write(df):
    df.write.mode("overwrite").csv('/refined/departement/v1/csv')

  @staticmethod
  def sort_by_nb_locality(df):
    return df.orderBy(Departments.NB_LOCALITY, ascending=[0])

  @staticmethod
  def group_by_departments(df):
    return df.groupBy(Departments.DEPARTEMENT).agg(F.count("*").alias(Departments.NB_LOCALITY))
  
  @staticmethod
  def group_by_departments_with_corse_in_numeric(df):
    
    # @uf('string') remplace plus simplement get_department_with_corse_in_numericUDF = udf(lambda x:Departments.get_department_with_corse_in_numeric(x))
    @udf('string')
    def get_department_with_corse_in_numeric(department):
      return_departement = ""
      if department == "2A":
        return_departement= "20"
      elif department == "2B":
        return_departement= "20"
      else:
        return_departement = department
      return return_departement

    df_with_corse_numeric = df.withColumn(Departments.DEPARTEMENT, get_department_with_corse_in_numeric(df[Departments.DEPARTEMENT]))

    return df_with_corse_numeric.groupBy(Departments.DEPARTEMENT).agg(F.count("*").alias(Departments.NB_LOCALITY))

    