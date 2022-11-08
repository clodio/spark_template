class Cities:
  
  CODE_POSTAL = "code_postal"
  CODE_INSEE = "code_commune_insee"
  NOM_COMMUNE = "nom_de_la_commune"
  LIGNE5 = "ligne_5 "
  LIBELLE_D_ACHEMINEMENT = "libelle_d_acheminement "
  COORDONNES_GPS = "coordonnees_gps"

  @staticmethod
  def read(spark):
    #return spark.read.csv("/data/raw/cities/v1/csv/laposte_hexasmal.csv", header=True, sep=";")
    return spark.read.parquet("/test/cities/v1/parquet")

  @staticmethod
  def write(df):
    df.write.mode("overwrite").parquet('/refined/cities/v1/parquet')
    