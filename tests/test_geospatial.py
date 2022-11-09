from spark_cities.geospatial import split_lat_long
from spark_cities.geospatial import add_departement_column_from_postal_code
from spark_cities.geospatial import add_departement_column_from_postal_codeUDF

def test_split_lat_long(spark_session):
  # GIVEN
  cities = [('felines', '44.726373096,5.373096726'), ('ambares', '-45.111111111,-4.3222222222')]
  input_df = spark_session.createDataFrame(cities, ['commune', 'coordonnees_gps'])

  # WHEN
  actual_df = split_lat_long(input_df, "coordonnees_gps")
  # pour tester un dataframe il faut faire une conversion
  actual = list(map(lambda x: x.asDict(), actual_df.collect()))

  # THEN
  expected = [
    {"commune": "felines", "latitude": 44.726373096, "longitude": 5.373096726 },
    {"commune": "ambares", "latitude": -45.111111111, "longitude": -4.3222222222}
  ]
  assert expected == actual


def test_add_departement_column_from_postal_code(spark_session):
  # GIVEN
  cities = [('30000', '30620'), ('20000', '2A000'), ('25000', '2B000')]
  input_df = spark_session.createDataFrame(cities, ['code_postal', 'code_commune_insee'])

  # WHEN
  actual_df = add_departement_column_from_postal_code(input_df)
  # pour tester un dataframe il faut faire une conversion
  actual = list(map(lambda x: x.asDict(), actual_df.collect()))

  # THEN
  expected = [
    {'code_postal':'30000', 'code_commune_insee':'30620', "departement": "30"},
    {'code_postal':'20000', 'code_commune_insee':'2A000', "departement": "2A"},
    {'code_postal':'25000', 'code_commune_insee':'2B000', "departement": "2B"}
  ]
  assert expected == actual
  

def test_add_departement_column_from_postal_codeUDF(spark_session):
  # GIVEN
  cities = [('30000', '30620'), ('20000', '2A000'), ('25000', '2B000')]
  input_df = spark_session.createDataFrame(cities, ['code_postal', 'code_commune_insee'])

  # WHEN
  actual_df = add_departement_column_from_postal_codeUDF(input_df)
  # pour tester un dataframe il faut faire une conversion
  actual = list(map(lambda x: x.asDict(), actual_df.collect()))

  # THEN
  expected = [
    {'code_postal':'30000', 'code_commune_insee':'30620', "departement": "30"},
    {'code_postal':'20000', 'code_commune_insee':'2A000', "departement": "2A"},
    {'code_postal':'25000', 'code_commune_insee':'2B000', "departement": "2B"}
  ]
  assert expected == actual