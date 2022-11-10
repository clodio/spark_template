from spark_cities.geospatial import split_lat_long
from spark_cities.geospatial import add_departement_column_from_postal_code
from spark_cities.geospatial import add_departement_column_from_postal_codeUDF
from spark_cities.geospatial import add_prefecture_geoloc_and_distance
from spark_cities.geospatial import *

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
  print(actual)
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


def test_add_prefecture_geoloc_and_distance(spark_session):
  # GIVEN
  cities = [    ('10000', '10','prefecture 10000', '44.700000000,5.300000000'), \
                ('10001', '10','ville 1', '44.800000000,5.400000000'), \
                ('10002', '10','ville 2', '44.900000000,5.500000000'), \
                ('30000', '30','prefecture 30000', '45.700000000,6.300000000'), \
                ('30001', '30','ville 3', '45.800000000,6.400000000'), \
            ]
  input_df = spark_session.createDataFrame(cities, ['code_postal', 'departement', 'commune', 'coordonnees_gps'])

  # WHEN
  actual_df = add_prefecture_geoloc_and_distance(input_df)
  # pour tester un dataframe il faut faire une conversion
  actual = list(map(lambda x: x.asDict(), actual_df.collect()))

  # THEN
  expected = [
    {'code_postal':'10000', 'departement':'10', "commune": "prefecture 10000", "coordonnees_gps": "44.700000000,5.300000000", "geoloc_prefecture": "44.700000000,5.300000000", "distance": "0.0"},
    {'code_postal':'10001', 'departement':'10', "commune": "ville 1", "coordonnees_gps": "44.800000000,5.400000000", "geoloc_prefecture": "44.700000000,5.300000000", "distance": "0.14142135623730587"},
    {'code_postal':'10002', 'departement':'10', "commune": "ville 2", "coordonnees_gps": "44.900000000,5.500000000", "geoloc_prefecture": "44.700000000,5.300000000", "distance": "0.2828427124746161"},
    {'code_postal':'30000', 'departement':'30', "commune": "prefecture 30000", "coordonnees_gps": "45.700000000,6.300000000", "geoloc_prefecture": "45.700000000,6.300000000", "distance": "0.0"},
    {'code_postal':'30001', 'departement':'30', "commune": "ville 3", "coordonnees_gps": "45.800000000,6.400000000", "geoloc_prefecture": "45.700000000,6.300000000", "distance": "0.14142135623730587"}
  ]
  assert expected == actual
  

def test_get_distance_stats_from_prefecture(spark_session):
    # GIVEN
    cities = [      ('10000', '10','prefecture 10000', '44.700000000,5.300000000', '0.0'), \
                    ('10001', '10','ville 1', '44.800000000,5.400000000', '0.14142135623730587'), \
                    ('10002', '10','ville 2', '44.900000000,5.500000000', '0.2828427124746161'), \
                    # ('10003', '10','ville 3', '44.8200000000,5.4200000000', '0.1428427124746161'), \
                    ('30000', '30','prefecture 30000', '45.700000000,6.300000000', '0.0'), \
                    ('30001', '30','ville 3', '45.800000000,6.400000000', '0.14142135623730587'), \
                    ('30002', '30','ville 4', '45.810000000,6.410000000', '0.14242135623730587') \
                ]
    input_df = spark_session.createDataFrame(cities, ['code_postal', 'departement', 'commune', 'coordonnees_gps', 'distance'])

    # WHEN
    actual_df = get_distance_stats_from_prefecture(input_df)
    # pour tester un dataframe il faut faire une conversion
    actual = list(map(lambda x: x.asDict(), actual_df.collect()))

    # THEN
    expected = [
        {'departement':'10', "dist_mean": 0.14142135623730587, "dist_avg": 0.14142135623730734},
        {'departement':'30', "dist_mean": 0.14142135623730587, "dist_avg": 0.09461423749153725}
    ]
    assert expected == actual