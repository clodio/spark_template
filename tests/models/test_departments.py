from spark_cities.models.departments import Departments

def test_departments_group_by_departments(spark_session):
    # GIVEN
    cities = [('25000', '25'), ('2A000', '2A'), ('2A100', '2A'),('2B100', '2B')]
    input_df = spark_session.createDataFrame(cities, ['code_commune_insee', 'departement'])

    # WHEN
    actual_df = Departments.group_by_departments(input_df)
    # pour tester un dataframe il faut faire une conversion
    actual = list(map(lambda x: x.asDict(), actual_df.collect()))

    # THEN
    expected = [
        { "departement": "25", "nb_localites": 1},
        { "departement": "2A", "nb_localites": 2},
        { "departement": "2B", "nb_localites": 1}
        
    ]
    print (actual)
    assert expected == actual

def test_departments_sort_by_nb_locality(spark_session):
    # GIVEN
    cities = [('25000', '25'), ('25001', '25'),('2A000', '2A'),('2A001', '2A'), ('2A002', '2A'),('2B100', '2B')]
    input_df = spark_session.createDataFrame(cities, ['code_commune_insee', 'departement'])

    # WHEN
    actual_df = Departments.sort_by_nb_locality(Departments.group_by_departments(input_df))

    # pour tester un dataframe il faut faire une conversion
    actual = list(map(lambda x: x.asDict(), actual_df.collect()))

    # THEN
    expected = [
        { "departement": "2A", "nb_localites": 3},
        { "departement": "25", "nb_localites": 2},
        { "departement": "2B", "nb_localites": 1}
        
    ]
    assert expected == actual

def test_departments_department_with_corse_in_numeric(spark_session):
    # GIVEN
    departements = ['25','2A','2B']

    # WHEN
    actual = [Departments.get_department_with_corse_in_numeric(departements[0]),Departments.get_department_with_corse_in_numeric(departements[1]),Departments.get_department_with_corse_in_numeric(departements[2])]

    # THEN
    expected = ['25','20','20']
    assert expected == actual

def test_group_by_departments_with_corse_in_numeric(spark_session):
    # GIVEN
    cities = [('25000', '25'), ('2A000', '2A'), ('2A100', '2A'),('2B100', '2B')]
    input_df = spark_session.createDataFrame(cities, ['code_commune_insee', 'departement'])

    # WHEN
    actual_df = Departments.group_by_departments_with_corse_in_numeric(input_df)
    # pour tester un dataframe il faut faire une conversion
    actual = list(map(lambda x: x.asDict(), actual_df.collect()))

    # THEN
    expected = [
        { "departement": "25", "nb_localites": 1},
        { "departement": "20", "nb_localites": 3}
    ]
    print (actual)
    assert expected == actual
    

