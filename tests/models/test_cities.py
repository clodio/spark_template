from spark_cities.models.cities import Cities

def test_Cities(spark_session):
    # GIVEN
    CODE_POSTAL = "code_postal"
    CODE_INSEE = "code_commune_insee"
    NOM_COMMUNE = "nom_de_la_commune"
    LIGNE5 = "ligne_5 "
    LIBELLE_D_ACHEMINEMENT = "libelle_d_acheminement "
    COORDONNES_GPS = "coordonnees_gps"

    # WHEN
    city = Cities

    # THEN
    assert city.CODE_POSTAL == CODE_POSTAL
    assert city.CODE_INSEE == CODE_INSEE
    assert city.NOM_COMMUNE == NOM_COMMUNE
    assert city.LIGNE5 == LIGNE5
    assert city.LIBELLE_D_ACHEMINEMENT == LIBELLE_D_ACHEMINEMENT
    assert city.COORDONNES_GPS == COORDONNES_GPS