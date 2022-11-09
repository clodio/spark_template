#!/bin/bash

#spark-submit --master local ./__main__.py
# spark-submit --master local --deploy-mode cluster --py-files /home/simplon/spark_simplon/dist/spark_cities-0.1-py3.7.egg __main__.py

# Création du egg pour pouvoir ensuite lancer spark-submit
./create_egg.sh

# lanceement du projet avec spark-submit
spark-submit --master local --py-files ./dist/SparkCities-0.1-py3.7.egg __main__.py
