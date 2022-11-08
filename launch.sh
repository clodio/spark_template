#!/bin/bash

#spark-submit --master local ./__main__.py
# spark-submit --master local --deploy-mode cluster --py-files /home/simplon/spark_simplon/dist/spark_cities-0.1-py3.7.egg __main__.py

./compile.sh
spark-submit --master local --py-files ./dist/SparkCities-0.1-py3.7.egg __main__.py
