# spark-simplon

## Contexte du projet

Après votre première mise a disposition du fichier des communes française les équipes métiers reviennent vers vous car elle souhaite pourvoir exploiter la colonne de coordonnée GPS. Aussi elle aimerait savoir la distance à vol d'oiseau de chaque ville par rapport à la prefecture

## Modalités pédagogiques

### Travail préparatoire

démarrer hadoop sur la vm avec le script ~/start-hadoop.sh

* créer l'arborescence suivante sur hdfs /data/raw/cities/v1/csv/ sur hdfs
* copier le fichier /user/simplon/laposte_hexasmal.csv dans le repertoire /data/raw/cities/v1/csv/
* créer une table hive externe nommée cities qui pointe sur le répertoire /data/raw/cities/v1/csv/
* utiliser tblproperties ('skip.header.line.count'='1') lors de la création pour ignore le header du fichier csv.

```bash
hdfs dfs -mkdir -p /data/raw/cities/v1/csv/ 
hdfs dfs -cp /user/simplon/laposte_hexasmal.csv /data/raw/cities/v1/csv/ 
hdfs dfs -cp laposte_hexasmal.csv /data/raw/cities/v1/csv/laposte_hexasmal.csv
```

```hive
DROP TABLE cities;
CREATE EXTERNAL TABLE cities
(
  code_commune_insee string,
  nom_de_la_commune string,
  code_postal string,
  ligne_5 string,
  libelle_d_acheminement string,
  coordonnees_gps string

)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/data/raw/cities/v1/csv/'
TBLPROPERTIES('skip.header.line.count'='1');
```

vérifier que table pointe correctement sur le fichier en faisant une requête hive

```hive
hive> select * from cities LIMIT 1;
```

### Lecture de fichier

lancer un shell interactif pyspark ou pyspark3 dans le shell interactif

```bash
spark-shell --master local[*]
pyspark3
```

Lire le fichier csv cities depuis HDFS

```spark
df_hdfs = spark.read.csv("hdfs:///data/raw/cities/v1/csv/laposte_hexasmal.csv", header=True, sep=";")
```

Lire la table hive cities

```spark
spark.sql("show tables").show()
df_hive = spark.sql("select * from cities limit 5")
df_hive.show()
```

comparer le schéma des deux tables comparer les premières lignes des deux dataframe

```spark
df_hive.printSchema()
df_hdfs.printSchema()
```

Créer un dataframe à partir de la liste de personne ci-dessous people_list = [('john', 'doe', 34, 75018), ('jane', 'doe', 42, 64310), ('paul', 'martin', 14, 33600)]
Le nom et type de colonnes sont: FirstName: string LastName: string Age: long ZipCode: long

```spark
people_list = [('john', 'doe', 34, 75018), ('jane', 'doe', 42, 64310), ('paul', 'martin', 14, 33600)]
df_people = spark.createDataFrame(people_list, ['FirstName', 'LastName', 'Age', 'ZipCode'])
df_people.show()
df_people.dtypes
df_people.printSchema()
```

écrire le dataframe de personne dans HDFS au format parquet dans le répertoire /raw/people/v1/parquet

```bash
hdfs dfs -mkdir -p /raw/people/v1/
```

```spark
df_people.write.mode("overwrite").parquet('hdfs:///raw/people/v1/parquet/')
```

écrire le dataframe de personne dans une table hive nommée 'people' la table doit être une table interne

```spark
df_people.write.mode("overwrite").saveAsTable("people");
spark.sql("drop table people")
spark.sql("alter table people set tblproperties('EXTERNAL'='FALSE')")

```

### App

/!\ pour installer les dépendances : pas de proxy et lancer la commande suivante

```bash
cd <projet>
sudo pip3 install -r requirements.txt
```

Lancer le projet

```bash
spark-submit --master local ./fichier.py
```

* créer un fichier python
* créer une spark session
* créer un dataframe de cities en utilisant le fichier csv cities
* écrire le dataframe de cities dans HDFS au format parquet dans /experiment/cities/v1/parquet
* faire en sorte que les données soient écrasées si on relance notre application stopper la spark session

* supprimer les données cities sur HDFS Lancer votre fichier python en utilisant spark-submit vérifier que les données ont bien été créées

* créer une application python placer votre fichier de script cities dans le main de votre application packager votre application dans un egg

* supprimer les données cities sur HDFS Lancer votre egg python en utilisant spark-submit vérifier que les données ont bien été créées

* Créer une classe cities qui possède une fonction read servant à charger les données cities de hive dans un dataframe.

* écrire le nom de chaque colonne de la table dans une constante

### Manipulation de base​

* reprenez votre application spark et créer un nouveau dataframe cities_clean dans le main de votre application qui contiendra uniquement les colonnes suivantes ;

* code postal, code insee, nom de la ville, coordonnees gps

* Créer une colonne 'dept' qui contient les deux premiers chiffres du code postal. La colonne doit être de type string

* sauvegarder votre dataframe nettoyer sur HDFS dans le dossier /refined/cities/v1/parquet

### Test

​* dans votre application python créer une fonction split_lat_long qui prend en entrée un dataframe et qui renvoie un dataframe. La fonction doit transformer la colonne coordonnees_gps en deux colonnes latitude et longitude. Les données de latitude doivent être de type double. La colonne coordonnees_gps initiale ne doit plus exister dans le dataframe de sortie

* écrire un test pour tester la fonction split_lat_long

* créer une fonction 'departement' qui extrait les deux premiers chiffres du code postal dans une colonne dept

* écrire un test pour tester la fonction departement

* Reprendre le code de votre application pour enchaîner l’exécution des fonction split_lat_long et departement écrire le résultat dans /refined/cities/v1/parquet

### Aggregation & Jointure

* à partir du dataframe clean_cities
* créer un nouveau dataframe contenant le nombre de communes par département,
* sauvegarder le résultat dans un fichier csv unique trié par ordre décroissant de compte (le département contenant le plus de villes doit être sur la première ligne)
* sauvegarder le résultat sur hdfs au format csv dans le dossier /refined/departement/v1/csv

### UDF

* Créer la fonction departement_udf qui a les mêmes paramètres d'entrée et sortie que la fonction département précédente, mais qui calcule correctement le département corse en utilisant une UDF (utiliser le test du chapitre précédent pour tester que votre fonction marche bien.

* sauvegarder le résultat sur HDFS en csv dans le dossier /refined/departement/v2/csv
* Faire une nouvelle fonction departement_fct qui gère le cas de la Corse sans UDF, mais uniquement avec les fonctions disponible dans sur les colonnes. vous pouvez par exemple utiliser les fonctions ; case, when
* Une fois les fonctions terminées dans le main de votre application faire un benchmark pour voir laquelle des deux solutions est la plus rapide.

### Window Function

* À l'aide de window function à chaque ville ajouter les coordonnées GPS de la préfecture du département.
* On la préfecture du département se situe dans la ville ayant le code postal le plus petit dans tout le département. Pour l’exercice on considère également que la Corse est un seul département (on ne sépare pas la haute corse et la corse du sud).
* Une fois la préfecture trouvée, calculer la distance relative de chaque ville par rapport à la préfecture. On ne cherche pas une distance en km.
* calculer la distance moyenne et médiane à la préfecture par département sauvegarder le résultat sur HDFS en csv dans le dossier /refined/departement/v3/csv

### Scala

réécrire votre application en scala
