# Projet Hadoop TP3

## Lancement du conteneur Hadoop

Pour démarrer le conteneur Hadoop avec Docker, exécute la commande suivante :

```bash
docker run --rm -p 8088:8088 -p 9870:9870 -p 9864:9864 -v "C:\Users\User\Desktop\BigData\hadoop-tp3\data:/data" -v "C:\Users\User\Desktop\BigData\hadoop-tp3\jars:/jars" --name hadoop-container hadoop-image 
```
## Connexion au conteneur
Une fois le conteneur démarré, tu peux te connecter à celui-ci via une session bash en exécutant :

```bash
docker exec -it hadoop-container /bin/bash
```
##  Compilation et préparation des fichiers
### Nettoyer et compiler le projet Maven :
```bash
mvn clean package
```
### Créer le répertoire HDFS pour les données :
```bash
hdfs dfs -mkdir -p /user/Alain/relationships
hdfs dfs -put data/relationships/data.txt /user/Alain/relationships/

```
### Charger les données dans HDFS :
```bash
hadoop fs -put data/relationships/data.txt /input/job1/
hdfs dfs -mkdir -p /input/job1/
```
## Exécution des Jobs Hadoop
### Exécuter le Job 1 :
```bash
hadoop jar /jars/tpfinal-alain_abhay_job1.jar /input/job1/data.txt /output/job1
```
### Vérifier les résultats :
```bash
hdfs dfs -cat /output/job1/part-r-00000
```
### Supprimer les répertoires de sortie (si besoin) :
```bash
hdfs dfs -rm -r /output/job1
```
### Exécuter le Job 2 :
```bash
hadoop jar /jars/tpfinal-alain_abhay_job2.jar /output/job1/part-r-00000 /output/job2
```
### Exécuter le Job 3 :
```bash
hadoop jar /jars/tpfinal-alain_abhay_job3.jar /output/job2/part-r-00000 /output/job3
```
