# Architecture Secur-SN

```text
Sources GAMA / CETUD / Meteo / GPS
  -> MinIO landing
  -> NiFi
  -> Kafka KRaft : secur_incidents_raw + secur_meteo
  -> Spark Structured Streaming local[1] par defaut, configurable
  -> HBase temps reel
  -> HDFS Gold Parquet
  -> Hive Metastore PostgreSQL + HiveServer2
  -> ML, Airflow, dashboard, recommandations
```

## Choix techniques

- Kafka est un broker KRaft unique, sans ZooKeeper.
- Spark supprime les PII avant toute sortie et calcule le score a partir de la
  gravite, du vehicule, de la meteo et de l'heure critique.
- HBase est le magasin de consultation temps reel : incidents, hotspots et
  statistiques par zone.
- HDFS contient les donnees Gold partitionnees par lot et par date.
- Hive expose les vues analytiques a partir de son metastore PostgreSQL.
- Airflow orchestre les preuves batch, le ML et les rapports; il ne remplace pas
  le streaming Spark.

## Donnees sensibles

`incident_id`, `nom_victime` et `tel_temoin` ne sortent jamais de Spark. Ils
sont remplaces par `incident_secure`, un hash SHA-256 sale.
