# Runbook microservices Secur-SN

## Demarrage unique

```bash
conda activate secur-sn
make setup
make up
```

Compose initialise les topics Kafka, les buckets MinIO, le flow NiFi, les
repertoires HDFS, les tables HBase et les tables/vues Hive. Les services
Python dans `services/` remplissent ces roles d'initialisation; il n'y a pas de
script shell annexe a lancer.

`platform-ready` attend les endpoints Kafka, MinIO, NiFi, Spark, HDFS, HBase
et Hive. Airflow est volontairement hors du demarrage de base pour preserver le
CPU du flux temps reel. Lancez-le seulement pour executer ou observer le DAG :

```bash
make airflow     # scheduler et base Airflow
make ui          # Kafka UI et webserver Airflow
```

Le webserver Airflow attend toujours `platform-ready` avant de s'ouvrir.

Au premier lancement, NiFi doit extraire ses extensions. `nifi-work-init`
prepare les permissions du volume `nifi_work_cache`, qui conserve ensuite ce
cache pour accelerer les redemarrages suivants.

## Dependances entre services

```text
Kafka unique + MinIO -> NiFi init -> field-simulator
NameNode + 2 DataNodes -> hdfs-init -> Spark Streaming
HBase -> Spark Streaming
PostgreSQL -> Hive Metastore -> HiveServer2 -> hive-init
Spark Streaming -> HBase + HDFS Gold -> Hive external tables
```

Le driver Spark local attend HBase avant d'ouvrir ses micro-batches. `hdfs-init`
attend les deux DataNodes et cree la zone Gold avant le driver. Ainsi, Kafka ne
perd pas d'offset pendant l'amorcage du stockage.

## Ordre d'observation

```bash
make ps
make logs SERVICE=nifi
make logs SERVICE=spark-streaming
make logs SERVICE=hive-init
make scan-hbase
make hdfs-ls
make hive-query
```

Les interfaces web sont documentees dans le README. Pour arreter sans effacer
les donnees persistantes :

```bash
make down
```

## Stockage

- HBase conserve `secur:incidents_temps_reel` avec TTL de 24 heures,
  `secur:hotspots` et `secur:stats_zone`.
- HDFS contient les Parquet Gold partitionnes par `batch_id` et date.
- Hive possede un metastore PostgreSQL distinct du HiveServer2. `hive-init`
  decouvre les nouvelles partitions toutes les 60 secondes.

Les snapshots de hotspot peuvent etre republies par Structured Streaming. La
vue `vue_hotspots` garde la version du `batch_id` le plus recent pour un meme
`hotspot_id`.

## Mode local

`make streaming`, `make ml` et `make dashboard` produisent des artefacts de
preuve locaux. Ils ne remplacent pas le stockage live HBase/HDFS/Hive.
