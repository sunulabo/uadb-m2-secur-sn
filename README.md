# Secur-SN

Secur-SN implemente le sujet Big Data d'intelligence securitaire routiere au
Senegal. Le projet privilegie un flux microservices visible et reproductible,
avec Conda pour les preuves locales et Docker Compose pour l'infrastructure.

## Flux live

```text
field-simulator -> MinIO landing -> NiFi -> Kafka raw -> Spark Streaming
Spark -> HBase (consultation temps reel)
Spark -> HDFS Gold (Parquet + JSON snapshots) -> Hive Metastore PostgreSQL -> Hive views
```

MinIO est uniquement la zone d'atterrissage. Kafka ne transporte que les flux
bruts `secur_incidents_raw` et `secur_meteo` (et leurs DLQ). Apres le score et
l'anonymisation, Spark ecrit directement HBase et HDFS, sans topic Kafka
intermediaire.

## Demarrage

Docker Desktop doit disposer de 4 coeurs et 8 Go de memoire au minimum. Une
allocation de 10 a 12 Go rend HDFS, HBase et Hive plus confortables.

```bash
cd secur-sn
conda env create -f environment.yml
conda activate secur-sn
cp .env.example .env
make setup
make up
```

La commande Docker Compose equivalente est :

```bash
docker compose up -d --build
```

`make up` lance le flux de donnees essentiel : Kafka KRaft unique, MinIO,
NiFi, simulateur terrain, Spark local sur un thread, HDFS (NameNode + 2
DataNodes), HBase, PostgreSQL Hive, Hive Metastore, HiveServer2 et les
services d'initialisation Python. Aucun script d'orchestration sur la machine hote n'est
necessaire. Airflow et les interfaces optionnelles sont demarres a la demande
pour limiter la charge CPU.

Au premier lancement, NiFi doit decomprimer ses extensions Java et peut prendre
plusieurs minutes. Son repertoire de travail est ensuite conserve dans le
volume `nifi_work_cache`, prepare automatiquement par `nifi-work-init`, ce qui
accelere fortement les redemarrages suivants.
HBase atteint son etat `healthy` avant le chargement intensif de NiFi et le
demarrage de Spark, afin de conserver son master stable sur Docker Desktop.
Le service `platform-ready` confirme Kafka, MinIO, NiFi, Spark, HDFS, HBase et
Hive. Il est aussi utilise lorsque l'on lance Airflow a la demande.

## Interfaces

- MinIO Console : <http://localhost:9001> (`securadmin` / `securadmin123`)
- NiFi : <http://localhost:8081/nifi>
- Spark driver : <http://localhost:4040>
- HDFS NameNode : <http://localhost:9870>
- HBase Master : <http://localhost:16010>
- HiveServer2 : <http://localhost:10002>

Interfaces optionnelles, a lancer seulement pour l'observation :

```bash
make ui          # Kafka UI (http://localhost:8088) + Airflow Webserver (http://localhost:8082)
make airflow     # Scheduler Airflow et sa base, pour executer le DAG ML
```

## Verification du flux

1. Dans MinIO, verifier les nouveaux JSONL dans `secur-sn-landing`.
2. Dans NiFi, ouvrir `Secur_SN_Ingestion_MinIO_Kafka` : `ListS3`, `FetchS3Object`
   et `PublishKafka_2_6` doivent traiter les objets.
3. Dans Kafka UI, verifier `secur_incidents_raw` et `secur_meteo`.
4. Dans Spark UI, verifier l'application `SecurSNStreaming` et ses deux threads locaux.
5. Examiner le temps reel : `make scan-hbase`.
6. Examiner HDFS Gold : `make hdfs-ls`.
7. Consulter les vues analytiques : `make hive-query`.

Les sorties HDFS sont organisees ainsi :

```text
/secur-sn/gold/alerts/batch_id=N/event_date=YYYY-MM-DD/*.parquet
/secur-sn/gold/hotspots/batch_id=N/snapshot_date=YYYY-MM-DD/*.parquet
/secur-sn/gold/hotspots_live/batch_id=N/*.json
```

`hive-init` cree les tables externes et execute `MSCK REPAIR` toutes les
minutes. Les vues attendues sont `secur_sn.vue_hotspots`,
`secur_sn.vue_tendances_vehicule`, `secur_sn.vue_risque_meteo` et
`secur_sn.vue_recommandations_patrouilles`.

## Demarrage par brique

```bash
make kafka       # Kafka unique et topics raw
make minio       # MinIO et buckets landing/quarantine
make ingestion   # MinIO -> NiFi -> Kafka + simulateur
make spark       # Spark local[2] et driver live
make hdfs        # NameNode, deux DataNodes et preparation Gold
make hbase       # stockage temps reel
make hive        # PostgreSQL, metastore, HiveServer2 et catalogue HDFS
make storage     # HDFS + HBase + Hive ensemble
make streaming-live  # driver Spark live (dependances incluses)
```

Le fallback reste disponible pour les preuves locales sans Docker :

```bash
make streaming MAX_RECORDS=40
make ml
make dashboard
make validate
```

## Confidentialite et reprise

Les champs `incident_id`, `nom_victime` et `tel_temoin` sont supprimes avant
toute sortie Spark. `incident_secure` est un hash SHA-256 sale. Les ecritures
HBase utilisent `incident_secure` ou `hotspot_id` comme cle; elles sont donc
idempotentes lors de la reprise d'un micro-batch. HDFS ecrit chaque lot dans
son propre chemin `batch_id=N` en mode overwrite pour ne pas empiler de fichiers
apres un nouvel essai.

Les checkpoints Spark persistent dans le volume Docker `spark_checkpoints`.
Au premier demarrage, Spark lit le backlog Kafka (`earliest`); ensuite il reprend
les offsets depuis ces checkpoints.
