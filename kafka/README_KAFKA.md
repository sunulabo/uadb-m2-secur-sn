# Kafka Secur-SN

Kafka est la couche de transport avant Spark. Les topics crees sont :

- `secur_incidents_raw` : incidents bruts avec PII ;
- `secur_meteo` : observations meteo par zone ;
- `secur_incidents_dlq` et `secur_meteo_dlq` : messages rejetes par ingestion.

Spark ne republie pas dans Kafka. Apres anonymisation, ses sorties vont
directement vers HBase et HDFS Gold.

```bash
make kafka
make consume-incidents
```

Kafka UI est expose sur <http://localhost:8088>. Le cluster utilise l'image
officielle `apache/kafka` en mode KRaft, avec trois brokers et six partitions
par topic (`replication.factor=3`, `min.insync.replicas=2`).
