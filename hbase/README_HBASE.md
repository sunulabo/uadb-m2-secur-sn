# HBase Secur-SN

Spark ecrit directement dans HBase via Thrift depuis les executors. Aucun
consumer Kafka HBase n'est utilise.

Au demarrage, le driver Spark cree le namespace `secur` via l'API REST HBase,
puis initialise les tables via Thrift. Cela conserve les noms de tables du
sujet sans imposer une commande manuelle avant `make up`.

- `secur:incidents_temps_reel` : alertes anonymisees, TTL 24 heures ;
- `secur:hotspots` : dernier etat de chaque hotspot ;
- `secur:stats_zone` : cumul idempotent par zone et `hotspot_id`.

```bash
make hbase
make scan-hbase
make logs SERVICE=spark-streaming
```

L'interface HBase Master est sur <http://localhost:16010>. Le flux hotspot est
ecrit par une seule partition Spark pour conserver un cumul de zone coherent;
le calcul d'alertes utilise les deux threads locaux du driver Spark.
