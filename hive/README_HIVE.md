# Hive Secur-SN

Spark ecrit l'agregat analytique 24 h dans HDFS Gold. Hive ne copie pas les donnees :
`hive-init` maintient des tables externes et les vues analytiques a partir du
metastore PostgreSQL distant.

- `secur_sn.incidents_historique` : `hdfs://namenode:8020/secur-sn/gold/alerts` ;
- `secur_sn.hotspots_24h_historique` : `hdfs://namenode:8020/secur-sn/gold/hotspots_24h` ;
- `vue_hotspots_24h` et `vue_recommandations_patrouilles`.

```bash
make hive
make hdfs-ls
make hive-query
make logs SERVICE=hive-init
```

Le metastore ecoute sur le port `9083` dans le reseau Docker. HiveServer2 est
accessible sur `localhost:10000` (JDBC/Beeline) et son interface web est sur
<http://localhost:10002>.
