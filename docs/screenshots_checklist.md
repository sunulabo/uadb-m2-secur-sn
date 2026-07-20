# Checklist captures

- `docker compose ps`
- liste des topics Kafka
- message brut `secur_incidents_raw` avec PII
- message ou fichier alerte sans PII
- interface NiFi et process group
- terminal Spark ou fallback streaming
- scan HBase ou fallback `hbase_hotspots_mock.jsonl`
- requete Hive ou fallback `hive_vue_hotspots_preview.csv`
- DAG Airflow `secur_sn_monitoring`
- dashboard final `reports/dashboard_hotspots.png`
- tests `make validate`
- arborescence projet
