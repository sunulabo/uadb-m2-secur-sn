# Airflow Secur-SN

DAG attendu : `secur_sn_monitoring`.

Le DAG contient :

- `start`
- `generate_history`
- `valider_fichiers_du_jour`
- `verifier_privacy`
- `consolider_hive`
- `detecter_zones_rouges`
- `branch_retrain_or_update` avec `BranchPythonOperator`
- `retrain_model` si au moins deux zones sont rouges
- `update_hotspots` sinon
- `exporter_aggregats_hdfs`
- `generer_dashboard`
- `generer_rapport_crise`
- `nettoyer_quarantaine_30j`
- `end`

Commandes :

```bash
make airflow
make airflow-ui
```

`make up` ne demarre pas Airflow, afin de reserver le CPU au flux temps reel.
`make airflow` demarre le scheduler et sa base PostgreSQL. `make airflow-ui`
demarre ensuite le webserver; `make ui` lance a la fois le webserver Airflow et
Kafka UI. `platform-ready` attend Kafka, MinIO, NiFi, Spark, HDFS, HBase et
Hive avant que le webserver ou le scheduler ne soient demarres.

Interface : `http://localhost:8082`.

Identifiants demo : `admin` / `admin`.
