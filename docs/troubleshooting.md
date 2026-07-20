# Troubleshooting Secur-SN

## Kafka ne demarre pas

Verifier Docker Desktop, puis relancer :

```bash
docker compose down
make kafka
```

## Spark streaming indisponible

Le driver Spark local est un service Docker. Verifier ses logs puis son interface :

```bash
make logs SERVICE=spark-streaming
make streaming-live
```

Le fallback local reste disponible avec `make streaming`.

## HBase indisponible

Le projet conserve une preuve locale :

```bash
make hbase
cat data/processed/hbase_hotspots_mock.jsonl
```

## Hive trop lourd

Utiliser le SQL livre et le preview CSV :

```bash
make hive
cat reports/hive_vue_hotspots_preview.csv
```

## Airflow indisponible

Presenter `airflow/dags/secur_sn_dag.py` et lancer manuellement :

```bash
make ml
make dashboard
```

## PII en sortie

Relancer les tests :

```bash
make test
```

Les champs interdits sont `incident_id`, `nom_victime`, `tel_temoin`.
