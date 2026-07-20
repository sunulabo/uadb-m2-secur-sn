"""DAG Airflow Secur-SN avec validation, privacy, ML et reporting."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


PROJECT_ROOT = Path("/opt/airflow/secur-sn")


def choose_next_task() -> str:
    """Branche vers retrain_model si trop de zones rouges sont detectees."""
    path = PROJECT_ROOT / "data" / "processed" / "hotspots_fallback.jsonl"
    red_count = 0
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("niveau_risque") == "ROUGE":
                red_count += 1
    return "retrain_model" if red_count >= 2 else "update_hotspots"


with DAG(
    dag_id="secur_sn_monitoring",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["secur-sn", "big-data", "hotspots", "privacy"],
) as dag:
    start = EmptyOperator(task_id="start")

    generate_history = BashOperator(
        task_id="generate_history",
        bash_command="cd /opt/airflow/secur-sn && python producers/generate_batch_history.py --count 200",
    )

    valider_fichiers_du_jour = BashOperator(
        task_id="valider_fichiers_du_jour",
        bash_command=(
            "cd /opt/airflow/secur-sn && "
            "test -s data/processed/alerts_fallback.jsonl && "
            "test -s data/processed/hotspots_fallback.jsonl"
        ),
    )

    verifier_privacy = BashOperator(
        task_id="verifier_privacy",
        bash_command=(
            "cd /opt/airflow/secur-sn && "
            "python -c \"from spark.spark_utils import write_privacy_metrics; "
            "m=write_privacy_metrics(); "
            "raise SystemExit(0 if m['processed_pii_leaks'] == 0 else 1)\""
        ),
    )

    consolider_hive = BashOperator(
        task_id="consolider_hive",
        bash_command=(
            "cd /opt/airflow/secur-sn && "
            "python -c \"from hive.catalog import catalog_statements, hive_settings; "
            "assert 'hdfs://' in '\\n'.join(catalog_statements(hive_settings()))\""
        ),
    )

    detecter_zones_rouges = BashOperator(
        task_id="detecter_zones_rouges",
        bash_command="cd /opt/airflow/secur-sn && python spark/streaming_secur_sn.py --fallback --max-records 80",
    )

    branch_retrain_or_update = BranchPythonOperator(
        task_id="branch_retrain_or_update",
        python_callable=choose_next_task,
    )

    retrain_model = BashOperator(
        task_id="retrain_model",
        bash_command="cd /opt/airflow/secur-sn && python spark/train_hotspot_model.py",
    )

    update_hotspots = BashOperator(
        task_id="update_hotspots",
        bash_command=(
            "cd /opt/airflow/secur-sn && "
            "python -c \"from hive.catalog import repair_statements; assert len(repair_statements()) == 3\""
        ),
    )

    exporter_aggregats_hdfs = BashOperator(
        task_id="exporter_aggregats_hdfs",
        bash_command=(
            "cd /opt/airflow/secur-sn && "
            "mkdir -p reports/airflow_exports && "
            "python -c \"from hive.catalog import gold_location, hive_settings; "
            "open('reports/airflow_exports/hdfs_gold_location.txt', 'w').write(gold_location(hive_settings(), 'hotspots') + '\\\\n')\""
        ),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    generer_dashboard = BashOperator(
        task_id="generer_dashboard",
        bash_command="cd /opt/airflow/secur-sn && python dashboard/generate_dashboard.py",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    generer_rapport_crise = BashOperator(
        task_id="generer_rapport_crise",
        bash_command="cd /opt/airflow/secur-sn && test -s reports/rapport_crise_zone_rouge.md",
    )

    nettoyer_quarantaine_30j = BashOperator(
        task_id="nettoyer_quarantaine_30j",
        bash_command="cd /opt/airflow/secur-sn && mkdir -p data/quarantine && find data/quarantine -type f -mtime +30 -delete",
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    start >> generate_history >> valider_fichiers_du_jour >> verifier_privacy
    verifier_privacy >> consolider_hive >> detecter_zones_rouges >> branch_retrain_or_update
    branch_retrain_or_update >> [retrain_model, update_hotspots] >> exporter_aggregats_hdfs
    exporter_aggregats_hdfs >> generer_dashboard >> generer_rapport_crise >> nettoyer_quarantaine_30j >> end
