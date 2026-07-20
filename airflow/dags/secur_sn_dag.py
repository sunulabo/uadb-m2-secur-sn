"""DAG Airflow Secur-SN avec validation, privacy, ML et reporting."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule


def choose_next_task() -> str:
    """Branche selon les cellules rouges calculees par la vue Hive 24 h."""
    from hive.airflow_client import analytics_risk_summary

    summary = analytics_risk_summary()
    return "retrain_model" if summary["red_cells"] >= 2 else "update_hotspots"


with DAG(
    dag_id="secur_sn_monitoring",
    start_date=datetime(2026, 1, 1),
    schedule="0 */2 * * *",
    catchup=False,
    tags=["secur-sn", "big-data", "hotspots", "privacy"],
) as dag:
    start = EmptyOperator(task_id="start")

    generate_history = EmptyOperator(task_id="generate_history")

    valider_fichiers_du_jour = PythonOperator(
        task_id="valider_fichiers_du_jour",
        python_callable=lambda: __import__("hive.airflow_client", fromlist=["analytics_risk_summary"]).analytics_risk_summary(),
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

    consolider_hive = PythonOperator(
        task_id="consolider_hive",
        python_callable=lambda: __import__("hive.airflow_client", fromlist=["refresh_analytics_partitions"]).refresh_analytics_partitions(),
    )

    detecter_zones_rouges = PythonOperator(
        task_id="detecter_zones_rouges",
        python_callable=lambda: __import__("hive.airflow_client", fromlist=["analytics_risk_summary"]).analytics_risk_summary(),
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
            "python -c \"from hive.airflow_client import analytics_risk_summary; print(analytics_risk_summary())\""
        ),
    )

    exporter_aggregats_hdfs = BashOperator(
        task_id="exporter_aggregats_hdfs",
        bash_command=(
            "cd /opt/airflow/secur-sn && "
            "mkdir -p reports/airflow_exports && "
            "python -c \"from hive.catalog import gold_location, hive_settings; "
            "open('reports/airflow_exports/hdfs_gold_location.txt', 'w').write(gold_location(hive_settings(), 'hotspots_24h') + '\\\\n')\""
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

    start >> generate_history >> verifier_privacy >> consolider_hive
    consolider_hive >> valider_fichiers_du_jour >> detecter_zones_rouges >> branch_retrain_or_update
    branch_retrain_or_update >> [retrain_model, update_hotspots] >> exporter_aggregats_hdfs
    exporter_aggregats_hdfs >> generer_dashboard >> generer_rapport_crise >> nettoyer_quarantaine_30j >> end
