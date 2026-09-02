#!/usr/bin/env python3
"""Pipeline Spark Structured Streaming et fallback local JSON."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark.spark_utils import (
    FORBIDDEN_PII_FIELDS,
    aggregate_alerts,
    build_alert,
    grid_2km_id,
    load_env_file,
    project_path,
    read_jsonl,
    sample_incident,
    sample_meteo,
    write_csv,
    write_jsonl,
)
from spark.hbase_sink import hbase_settings, wait_for_hbase, write_hotspot_partition, write_incident_partition


DEFAULT_STREAMING_SETTINGS = {
    "checkpoint_root": "/opt/secur-sn/data/checkpoints",
    "hdfs_gold_root": "/secur-sn/gold",
    "hdfs_namenode_uri": "hdfs://namenode:8020",
    "hotspot_window": "5 minutes",
    "hotspot_slide": "1 minute",
    "analytics_window": "24 hours",
    "analytics_slide": "1 hour",
    "max_offsets_per_trigger": "120",
    "starting_offsets": "earliest",
    "trigger_interval": "60 seconds",
    "watermark_delay": "2 minutes",
}


def streaming_ready_marker() -> Path:
    """Retourne le marqueur ecrit seulement apres le demarrage des deux requetes."""
    return Path(os.getenv("SPARK_READY_MARKER", "/opt/secur-sn/data/runtime/spark_streaming_ready.json"))


def clear_streaming_ready_marker() -> None:
    marker = streaming_ready_marker()
    marker.unlink(missing_ok=True)


def write_streaming_ready_marker() -> None:
    marker = streaming_ready_marker()
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(
        json.dumps(
            {
                "status": "ready",
                "queries": [
                    "secur_sn_alerts_to_hbase",
                    "secur_sn_hotspots_to_hbase",
                    "secur_sn_hotspots_24h_to_hdfs",
                ],
                "started_at": datetime.now(timezone.utc).isoformat(),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def streaming_settings(environment: Mapping[str, str] | None = None) -> Dict[str, str]:
    """Retourne les reglages live, utilisables sans demarrer PySpark."""
    env = os.environ if environment is None else environment
    return {
        "bootstrap": env.get(
            "KAFKA_STREAMING_BOOTSTRAP_SERVERS",
            env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        ),
        "checkpoint_root": env.get("SPARK_CHECKPOINT_ROOT", DEFAULT_STREAMING_SETTINGS["checkpoint_root"]),
        "hdfs_gold_root": env.get("HDFS_GOLD_ROOT", DEFAULT_STREAMING_SETTINGS["hdfs_gold_root"]),
        "hdfs_namenode_uri": env.get("HDFS_NAMENODE_URI", DEFAULT_STREAMING_SETTINGS["hdfs_namenode_uri"]),
        "hotspot_window": env.get("SPARK_HOTSPOT_WINDOW", DEFAULT_STREAMING_SETTINGS["hotspot_window"]),
        "hotspot_slide": env.get("SPARK_HOTSPOT_SLIDE", DEFAULT_STREAMING_SETTINGS["hotspot_slide"]),
        "analytics_window": env.get("SPARK_ANALYTICS_WINDOW", DEFAULT_STREAMING_SETTINGS["analytics_window"]),
        "analytics_slide": env.get("SPARK_ANALYTICS_SLIDE", DEFAULT_STREAMING_SETTINGS["analytics_slide"]),
        "max_offsets_per_trigger": env.get(
            "SPARK_MAX_OFFSETS_PER_TRIGGER", DEFAULT_STREAMING_SETTINGS["max_offsets_per_trigger"]
        ),
        "salt": env.get("SECUR_SECRET_SALT", "change_me_for_demo"),
        "starting_offsets": env.get("SPARK_STARTING_OFFSETS", DEFAULT_STREAMING_SETTINGS["starting_offsets"]),
        "trigger_interval": env.get("SPARK_TRIGGER_INTERVAL", DEFAULT_STREAMING_SETTINGS["trigger_interval"]),
        "watermark_delay": env.get("SPARK_WATERMARK_DELAY", DEFAULT_STREAMING_SETTINGS["watermark_delay"]),
    }


def hdfs_options(settings: Mapping[str, str]) -> Dict[str, str]:
    """Construit la configuration Hadoop employee par Spark et ses executors."""
    namenode_uri = settings["hdfs_namenode_uri"].rstrip("/")
    if not namenode_uri.startswith("hdfs://"):
        raise ValueError(f"HDFS_NAMENODE_URI invalide: {namenode_uri}")
    return {"spark.hadoop.fs.defaultFS": namenode_uri}


def hdfs_gold_path(settings: Mapping[str, str], prefix: str) -> str:
    """Construit un chemin HDFS Gold, sans aucun stockage MinIO apres Spark."""
    root = settings["hdfs_gold_root"].strip("/")
    clean_prefix = prefix.strip("/")
    if not root:
        raise ValueError("HDFS_GOLD_ROOT ne peut pas etre vide")
    base = f"{settings['hdfs_namenode_uri'].rstrip('/')}/{root}"
    return f"{base}/{clean_prefix}" if clean_prefix else base


def assert_pii_free_columns(columns: Sequence[str], output_name: str) -> None:
    """Empeche une ecriture live si un champ PII survit au traitement."""
    leaked = sorted(set(columns).intersection(FORBIDDEN_PII_FIELDS))
    if leaked:
        raise ValueError(f"PII interdites dans {output_name}: {', '.join(leaked)}")


def run_fallback(max_records: int) -> int:
    """Execute le pipeline sans Spark ni Kafka, en JSON local."""
    incidents_path = project_path("data", "raw", "incidents_demo.jsonl")
    meteo_path = project_path("data", "raw", "meteo_demo.jsonl")

    incidents = read_jsonl(incidents_path)
    if len(incidents) < max_records:
        incidents.extend(sample_incident(index=index) for index in range(len(incidents), max_records))
        write_jsonl(incidents_path, incidents)

    meteo_rows = read_jsonl(meteo_path)
    if len(meteo_rows) < max_records:
        meteo_rows.extend(sample_meteo(index=index) for index in range(len(meteo_rows), max_records))
        write_jsonl(meteo_path, meteo_rows)

    meteo_by_zone: Dict[str, dict] = {row["zone"]: row for row in meteo_rows}
    alerts = []
    for index, incident in enumerate(incidents[:max_records]):
        meteo = meteo_by_zone.get(incident["zone"])
        if meteo is None:
            meteo = sample_meteo(index=index)
            meteo["zone"] = incident["zone"]
        alerts.append(build_alert(incident, meteo))

    hotspots = aggregate_alerts(alerts)
    write_jsonl(project_path("data", "processed", "alerts_fallback.jsonl"), alerts)
    write_jsonl(project_path("data", "processed", "hotspots_fallback.jsonl"), hotspots)
    write_csv(project_path("data", "processed", "hotspots_historique.csv"), hotspots)
    write_csv(project_path("data", "processed", "incidents_historique.csv"), alerts)

    print(f"Fallback Spark termine: {len(alerts)} alertes, {len(hotspots)} hotspots.")
    print("Sortie alertes: data/processed/alerts_fallback.jsonl")
    return 0


def run_spark_streaming() -> int:
    """Lance Kafka -> Spark -> HBase temps reel et HDFS Gold analytique."""
    clear_streaming_ready_marker()
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import (
            col,
            concat_ws,
            count,
            current_timestamp,
            date_format,
            expr,
            from_json,
            lit,
            max as spark_max,
            avg as spark_avg,
            round as spark_round,
            sha2,
            sum as spark_sum,
            udf,
            when,
            window,
        )
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )
    except Exception as exc:
        print(f"PySpark indisponible ({exc}). Utilisez --fallback.", file=sys.stderr)
        return 2

    load_env_file()
    settings = streaming_settings()
    checkpoint_root = Path(settings["checkpoint_root"])
    hbase_config = hbase_settings()

    spark_builder = SparkSession.builder.appName("SecurSNStreaming").config("spark.sql.shuffle.partitions", "3")
    for key, value in hdfs_options(settings).items():
        spark_builder = spark_builder.config(key, value)
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Le driver ne lance aucun micro-batch tant que le schema HBase n'est pas pret.
    wait_for_hbase(hbase_config)

    incident_schema = StructType(
        [
            StructField("incident_id", StringType(), False),
            StructField("nom_victime", StringType(), False),
            StructField("tel_temoin", StringType(), False),
            StructField("zone", StringType(), False),
            StructField("type_incident", StringType(), False),
            StructField("type_vehicule", StringType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("nb_victimes", IntegerType(), False),
            StructField("heure", IntegerType(), False),
            StructField("facteur_heure", DoubleType(), False),
            StructField("timestamp", StringType(), False),
        ]
    )
    meteo_schema = StructType(
        [
            StructField("zone", StringType(), False),
            StructField("temperature", DoubleType(), True),
            StructField("pluie_mm", DoubleType(), True),
            StructField("visibilite", StringType(), True),
            StructField("route_mouillee", BooleanType(), True),
            StructField("timestamp", StringType(), False),
        ]
    )

    def score_gravite_udf_value(value: str) -> float:
        if value and "MORTEL" in value:
            return 5.0
        if value and "GRAVE" in value:
            return 3.0
        if value and "LEGER" in value:
            return 1.0
        return 0.5

    def score_vehicule_udf_value(value: str) -> float:
        return {
            "MOTO_JAKARTA": 1.5,
            "CAR_RAPIDE": 1.3,
            "CAMION": 1.2,
            "VOITURE": 1.0,
            "BUS": 0.9,
            "CHARRETTE": 0.8,
        }.get(value, 1.0)

    def score_meteo_udf_value(pluie: float, route_mouillee: bool, visibilite: str) -> float:
        pluie = pluie or 0.0
        visibilite = (visibilite or "BONNE").upper()
        if pluie > 20:
            return 1.5
        if route_mouillee or pluie > 0 or visibilite in {"FAIBLE", "MAUVAISE"}:
            return 1.2
        return 1.0

    gravite_udf = udf(score_gravite_udf_value, DoubleType())
    vehicule_udf = udf(score_vehicule_udf_value, DoubleType())
    meteo_udf = udf(score_meteo_udf_value, DoubleType())
    grid_udf = udf(grid_2km_id, StringType())

    def scored_alerts_stream():
        """Construit un flux independant, sans topic Kafka intermediaire apres Spark."""
        raw_incidents = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings["bootstrap"])
            .option("subscribe", "secur_incidents_raw")
            .option("startingOffsets", settings["starting_offsets"])
            .option("maxOffsetsPerTrigger", settings["max_offsets_per_trigger"])
            .option("failOnDataLoss", "false")
            .load()
        )
        incidents = (
            raw_incidents.select(from_json(col("value").cast("string"), incident_schema).alias("data"))
            .select("data.*")
            .where(col("heure").between(0, 23))
            .withColumn("event_ts", expr("to_timestamp(timestamp)"))
            .where(col("event_ts").isNotNull())
        )
        raw_meteo = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings["bootstrap"])
            .option("subscribe", "secur_meteo")
            .option("startingOffsets", settings["starting_offsets"])
            .option("maxOffsetsPerTrigger", settings["max_offsets_per_trigger"])
            .option("failOnDataLoss", "false")
            .load()
        )
        meteo = (
            raw_meteo.select(from_json(col("value").cast("string"), meteo_schema).alias("data"))
            .selectExpr(
                "data.zone as meteo_zone",
                "data.temperature",
                "data.pluie_mm",
                "data.visibilite",
                "data.route_mouillee",
                "to_timestamp(data.timestamp) as meteo_ts",
            )
            .where(col("meteo_ts").isNotNull())
        )
        joined = incidents.withWatermark("event_ts", settings["watermark_delay"]).join(
            meteo.withWatermark("meteo_ts", settings["watermark_delay"]),
            expr(
                "zone = meteo_zone AND meteo_ts >= event_ts - interval 15 minutes "
                "AND meteo_ts <= event_ts + interval 15 minutes"
            ),
            "inner",
        )
        return (
            joined.withColumn(
                "incident_secure",
                sha2(concat_ws(":", lit(settings["salt"]), col("incident_id"), col("timestamp")), 256),
            )
            .withColumn("score_gravite", gravite_udf(col("type_incident")))
            .withColumn("score_vehicule", vehicule_udf(col("type_vehicule")))
            .withColumn("score_meteo", meteo_udf(col("pluie_mm"), col("route_mouillee"), col("visibilite")))
            .withColumn(
                "score_risque",
                spark_round(col("score_gravite") * col("score_vehicule") * col("score_meteo") * col("facteur_heure"), 3),
            )
            .withColumn("grid_2km_id", grid_udf(col("latitude"), col("longitude")))
            .drop("incident_id", "nom_victime", "tel_temoin")
            .select(
                "incident_secure",
                "zone",
                "type_incident",
                "type_vehicule",
                "latitude",
                "longitude",
                "grid_2km_id",
                "nb_victimes",
                "heure",
                "event_ts",
                "score_gravite",
                "score_vehicule",
                "score_meteo",
                "score_risque",
            )
        )

    safe_alerts = scored_alerts_stream()
    hotspots = (
        scored_alerts_stream()
        .groupBy(window(col("event_ts"), settings["hotspot_window"], settings["hotspot_slide"]), col("zone"), col("grid_2km_id"))
        .agg(
            count("*").alias("nb_incidents"),
            spark_sum("nb_victimes").alias("nb_victimes"),
            spark_sum("score_risque").alias("score_risque_sum"),
            spark_max("heure").alias("heure_critique"),
            spark_avg("latitude").alias("latitude"),
            spark_avg("longitude").alias("longitude"),
        )
        .withColumn("score_risque", spark_round(col("score_risque_sum") * col("nb_incidents") / lit(3.0), 3))
        .withColumn(
            "niveau_risque",
            when(col("score_risque") > 20, lit("ROUGE")).when(col("score_risque") > 10, lit("ORANGE")).otherwise(lit("VERT")),
        )
        .withColumn("hotspot_id", concat_ws("#", lit("ops"), col("grid_2km_id"), expr("date_format(window.end, 'yyyyMMddHHmm')")))
        .select(
            "hotspot_id",
            "zone",
            "grid_2km_id",
            "latitude",
            "longitude",
            "nb_incidents",
            "nb_victimes",
            "heure_critique",
            "score_risque",
            "niveau_risque",
            expr("window.start as window_start"),
            expr("window.end as timestamp"),
        )
    )
    hotspots_24h = (
        scored_alerts_stream()
        .groupBy(
            window(col("event_ts"), settings["analytics_window"], settings["analytics_slide"]),
            col("zone"),
            col("grid_2km_id"),
        )
        .agg(
            count("*").alias("nb_incidents"),
            spark_sum("nb_victimes").alias("nb_victimes"),
            spark_sum("score_risque").alias("score_risque_sum"),
            spark_max("heure").alias("heure_critique"),
            spark_avg("latitude").alias("latitude"),
            spark_avg("longitude").alias("longitude"),
        )
        .withColumn("score_risque", spark_round(col("score_risque_sum") * col("nb_incidents") / lit(3.0), 3))
        .withColumn(
            "niveau_risque",
            when(col("score_risque") > 20, lit("ROUGE")).when(col("score_risque") > 10, lit("ORANGE")).otherwise(lit("VERT")),
        )
        .withColumn("hotspot_24h_id", concat_ws("#", lit("24h"), col("grid_2km_id"), expr("date_format(window.end, 'yyyyMMddHHmm')")))
        .select(
            "hotspot_24h_id", "zone", "grid_2km_id", "latitude", "longitude", "nb_incidents", "nb_victimes",
            "heure_critique", "score_risque", "niveau_risque", expr("window.start as window_start"),
            expr("window.end as window_end"),
        )
    )

    def write_alerts_batch(batch_df, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        assert_pii_free_columns(batch_df.columns, "alertes Spark")
        alerts = (
            batch_df.withColumn("batch_id", lit(batch_id))
            .withColumn("processed_at", current_timestamp())
            .persist()
        )
        try:
            assert_pii_free_columns(alerts.columns, "HBase alertes")
            alerts.rdd.foreachPartition(lambda rows: write_incident_partition(rows, hbase_config))
        finally:
            alerts.unpersist()

    def write_hotspots_batch(batch_df, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        assert_pii_free_columns(batch_df.columns, "hotspots Spark")
        snapshots = (
            batch_df.withColumn("batch_id", lit(batch_id))
            .withColumn("processed_at", current_timestamp())
            .persist()
        )
        try:
            assert_pii_free_columns(snapshots.columns, "HBase hotspots")
            # Les agregats sont petits; une partition protege le cumul idempotent par cellule.
            snapshots.repartition(1).rdd.foreachPartition(lambda rows: write_hotspot_partition(rows, hbase_config))
        finally:
            snapshots.unpersist()

    def write_hotspots_24h_batch(batch_df, batch_id: int) -> None:
        """Archive l'agregat analytique 24 h dans HDFS pour Hive uniquement."""
        if batch_df.rdd.isEmpty():
            return
        assert_pii_free_columns(batch_df.columns, "hotspots analytiques 24h Spark")
        aggregates = (
            batch_df.withColumn("batch_id", lit(batch_id))
            .withColumn("processed_at", current_timestamp())
            .withColumn("snapshot_date", date_format(col("window_end"), "yyyy-MM-dd"))
        )
        assert_pii_free_columns(aggregates.columns, "HDFS hotspots analytiques 24h")
        (
            aggregates.drop("batch_id")
            .write.mode("overwrite")
            .partitionBy("snapshot_date")
            .parquet(hdfs_gold_path(settings, f"hotspots_24h/batch_id={batch_id}"))
        )

    alerts_query = (
        safe_alerts.writeStream.foreachBatch(write_alerts_batch)
        .queryName("secur_sn_alerts_to_hbase")
        .option("checkpointLocation", str(checkpoint_root / "alerts_to_hbase_v4"))
        .outputMode("append")
        .trigger(processingTime=settings["trigger_interval"])
        .start()
    )
    hotspots_query = (
        hotspots.writeStream.foreachBatch(write_hotspots_batch)
        .queryName("secur_sn_hotspots_to_hbase")
        .option("checkpointLocation", str(checkpoint_root / "hotspots_to_hbase_v4"))
        .outputMode("append")
        .trigger(processingTime=settings["trigger_interval"])
        .start()
    )
    hotspots_24h_query = (
        hotspots_24h.writeStream.foreachBatch(write_hotspots_24h_batch)
        .queryName("secur_sn_hotspots_24h_to_hdfs")
        .option("checkpointLocation", str(checkpoint_root / "hotspots_24h_to_hdfs_v1"))
        .outputMode("append")
        .trigger(processingTime=settings["trigger_interval"])
        .start()
    )

    write_streaming_ready_marker()
    print("Streaming Secur-SN actif: Kafka -> Spark -> HBase, agregat 24h -> HDFS Gold.", flush=True)
    print(f"Checkpoints: {checkpoint_root}", flush=True)
    print(f"Sorties Gold: {hdfs_gold_path(settings, '')}", flush=True)
    spark.streams.awaitAnyTermination()
    alerts_query.stop()
    hotspots_query.stop()
    hotspots_24h_query.stop()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fallback", action="store_true")
    parser.add_argument("--max-records", type=int, default=60)
    args = parser.parse_args()

    if args.fallback:
        return run_fallback(args.max_records)
    return run_spark_streaming()


if __name__ == "__main__":
    raise SystemExit(main())
