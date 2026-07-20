-- Reference Beeline. Le service hive-init applique le meme DDL automatiquement.
CREATE DATABASE IF NOT EXISTS secur_sn;
USE secur_sn;

CREATE EXTERNAL TABLE IF NOT EXISTS incidents_historique (
  incident_secure STRING,
  zone STRING,
  type_incident STRING,
  type_vehicule STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  nb_victimes INT,
  heure INT,
  event_ts TIMESTAMP,
  score_gravite DOUBLE,
  score_vehicule DOUBLE,
  score_meteo DOUBLE,
  score_risque DOUBLE,
  processed_at TIMESTAMP
)
PARTITIONED BY (batch_id INT, event_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/secur-sn/gold/alerts';

CREATE EXTERNAL TABLE IF NOT EXISTS hotspots_historique (
  hotspot_id STRING,
  zone STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  nb_incidents BIGINT,
  nb_victimes BIGINT,
  heure_critique INT,
  score_risque DOUBLE,
  niveau_risque STRING,
  window_start TIMESTAMP,
  `timestamp` TIMESTAMP,
  processed_at TIMESTAMP
)
PARTITIONED BY (batch_id INT, snapshot_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/secur-sn/gold/hotspots';

MSCK REPAIR TABLE incidents_historique;
MSCK REPAIR TABLE hotspots_historique;

CREATE OR REPLACE VIEW vue_hotspots AS
WITH derniers_hotspots AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY hotspot_id ORDER BY batch_id DESC, processed_at DESC
  ) AS rang
  FROM hotspots_historique
)
SELECT zone, nb_incidents, nb_victimes, heure_critique, score_risque, niveau_risque, `timestamp`
FROM derniers_hotspots
WHERE rang = 1
ORDER BY score_risque DESC;

CREATE OR REPLACE VIEW vue_tendances_vehicule AS
SELECT type_vehicule, COUNT(*) AS nb_incidents, AVG(score_risque) AS score_moyen,
       AVG(nb_victimes) AS victimes_moyennes
FROM incidents_historique
GROUP BY type_vehicule
ORDER BY nb_incidents DESC;

CREATE OR REPLACE VIEW vue_risque_meteo AS
SELECT zone, AVG(score_meteo) AS score_meteo_moyen, AVG(score_risque) AS score_risque_moyen
FROM incidents_historique
GROUP BY zone
ORDER BY score_risque_moyen DESC;

CREATE OR REPLACE VIEW vue_recommandations_patrouilles AS
SELECT zone, niveau_risque, score_risque, heure_critique,
       CASE WHEN niveau_risque = 'ROUGE' THEN 'Patrouille immediate et renfort CETUD'
            WHEN niveau_risque = 'ORANGE' THEN 'Patrouille preventive ciblee'
            ELSE 'Surveillance reguliere' END AS recommandation
FROM vue_hotspots
WHERE niveau_risque IN ('ORANGE', 'ROUGE')
ORDER BY score_risque DESC;
