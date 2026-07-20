-- Reference Beeline. Le service hive-init applique le meme DDL automatiquement.
CREATE DATABASE IF NOT EXISTS secur_sn;
USE secur_sn;

CREATE EXTERNAL TABLE IF NOT EXISTS hotspots_24h_historique (
  hotspot_24h_id STRING,
  zone STRING,
  grid_2km_id STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  nb_incidents BIGINT,
  nb_victimes BIGINT,
  heure_critique INT,
  score_risque DOUBLE,
  niveau_risque STRING,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  processed_at TIMESTAMP
)
PARTITIONED BY (batch_id INT, snapshot_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/secur-sn/gold/hotspots_24h';

MSCK REPAIR TABLE hotspots_24h_historique;

CREATE OR REPLACE VIEW vue_hotspots_24h AS
WITH derniers_hotspots AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY hotspot_24h_id ORDER BY batch_id DESC, processed_at DESC
  ) AS rang
  FROM hotspots_24h_historique
)
SELECT hotspot_24h_id, zone, grid_2km_id, latitude, longitude,
       nb_incidents, nb_victimes, heure_critique, score_risque,
       niveau_risque, window_start, window_end
FROM derniers_hotspots
WHERE rang = 1
ORDER BY score_risque DESC;

CREATE OR REPLACE VIEW vue_recommandations_patrouilles AS
SELECT zone, grid_2km_id, niveau_risque, score_risque, heure_critique,
       CASE WHEN niveau_risque = 'ROUGE' THEN 'Patrouille immediate et renfort CETUD'
            WHEN niveau_risque = 'ORANGE' THEN 'Patrouille preventive ciblee'
            ELSE 'Surveillance reguliere' END AS recommandation
FROM vue_hotspots_24h
WHERE niveau_risque IN ('ORANGE', 'ROUGE')
ORDER BY score_risque DESC;
