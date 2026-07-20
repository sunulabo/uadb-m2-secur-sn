# Plan de demonstration

## Avant la soutenance

```bash
cd secur-sn
conda env create -f environment.yml
conda activate secur-sn
cp .env.example .env
make setup
docker compose config
make validate
make streaming
make ml
make dashboard
make demo
```

## Demo live

1. Montrer l'arborescence du projet.
2. Montrer `docs/microservices_runbook.md`.
3. Lancer `make up`.
4. Montrer MinIO Console avec le bucket `secur-sn-landing`.
5. Montrer NiFi et le groupe `Secur_SN_Ingestion_MinIO_Kafka`.
6. Montrer Kafka UI et les topics crees automatiquement.
7. Lancer `make streaming`.
8. Montrer que les alertes n'ont pas de PII.
9. Executer `make hbase` puis `make hive`.
10. Montrer `hive/hive_setup.sql` et `reports/hive_vue_hotspots_preview.csv`.
11. Lancer `make ml`.
12. Lancer `make dashboard`.
13. Ouvrir `reports/dashboard_hotspots.png`.
14. Ouvrir `reports/dashboard_live.html`.
15. Montrer `reports/privacy_metrics.json`.
16. Montrer `reports/rapport_crise_zone_rouge.md` et `reports/rapport_crise_zone_rouge.png`.
17. Montrer `reports/confusion_matrix.png` et `reports/feature_importance.png`.
18. Terminer avec `reports/demo_proofs/summary.txt`.

## Phrase de defense

La plateforme est concue pour etre demonstrable localement. Quand un composant
lourd comme HBase ou Hive ne demarre pas sur la machine, le code garde la meme
logique metier et ecrit une preuve fallback locale documentee.

## Artefacts renforces apres analyse Vox-SN

- Dashboard HTML autonome avec cartes de synthese, privacy et recommandations.
- Rapport de crise centre sur la zone rouge prioritaire.
- Metrics privacy lisibles : PII detectees en entree, bloquees en sortie, fuites traitees.
- Visuels ML pour expliquer la performance et les variables importantes.
- Target `make demo` pour rejouer toute la chaine en une commande.
