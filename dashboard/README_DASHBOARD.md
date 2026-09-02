# Dashboard Secur-SN

Commande :

```bash
python dashboard/generate_dashboard.py
```

Fichiers generes :

- `reports/dashboard_hotspots.png`
- `reports/dashboard_live.html`
- `reports/privacy_metrics.json`
- `reports/rapport_crise_zone_rouge.md`
- `reports/rapport_crise_zone_rouge.png`
- `reports/recommandations_patrouilles.csv`
- `data/dashboard/dashboard_summary.json`

Le script utilise Matplotlib si disponible. Sinon, il produit un PNG fallback
valide avec un histogramme simplifie des scores par zone. Le dashboard HTML est
autonome, sans CDN, et peut etre ouvert directement dans un navigateur.

## Dashboard FastAPI

Le service FastAPI sert la carte opérationnelle sur `http://localhost:8050`.
Il lit les hotspots courts depuis HBase et les cellules 24 h depuis Hive.
Après `make up`, lancez `make dashboard-ui` : cette commande démarre uniquement
le conteneur du dashboard et ne relance pas les dépendances. Si HBase et Hive
sont indisponibles, le service affiche les hotspots fallback présents dans
`data/processed`. Pour démarrer automatiquement toutes les dépendances de
l'interface, utilisez `make ui`.

L'interface utilise Inertia avec Vue 3, Vite et Leaflet. FastAPI reste
responsable des lectures HBase/Hive et transmet les props à la page Vue.

## Vues de supervision

Le centre de situation propose six vues :

- **Vue générale** : KPI, priorités et chaîne MinIO/NiFi/Kafka/Spark ;
- **Carte opérationnelle** : cellules 2 km x 2 km et sélection d'un hotspot ;
- **Analytique 24 h** : agrégats issus de Hive/HDFS Gold ;
- **Alertes HBase** : alertes opérationnelles uniquement ;
- **ML et recommandations** : artefacts ML, statut Airflow et actions terrain ;
- **Qualité des données** : sources, sorties, batches et confidentialité.

Les endpoints JSON correspondants sont `/api/dashboard/overview`,
`/api/dashboard/map`, `/api/dashboard/analytics`, `/api/dashboard/alerts`,
`/api/dashboard/ml` et `/api/dashboard/quality`. Ils conservent un fallback
local si HBase, Hive ou les artefacts ML sont indisponibles.
