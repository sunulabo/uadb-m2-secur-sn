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
