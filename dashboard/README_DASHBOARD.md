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
