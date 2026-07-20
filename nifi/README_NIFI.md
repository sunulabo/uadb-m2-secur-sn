# NiFi Secur-SN

Objectif : montrer une ingestion multi-sources avec NiFi comme producteur Kafka.
Le flow principal lit des fichiers JSONL bruts dans MinIO/S3 et les publie vers
le cluster Kafka KRaft.

Fichier livrable :

- `nifi/flow-definitions/secur_sn_ingestion.json`

Sources simulees :

- GAMA incidents routiers, deposes dans `s3://secur-sn-landing/incidents/...`
- CETUD transport et vehicules, portes par les champs incidents
- METEO observations par zone, deposees dans `s3://secur-sn-landing/meteo/...`

Commande proche production :

```bash
make up
```

Ce demarrage :

1. demarre le broker Kafka KRaft unique ;
2. cree les topics repliques `secur_incidents_raw`, `secur_meteo` et leurs DLQ ;
3. demarre MinIO et cree les buckets Secur-SN ;
4. demarre NiFi ;
5. cree le process group `Secur_SN_Ingestion_MinIO_Kafka` ;
6. lance le simulateur terrain qui depose les JSONL dans MinIO ;
7. publie les objets MinIO vers Kafka avec `PublishKafka_2_6`.

Processors attendus dans NiFi :

- `ListS3` : surveille les prefixes `incidents/` et `meteo/` du bucket MinIO ;
- `FetchS3Object` : recupere le contenu de chaque objet detecte ;
- `UpdateAttribute` : marque la source, le topic et l'etape du pipeline ;
- `PublishKafka_2_6` : publie vers `secur_incidents_raw` ou `secur_meteo` ;
- `PutFile` : met en quarantaine locale les echecs de lecture ou publication.

Interfaces utiles :

- NiFi : <http://localhost:8081/nifi>
- MinIO Console : <http://localhost:9001>
- Kafka UI : <http://localhost:8088>

Dans NiFi, ouvrir le process group `Secur_SN_Ingestion_MinIO_Kafka` depuis le
canvas racine. Il est positionne en haut a gauche. Les anciens groupes
`Secur_SN_Ingestion_Kafka` et `Secur_SN_Ingestion_Local` peuvent rester visibles
a droite, mais ils sont stoppes et ne representent pas le flux actif.

Le processor Kafka est configure en mode transactionnel avec `acks=all`. Les
offsets Kafka peuvent donc avancer de `N+1` pour un fichier de `N` lignes, car
Kafka stocke aussi un marqueur de transaction. Le consumer applicatif lit bien
uniquement les `N` messages JSON.

Commandes de debug par brique :

```bash
make kafka
make minio
make ingestion
make ps
```

Commande manuelle :

```bash
make ingestion
```

Captures a prendre :

- Process Group `Secur_SN_Ingestion_MinIO_Kafka`
- flux incidents vers `secur_incidents_raw`
- flux meteo vers `secur_meteo`
- controller service `Secur SN MinIO Credentials`
- queue ou data provenance
- processeur Kafka configure
