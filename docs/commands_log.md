# Journal de commandes conseille

```bash
conda env create -f environment.yml
conda activate secur-sn
cp .env.example .env
make setup
docker compose config
make up
make producers PRODUCER_MAX_MESSAGES=3
make streaming MAX_RECORDS=30
make hbase
make hive
make ml
make dashboard
make test
make collect
```
