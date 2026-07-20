SHELL := /bin/bash
.DEFAULT_GOAL := help
.PHONY: ui airflow-ui

PYTHON ?= python3
SERVICE ?=
MAX_RECORDS ?= 60
HISTORY_COUNT ?= 200
PRODUCER_MAX_MESSAGES ?= 30

.PHONY: help setup config build up full down ps status logs \
	kafka minio ingestion spark hdfs storage airflow \
	history producers streaming streaming-live hbase hive hive-query hdfs-ls ml dashboard privacy \
	trace demo collect validate test test-local clean-reports \
	consume-incidents scan-hbase

help:
	@printf '\nSecur-SN - commandes simples\n'
	@printf '============================\n'
	@printf 'Demarrage:\n'
	@printf '  make setup                 Prepare .env, dossiers et donnees demo\n'
	@printf '  make build                 Construit les services Docker locaux, dont Spark\n'
	@printf '  make up                    Lance le flux de donnees essentiel, sans interfaces optionnelles\n'
	@printf '  make full                  Alias de make up\n'
	@printf '  make down                  Arrete tout\n'
	@printf '  make ps                    Affiche les conteneurs\n\n'
	@printf 'Services par partie:\n'
	@printf '  make kafka                 Kafka KRaft unique + topics init\n'
	@printf '  make minio                 MinIO + buckets init\n'
	@printf '  make ingestion             Kafka + MinIO + NiFi + simulateur terrain\n'
	@printf '  make spark                 Spark local[1] et son driver live\n'
	@printf '  make hdfs                  Cluster HDFS : NameNode + 2 DataNodes\n'
	@printf '  make storage               HDFS Gold + HBase + Hive Metastore PostgreSQL\n'
	@printf '  make airflow               Airflow scheduler et base, a la demande\n'
	@printf '  make ui                    Kafka UI + Airflow Webserver\n'
	@printf '  make airflow-ui            Airflow Webserver seulement\n\n'
	@printf 'Pipeline local / preuves:\n'
	@printf '  make streaming             Streaming fallback local borne\n'
	@printf '  make streaming-live        Demarre le driver Spark/Kafka Docker\n'
	@printf '  make hbase                 Demarre le stockage HBase temps reel\n'
	@printf '  make hive                  Demarre Hive sur les Parquet HDFS Gold\n'
	@printf '  make hive-query            Affiche les vues Hive de controle\n'
	@printf '  make hdfs-ls               Liste les sorties HDFS Gold\n'
	@printf '  make ml                    Historique + entrainement ML\n'
	@printf '  make dashboard             Dashboard, privacy, rapport de crise\n'
	@printf '  make trace                 Trace locale complete du flux\n'
	@printf '  make demo                  Demo complete avec resume dans reports/demo_proofs\n'
	@printf '  make validate              Compile Python + tests + privacy\n\n'
	@printf 'Outils manuels:\n'
	@printf '  make logs SERVICE=nifi     Logs Docker du service choisi\n'
	@printf '  make consume-incidents     Lit quelques messages Kafka bruts\n'
	@printf '  make scan-hbase            Scan HBase\n\n'

setup:
	mkdir -p data/raw data/processed data/dashboard data/checkpoints data/samples reports screenshots
	test -f .env || cp .env.example .env
	touch screenshots/.gitkeep
	$(PYTHON) producers/generate_batch_history.py --count 120
	@printf 'Projet Secur-SN initialise. Prochaine etape: make up\n'

config:
	docker compose config

build:
	docker compose build kafka-init minio-init nifi-work-init nifi-init field-simulator spark-streaming namenode hdfs-init hive-metastore hive-init platform-ready

up:
	docker compose up -d --build --remove-orphans

full:
	$(MAKE) up

down:
	docker compose down

ps status:
	docker compose ps

logs:
	@test -n "$(SERVICE)" || { echo 'Usage: make logs SERVICE=nifi'; exit 2; }
	docker compose logs --tail=120 $(SERVICE)

kafka:
	docker compose up -d --build kafka kafka-init

minio:
	docker compose up -d --build minio minio-init

ingestion:
	docker compose up -d --build kafka kafka-init minio minio-init nifi nifi-init field-simulator

spark:
	docker compose up -d --build spark-streaming

hdfs:
	docker compose up -d --build namenode datanode-1 datanode-2 hdfs-init

storage:
	docker compose up -d --build namenode datanode-1 datanode-2 hdfs-init hbase hive-postgres hive-metastore hive-server hive-init

airflow:
	docker compose --profile airflow up -d --build airflow-scheduler

ui:
	docker compose --profile ui up -d kafka-ui airflow-webserver

airflow-ui:
	docker compose --profile ui up -d airflow-webserver

history:
	$(PYTHON) producers/generate_batch_history.py --count $(HISTORY_COUNT)

producers:
	@$(PYTHON) producers/kafka_producer_incidents.py --max-messages "$(PRODUCER_MAX_MESSAGES)" & \
	pid_incidents=$$!; \
	$(PYTHON) producers/kafka_producer_meteo.py --max-messages "$(PRODUCER_MAX_MESSAGES)" & \
	pid_meteo=$$!; \
	wait $$pid_incidents; \
	wait $$pid_meteo

streaming:
	$(PYTHON) spark/streaming_secur_sn.py --fallback --max-records $(MAX_RECORDS)

streaming-live:
	$(MAKE) spark

hbase:
	docker compose up -d --build hbase

hive:
	docker compose up -d --build namenode datanode-1 datanode-2 hdfs-init hive-postgres hive-metastore hive-server hive-init

hive-query:
	docker compose exec -T hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000/secur_sn -n hive -e "SHOW TABLES; DESCRIBE vue_hotspots"

hdfs-ls:
	docker compose exec -T namenode hdfs dfs -ls -R /secur-sn/gold

ml:
	$(PYTHON) producers/generate_batch_history.py --count 200
	$(PYTHON) spark/train_hotspot_model.py

dashboard:
	$(PYTHON) dashboard/generate_dashboard.py

privacy:
	$(PYTHON) -c "from spark.spark_utils import write_privacy_metrics; import json; print(json.dumps(write_privacy_metrics(), indent=2))"

trace:
	@set -euo pipefail; \
	report="reports/flow_trace.md"; \
	log_dir="reports/flow_trace"; \
	mkdir -p "$$log_dir"; \
	: > "$$report"; \
	printf '# Trace du flux Secur-SN\n\n' >> "$$report"; \
	printf -- '- Mode: fallback local borne\n' >> "$$report"; \
	run_step() { \
		name="$$1"; shift; log="$$log_dir/$${name}.log"; \
		printf '\n## %s\n\n- Commande: `%s`\n' "$$name" "$$*" >> "$$report"; \
		if "$$@" > "$$log" 2>&1; then \
			printf -- '- Resultat: OK\n- Log: `%s`\n' "$$log" >> "$$report"; \
		else \
			status=$$?; printf -- '- Resultat: ECHEC code %s\n- Log: `%s`\n' "$$status" "$$log" >> "$$report"; exit "$$status"; \
		fi; \
	}; \
	run_step "01_streaming" "$(PYTHON)" spark/streaming_secur_sn.py --fallback --max-records "$(MAX_RECORDS)"; \
	run_step "02_privacy" "$(PYTHON)" -c "from spark.spark_utils import write_privacy_metrics; write_privacy_metrics()"; \
		run_step "03_history" "$(PYTHON)" producers/generate_batch_history.py --count 200; \
		run_step "04_ml" "$(PYTHON)" spark/train_hotspot_model.py; \
		run_step "05_dashboard" "$(PYTHON)" dashboard/generate_dashboard.py; \
		printf '\n## Conclusion\n\nPreuves locales valides. Le flux live ecrit directement Spark -> HBase + HDFS -> Hive.\n' >> "$$report"; \
	echo "Trace generee: $$report"

demo:
	@set -u; \
	out_dir="reports/demo_proofs"; \
	summary="$$out_dir/summary.txt"; \
	mkdir -p "$$out_dir"; \
	: > "$$summary"; \
	failures=0; \
	log_line() { printf '%s\n' "$$1" | tee -a "$$summary"; }; \
	run_step() { \
		name="$$1"; shift; log_file="$$out_dir/$${name}.log"; \
		log_line ""; log_line "== $$name =="; log_line "Commande: $$*"; \
		if "$$@" > "$$log_file" 2>&1; then \
			log_line "OK: $$name"; \
		else \
			status=$$?; failures=$$((failures + 1)); log_line "ECHEC: $$name (code $$status, voir $$log_file)"; \
		fi; \
	}; \
	log_line "Secur-SN - preuves de demo"; \
	log_line "Date: $$(date -u '+%Y-%m-%dT%H:%M:%SZ')"; \
	docker compose ps > "$$out_dir/docker_compose_ps.log" 2>&1 || true; \
	run_step "01_streaming_fallback" "$(PYTHON)" spark/streaming_secur_sn.py --fallback --max-records 40; \
	run_step "02_history" "$(PYTHON)" producers/generate_batch_history.py --count 200; \
	run_step "03_ml_training" "$(PYTHON)" spark/train_hotspot_model.py; \
		run_step "04_dashboard_reports" "$(PYTHON)" dashboard/generate_dashboard.py; \
		run_step "05_privacy_check" "$(PYTHON)" -c "import json; m=json.load(open('reports/privacy_metrics.json', encoding='utf-8')); assert m['processed_pii_leaks'] == 0, m"; \
	log_line ""; log_line "Artefacts produits:"; \
	for artifact in reports/dashboard_hotspots.png reports/dashboard_live.html reports/privacy_metrics.json reports/rapport_crise_zone_rouge.md reports/rapport_crise_zone_rouge.png reports/confusion_matrix.png reports/feature_importance.png reports/ml_metrics.json reports/recommandations_patrouilles.csv; do \
		if [ -s "$$artifact" ]; then bytes="$$(wc -c < "$$artifact" | tr -d ' ')"; log_line "OK $$artifact ($$bytes octets)"; \
		else failures=$$((failures + 1)); log_line "MANQUANT $$artifact"; fi; \
	done; \
	log_line ""; \
	if [ "$$failures" -eq 0 ]; then log_line "Resultat: toutes les preuves attendues sont disponibles."; \
	else log_line "Resultat: $$failures probleme(s) detecte(s)."; fi; \
	exit "$$failures"

collect:
	@mkdir -p reports; \
	{ \
		echo "# Commandes et preuves Secur-SN"; \
		echo; \
		echo "Date: $$(date -u +"%Y-%m-%dT%H:%M:%SZ")"; \
		echo; \
		echo "## Fichiers generes"; \
		find data reports -maxdepth 2 -type f | sort; \
		echo; \
		echo "## Docker Compose"; \
		docker compose ps 2>&1 || true; \
	} > reports/commands_log_runtime.md; \
	echo "Preuves collectees dans reports/commands_log_runtime.md"

validate:
	$(PYTHON) -m compileall -q spark producers dashboard airflow tests nifi hbase services
	$(MAKE) test-local
	$(MAKE) privacy

test test-local:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py'

clean-reports:
	find reports/demo_proofs reports/flow_trace -type f -name '*.log' -delete 2>/dev/null || true

consume-incidents:
	docker compose exec -T kafka $${KAFKA_CLI_DIR:-/opt/kafka/bin}/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic secur_incidents_raw --from-beginning --max-messages 3

scan-hbase:
	@if docker compose ps hbase >/dev/null 2>&1; then \
		docker compose exec -T hbase bash -lc "echo \"scan 'secur:hotspots', {LIMIT => 5}\" | hbase shell -n" || true; \
	else \
		echo "HBase non lance. Demarrez-le avec: make hbase"; \
	fi
