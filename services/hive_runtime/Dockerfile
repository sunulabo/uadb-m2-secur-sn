FROM apache/hive:4.0.0

USER root

ARG POSTGRESQL_VERSION=42.7.5

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL -o /opt/hive/lib/postgres.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRESQL_VERSION}/postgresql-${POSTGRESQL_VERSION}.jar

COPY hadoop/conf/ /hive_custom_conf/
COPY services/hive_runtime/entrypoint.sh /usr/local/bin/secur-hive-entrypoint

RUN chmod 755 /usr/local/bin/secur-hive-entrypoint

ENV HIVE_CUSTOM_CONF_DIR=/hive_custom_conf

ENTRYPOINT ["/usr/local/bin/secur-hive-entrypoint"]
