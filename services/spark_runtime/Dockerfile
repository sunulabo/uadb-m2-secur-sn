FROM bitnamilegacy/spark:3.5.1

USER root

ARG SPARK_VERSION=3.5.1
ARG SCALA_BINARY_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl python3 python3-pip \
    && /usr/bin/python3 -m pip install --no-cache-dir --break-system-packages happybase==1.3.0 thrift==0.20.0 \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar \
    && curl -fsSL -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar \
    && curl -fsSL -o /opt/bitnami/spark/jars/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
    && curl -fsSL -o /opt/bitnami/spark/jars/commons-pool2-${COMMONS_POOL2_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar

ENV PYSPARK_PYTHON=/usr/bin/python3

USER 1001
