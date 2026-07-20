FROM apache/hadoop:3.5.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*

COPY hadoop/conf/ /opt/hadoop/etc/hadoop/
COPY services/hadoop_runtime/entrypoint.py /usr/local/bin/secur-hdfs

RUN chmod 755 /usr/local/bin/secur-hdfs

ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

ENTRYPOINT ["python3", "/usr/local/bin/secur-hdfs"]
