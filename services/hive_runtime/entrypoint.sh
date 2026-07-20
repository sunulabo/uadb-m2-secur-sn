#!/bin/sh
set -eu

# L'image Apache garde ce PID dans la couche writable lors d'un docker restart.
if [ "${SERVICE_NAME:-}" = "hiveserver2" ]; then
  rm -f /opt/hive/conf/hiveserver2.pid
fi

exec /entrypoint.sh "$@"
