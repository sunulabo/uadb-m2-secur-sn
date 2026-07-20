#!/usr/bin/env python3
"""Prepare les repertoires HDFS utiles au pipeline Secur-SN."""

from __future__ import annotations

import json
import os
import time

import requests


def webhdfs_url(path: str, operation: str) -> str:
    base = os.getenv("HDFS_WEB_URL", "http://namenode:9870").rstrip("/")
    return f"{base}/webhdfs/v1{path}?op={operation}"


def wait_for_datanodes(timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    required = int(os.getenv("HDFS_REQUIRED_DATANODES", "2"))
    last_error = ""
    while time.monotonic() < deadline:
        try:
            response = requests.get(
                os.getenv("HDFS_JMX_URL", "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"),
                timeout=5,
            )
            response.raise_for_status()
            beans = response.json().get("beans", [])
            live_nodes = json.loads(beans[0].get("LiveNodes", "{}")) if beans else {}
            if len(live_nodes) >= required:
                print(f"HDFS pret: {len(live_nodes)} DataNode(s) actif(s).", flush=True)
                return
            last_error = f"{len(live_nodes)}/{required} DataNode(s)"
        except Exception as exc:  # pragma: no cover - depend du cluster Docker
            last_error = str(exc)
        time.sleep(3)
    raise RuntimeError(f"HDFS indisponible apres attente: {last_error}")


def put_webhdfs(path: str, operation: str, extra_query: str = "") -> requests.Response:
    """Execute une operation WebHDFS malgre la phase de demarrage du NameNode."""
    deadline = time.monotonic() + int(os.getenv("HDFS_OPERATION_TIMEOUT_SECONDS", "300"))
    url = f"{webhdfs_url(path, operation)}{extra_query}"
    last_error = ""
    while time.monotonic() < deadline:
        try:
            response = requests.put(url, timeout=30)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = str(exc)
            time.sleep(3)
    raise RuntimeError(f"Operation WebHDFS {operation} refusee pour {path}: {last_error}")


def mkdir(path: str) -> None:
    response = put_webhdfs(path, "MKDIRS")
    if not response.json().get("boolean"):
        raise RuntimeError(f"Creation HDFS refusee: {path}")


def set_permission(path: str, permission: str) -> None:
    put_webhdfs(path, "SETPERMISSION", f"&permission={permission}")


def main() -> int:
    wait_for_datanodes(int(os.getenv("HDFS_READY_TIMEOUT_SECONDS", "600")))
    for path in (
        "/secur-sn/gold/alerts",
        "/secur-sn/gold/hotspots",
        "/secur-sn/gold/hotspots_live",
        "/user/hive/warehouse",
        "/tmp/hive",
    ):
        mkdir(path)
        print(f"Repertoire HDFS pret: {path}", flush=True)
    # HiveServer2 cree un sous-repertoire de session ici pour chaque requete.
    # Le controle interne de Hive exige donc que ce chemin soit ecrivable par tous.
    set_permission("/tmp/hive", "1777")
    print("Permissions HDFS appliquees: /tmp/hive -> 1777", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
