#!/usr/bin/env python3
"""Demarre un role HDFS unique sans script shell externe."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def run(command: list[str]) -> None:
    completed = subprocess.run(command, check=False)
    if completed.returncode:
        raise SystemExit(completed.returncode)


def main() -> int:
    role = os.getenv("HDFS_ROLE", "namenode").strip().lower()
    if role == "namenode":
        version_file = Path("/hadoop/dfs/name/current/VERSION")
        version_file.parent.mkdir(parents=True, exist_ok=True)
        if not version_file.exists():
            run(["hdfs", "namenode", "-format", "-force", "-nonInteractive"])
        os.execvp("hdfs", ["hdfs", "namenode"])
    if role == "datanode":
        Path("/hadoop/dfs/data").mkdir(parents=True, exist_ok=True)
        os.execvp("hdfs", ["hdfs", "datanode"])
    raise SystemExit(f"HDFS_ROLE invalide: {role}")


if __name__ == "__main__":
    raise SystemExit(main())
