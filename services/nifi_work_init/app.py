#!/usr/bin/env python3
"""Prepare le volume de travail NiFi pour l'utilisateur non privilegie."""

from __future__ import annotations

import os
from pathlib import Path


WORK_DIRECTORY = Path("/work")
NIFI_UID = 1000
NIFI_GID = 1000


def prepare_work_directory(path: Path = WORK_DIRECTORY) -> None:
    path.mkdir(parents=True, exist_ok=True)
    # NiFi creates the descendants itself. Recursing through the NAR cache on
    # every startup turns a lightweight permission check into a long disk scan.
    os.chown(path, NIFI_UID, NIFI_GID)
    path.chmod(0o755)


def main() -> int:
    prepare_work_directory()
    print("Volume de travail NiFi pret pour l'utilisateur nifi.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
