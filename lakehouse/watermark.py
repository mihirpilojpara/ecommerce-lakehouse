"""High-water marks for incremental loads.

Each key is stored in its own small file, so parallel tasks never overwrite
each other's state. A watermark is only saved AFTER the data is written, so a
failed run is simply retried from the previous watermark.
"""

import os
from pathlib import Path

from .config import META_ROOT


def _path(key: str) -> Path:
    return Path(META_ROOT) / "watermarks" / f"{key}.txt"


def get(key: str) -> str | None:
    path = _path(key)
    if not path.exists():
        return None
    value = path.read_text().strip()
    return value or None


def save(key: str, value: str) -> None:
    path = _path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(value)
    os.replace(tmp, path)  # atomic: readers never see a half-written file
