"""Books repository functions."""

from __future__ import annotations

import os
import re
from typing import Dict, List

from mccain_capital.runtime import BOOKS_DIR


def safe_filename(name: str) -> str:
    name = (name or "").strip()
    name = name.replace("\\", "/").split("/")[-1]
    name = re.sub(r"[^a-zA-Z0-9._ -]+", "", name)
    return name


def list_books() -> List[Dict[str, str]]:
    os.makedirs(BOOKS_DIR, exist_ok=True)
    files = []
    for fn in sorted(os.listdir(BOOKS_DIR)):
        if fn.lower().endswith(".pdf"):
            files.append({"name": fn, "path": os.path.join(BOOKS_DIR, fn)})
    return files
