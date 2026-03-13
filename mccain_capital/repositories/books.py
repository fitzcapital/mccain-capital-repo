"""Books repository functions."""

from __future__ import annotations

import os
import re
from typing import Dict, List

from mccain_capital import runtime as app_runtime


def safe_filename(name: str) -> str:
    name = (name or "").strip()
    name = name.replace("\\", "/").split("/")[-1]
    name = re.sub(r"[^a-zA-Z0-9._ -]+", "", name)
    return name


def list_books() -> List[Dict[str, str]]:
    books_dir = app_runtime.books_root()
    os.makedirs(books_dir, exist_ok=True)
    files = []
    for fn in sorted(os.listdir(books_dir)):
        if fn.lower().endswith(".pdf"):
            files.append({"name": fn, "path": os.path.join(books_dir, fn)})
    return files
