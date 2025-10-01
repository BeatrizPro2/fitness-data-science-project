# src/fitdays.py
from __future__ import annotations
import pathlib as _pl
import shutil
from typing import List

def save_fitdays_images(files: List, out_dir: str | _pl.Path) -> list[str]:
    """
    Save uploaded images to out_dir and return list of file paths.
    """
    out = _pl.Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    saved = []
    for f in files or []:
        dest = out / f.name
        with open(dest, "wb") as w:
            shutil.copyfileobj(f, w)
        saved.append(str(dest))
    return saved
