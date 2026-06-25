# src/fitdays.py
"""
Fitdays screenshot handling for the Fitness Data Science app.
 
Two responsibilities:
  1. save_fitdays_images(...)  -> persist uploaded screenshots to disk
  2. parse_fitdays_image(...) / parse_fitdays_images(...) -> OCR the
     screenshots into structured body-composition data (weight + breakdown)
 
The OCR step uses Tesseract via pytesseract. Tesseract must be installed on
the system (e.g. `brew install tesseract`, `apt install tesseract-ocr`, or the
Windows installer). pytesseract is a thin wrapper and needs that binary.
"""
from __future__ import annotations
import pathlib as _pl
import re
import shutil
from datetime import datetime
from typing import List, Optional
 
# ---------------------------------------------------------------------------
# 1) Saving uploaded screenshots (unchanged API used by app.py)
# ---------------------------------------------------------------------------
def save_fitdays_images(files: List, out_dir: str | _pl.Path) -> list[str]:
    """Save uploaded image files to out_dir and return list of file paths."""
    out = _pl.Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    saved = []
    for f in files or []:
        dest = out / f.name
        # Streamlit UploadedFile -> rewind before copy so re-reads work
        try:
            f.seek(0)
        except Exception:
            pass
        with open(dest, "wb") as w:
            shutil.copyfileobj(f, w)
        saved.append(str(dest))
    return saved
 
 
# ---------------------------------------------------------------------------
# 2) OCR parsing of a Fitdays screenshot
# ---------------------------------------------------------------------------
 
# Canonical metric definitions: OCR-line-prefix (lowercase) -> (key, unit)
# Order matters only for readability; matching uses longest-prefix below.
_METRICS = {
    "weight":               ("weight_lb", "lb"),
    "bmi":                  ("bmi", None),
    "body fat":             ("body_fat_pct", "%"),
    "fat mass":             ("fat_mass_lb", "lb"),
    "fat-free body weight": ("fat_free_weight_lb", "lb"),
    "muscle mass":          ("muscle_mass_lb", "lb"),
    "muscle rate":          ("muscle_rate_pct", "%"),
    "skeletal muscle":      ("skeletal_muscle_pct", "%"),
    "bone mass":            ("bone_mass_lb", "lb"),
    "protein mass":         ("protein_mass_lb", "lb"),
    "protein":              ("protein_pct", "%"),
    "water weight":         ("water_weight_lb", "lb"),
    "body water":           ("body_water_pct", "%"),
    "subcutaneous fat":     ("subcutaneous_fat_pct", "%"),
    "visceral fat":         ("visceral_fat", None),
    "bmr":                  ("bmr_kcal", "kcal"),
    "body age":             ("body_age", None),
    "ideal body weight":    ("ideal_weight_lb", "lb"),
}
 
# Standard / rating vocabulary (longest first so "too high" beats "high")
_STANDARDS = ["too high", "too low", "excellent", "standard",
              "normal", "high", "low"]
 
_NUM_RE = re.compile(r"-?\d+(?:\.\d+)?")
 
 
def _clean_line(line: str) -> str:
    return re.sub(r"\s+", " ", line).strip()
 
 
def _match_label(norm_line: str) -> Optional[str]:
    """Return the longest metric label that norm_line starts with."""
    best = None
    for label in _METRICS:
        if norm_line.startswith(label):
            if best is None or len(label) > len(best):
                best = label
    return best
 
 
def _extract_standard(tail: str) -> Optional[str]:
    t = tail.lower()
    for s in _STANDARDS:
        if s in t:
            # normalise to Title Case ("Too High", "Excellent", ...)
            return " ".join(w.capitalize() for w in s.split())
    return None
 
 
def _parse_date(text: str) -> Optional[str]:
    """Pull a date like '11:54 Sep.24,2025' out of header text -> ISO date."""
    # Look for 'Mon.DD,YYYY' (Fitdays format), tolerating OCR spacing
    m = re.search(r"([A-Za-z]{3})\.?\s*(\d{1,2})\s*,\s*(\d{4})", text)
    if m:
        raw = f"{m.group(1)} {int(m.group(2))} {m.group(3)}"
        for fmt in ("%b %d %Y", "%B %d %Y"):
            try:
                return datetime.strptime(raw, fmt).date().isoformat()
            except ValueError:
                continue
    # Fallback: ISO-ish date anywhere in text
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return m.group(0)
    return None
 
 
def parse_fitdays_image(path: str | _pl.Path) -> dict:
    """
    OCR a single Fitdays screenshot and return a dict of metrics.
 
    Returns keys like weight_lb, bmi, body_fat_pct, muscle_mass_lb, ...
    plus '<key>_standard' for the app's rating column, a 'date' (ISO str or
    None), and 'source_file'. Missing/unreadable fields are simply absent.
    """
    try:
        import pytesseract
        from PIL import Image
    except ImportError as e:
        raise ImportError(
            "Fitdays OCR needs pytesseract + Pillow. "
            "Install with: pip install pytesseract pillow"
        ) from e
 
    _ensure_tesseract(pytesseract)
 
    path = _pl.Path(path)
    img = Image.open(path)
    text = pytesseract.image_to_string(img)
 
    date = _parse_date(text)
    if date is None:
        # The date header is small and sits in a tight band near the top;
        # cropping too far down pulls in the summary card and breaks OCR.
        w, h = img.size
        for frac in (0.08, 0.07, 0.09):
            header = img.crop((0, 0, w, int(h * frac)))
            date = _parse_date(pytesseract.image_to_string(header))
            if date:
                break
 
    record: dict = {"source_file": path.name, "date": date}
 
    for raw_line in text.splitlines():
        line = _clean_line(raw_line)
        if not line:
            continue
        norm = line.lower()
        label = _match_label(norm)
        if not label:
            continue
        key, _unit = _METRICS[label]
        if key in record:        # keep first (top-most) occurrence
            continue
        rest = line[len(label):]
        # OCR often renders the "lb" unit as "1b"/"ib"/"Ib" glued to the
        # value (e.g. "174.5lb" -> "174.51b"); normalise so the number
        # parser stops at the unit instead of swallowing the leading "1".
        rest = re.sub(r"(?<=\d)\s*[1IilL]b(?=\b|\s|$)", " lb", rest)
        num = _NUM_RE.search(rest)
        if not num:
            continue
        record[key] = float(num.group())
        std = _extract_standard(rest[num.end():])
        if std:
            record[f"{key}_standard"] = std
 
    return record
 
 
def parse_fitdays_images(paths: List[str | _pl.Path]):
    """
    Parse multiple screenshots into a tidy pandas DataFrame (one row each),
    sorted by date. Returns an empty DataFrame if nothing parses.
    """
    import pandas as pd
    rows = []
    for p in paths or []:
        try:
            rec = parse_fitdays_image(p)
            if any(k not in ("source_file", "date") for k in rec):
                rows.append(rec)
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date").reset_index(drop=True)
    return df
 
 
def _ensure_tesseract(pytesseract) -> None:
    """On Windows, Tesseract is often installed but not on PATH. Point
    pytesseract at the common install locations so users don't have to."""
    import os, shutil
    if shutil.which("tesseract"):
        return  # already on PATH
    candidates = [
        r"C:\\Program Files\\Tesseract-OCR\\tesseract.exe",
        r"C:\\Program Files (x86)\\Tesseract-OCR\\tesseract.exe",
    ]
    for c in candidates:
        if os.path.exists(c):
            pytesseract.pytesseract.tesseract_cmd = c
            return
 
 
if __name__ == "__main__":
    # Quick test:  python src/fitdays.py path/to/screenshot.jpg
    import sys, json
    if len(sys.argv) < 2:
        print("usage: python src/fitdays.py <screenshot.jpg> [more.jpg ...]")
        raise SystemExit(1)
    for p in sys.argv[1:]:
        print(f"\n=== {p} ===")
        print(json.dumps(parse_fitdays_image(p), indent=2))
