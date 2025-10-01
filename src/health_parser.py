#!/usr/bin/env python3
"""
Apple Health export.xml → tidy CSVs (weight, BMI, body fat %, lean mass),
with optional merge of a Fitdays CSV.

Usage:
  python src/health_parser.py --xml data/export.xml --outdir data/processed
  python src/health_parser.py --xml data/export.xml --outdir data/processed --fitdays data/fitdays.csv

Outputs (in --outdir):
  - health_weight.csv          (timestamp, date, weight_lb)
  - health_bmi.csv             (timestamp, date, bmi)
  - health_bodyfat_pct.csv     (timestamp, date, body_fat_pct)
  - health_leanmass.csv        (timestamp, date, lean_mass_lb)
  - health_combined_daily.csv  (date, weight_lb, bmi, body_fat_pct, lean_mass_lb)
  - health_with_fitdays.csv    (if --fitdays provided)
"""

import argparse
import os
import csv
from collections import defaultdict, OrderedDict
from datetime import datetime
from xml.etree.ElementTree import iterparse

# ---- CLI ----
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--xml", required=True, help="Path to Apple Health export.xml")
    p.add_argument("--outdir", required=True, help="Directory to write CSVs")
    p.add_argument("--fitdays", default=None, help="Optional Fitdays CSV to merge (must include a 'date' column, YYYY-MM-DD)")
    return p.parse_args()

# ---- Helpers ----
def norm_date(iso_str: str):
    """
    Apple Health timestamps vary, e.g. '2025-09-10 12:22:00 -0400' or ISO with T.
    Return (date_only, iso_like_timestamp).
    """
    s = iso_str.replace("T", " ").replace("Z", "+0000")
    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.date().isoformat(), dt.isoformat()
        except ValueError:
            continue
    # Fallback: best-effort
    return iso_str[:10], iso_str

# Map HK types we care about → (label, expected unit)
TYPE_MAP = {
    "HKQuantityTypeIdentifierBodyMass": ("weight_kg", "kg"),
    "HKQuantityTypeIdentifierBodyMassIndex": ("bmi", "count"),
    "HKQuantityTypeIdentifierBodyFatPercentage": ("bodyfat_frac", "fraction"),
    "HKQuantityTypeIdentifierLeanBodyMass": ("lean_kg", "kg"),
}

def stream_records(xml_path: str):
    """
    Stream-parse the XML to avoid loading the whole file in memory.
    Yields tuples: (hk_type, value_str, date_only, iso_ts)
    """
    for event, elem in iterparse(xml_path, events=("end",)):
        if elem.tag == "Record":
            hk_type = elem.attrib.get("type")
            if hk_type in TYPE_MAP:
                value = elem.attrib.get("value")
                startDate = elem.attrib.get("startDate") or elem.attrib.get("creationDate") or elem.attrib.get("endDate")
                if value and startDate:
                    date_only, iso_ts = norm_date(startDate)
                    yield hk_type, value, date_only, iso_ts
            elem.clear()

def write_csv(path: str, rows, header):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

# ---- Main ----
def main():
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    rows_by_type = defaultdict(list)  # key → list of rows

    for hk_type, value, date_only, iso_ts in stream_records(args.xml):
        name, _unit = TYPE_MAP[hk_type]
        try:
            v = float(value)
        except ValueError:
            continue

        if name == "bodyfat_frac":
            # Apple Health stores body fat as fraction 0–1; convert to % when appropriate
            v_pct = v * 100.0 if v <= 1.5 else v
            rows_by_type["bodyfat_pct"].append([iso_ts, date_only, round(v_pct, 4)])
        elif name == "weight_kg":
            rows_by_type["weight"].append([iso_ts, date_only, round(v * 2.2046226218, 3)])  # → lb
        elif name == "lean_kg":
            rows_by_type["lean"].append([iso_ts, date_only, round(v * 2.2046226218, 3)])    # → lb
        elif name == "bmi":
            rows_by_type["bmi"].append([iso_ts, date_only, round(v, 3)])

    outputs = {}

    if rows_by_type.get("weight"):
        p = os.path.join(args.outdir, "health_weight.csv")
        write_csv(p, sorted(rows_by_type["weight"]), ["timestamp", "date", "weight_lb"])
        outputs["weight"] = p

    if rows_by_type.get("bmi"):
        p = os.path.join(args.outdir, "health_bmi.csv")
        write_csv(p, sorted(rows_by_type["bmi"]), ["timestamp", "date", "bmi"])
        outputs["bmi"] = p

    if rows_by_type.get("bodyfat_pct"):
        p = os.path.join(args.outdir, "health_bodyfat_pct.csv")
        write_csv(p, sorted(rows_by_type["bodyfat_pct"]), ["timestamp", "date", "body_fat_pct"])
        outputs["bodyfat"] = p

    if rows_by_type.get("lean"):
        p = os.path.join(args.outdir, "health_leanmass.csv")
        write_csv(p, sorted(rows_by_type["lean"]), ["timestamp", "date", "lean_mass_lb"])
        outputs["lean"] = p

    # Build a combined per-day table (last value per date per metric wins)
    by_date = OrderedDict()
    colmap = {
        "weight": ("weight_lb", "health_weight.csv"),
        "bmi": ("bmi", "health_bmi.csv"),
        "bodyfat": ("body_fat_pct", "health_bodyfat_pct.csv"),
        "lean": ("lean_mass_lb", "health_leanmass.csv"),
    }

    for key in ["weight", "bmi", "bodyfat", "lean"]:
        path = outputs.get(key)
        if not path:
            continue
        with open(path, newline="", encoding="utf-8") as f:
            next(f)  # skip header
            for line in f:
                ts, date_only, val = line.strip().split(",", 2)
                row = by_date.setdefault(date_only, {"date": date_only})
                row[colmap[key][0]] = float(val)

    combined_path = os.path.join(args.outdir, "health_combined_daily.csv")
    # Preserve column order
    def row_values(d):
        return [
            d.get("date"),
            d.get("weight_lb"),
            d.get("bmi"),
            d.get("body_fat_pct"),
            d.get("lean_mass_lb"),
        ]
    write_csv(
        combined_path,
        [row_values(v) for v in by_date.values()],
        ["date", "weight_lb", "bmi", "body_fat_pct", "lean_mass_lb"],
    )

    # Optional: merge Fitdays CSV
    if args.fitdays:
        try:
            import pandas as pd
        except ImportError:
            raise SystemExit("Pandas is required for --fitdays merge. Install with `pip install pandas`.")
        hp = __safe_read_csv(combined_path)
        fp = __safe_read_csv(args.fitdays)
        if "date" not in fp.columns:
            raise SystemExit("Fitdays CSV must include a 'date' column (YYYY-MM-DD).")
        merged = pd.merge(hp, fp, on="date", how="outer", sort=True)
        merged_path = os.path.join(args.outdir, "health_with_fitdays.csv")
        merged.to_csv(merged_path, index=False)
        print(f"Wrote {merged_path}")

    # Summary
    for k, p in outputs.items():
        print(f"{k}: {p}")
    print(f"combined: {combined_path}")
    print("Done.")

def __safe_read_csv(path):
    import pandas as pd
    return pd.read_csv(path)

if __name__ == "__main__":
    main()
