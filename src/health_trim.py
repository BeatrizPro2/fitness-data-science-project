# src/health_trim.py
from __future__ import annotations
import csv
import os
import re
import sys
import pathlib as _pl
from datetime import datetime, timedelta, timezone
from xml.etree.ElementTree import iterparse

# ---- Supported Apple Health types (add more any time) ----
TYPE_MAP = {
    # friendly_name: HK identifier in export.xml
    "body_mass": "HKQuantityTypeIdentifierBodyMass",
    "body_fat_pct": "HKQuantityTypeIdentifierBodyFatPercentage",
    "lean_mass": "HKQuantityTypeIdentifierLeanBodyMass",
    "heart_rate": "HKQuantityTypeIdentifierHeartRate",
    "resting_hr": "HKQuantityTypeIdentifierRestingHeartRate",
    "vo2max": "HKQuantityTypeIdentifierVO2Max",
    "step_count": "HKQuantityTypeIdentifierStepCount",
    "distance_walking_running": "HKQuantityTypeIdentifierDistanceWalkingRunning",
    "active_energy": "HKQuantityTypeIdentifierActiveEnergyBurned",
    "basal_energy": "HKQuantityTypeIdentifierBasalEnergyBurned",
    # workouts handled separately
}

_ISO_RE = re.compile(r"[:\-]")

def _to_utc(dt_str: str) -> datetime:
    """
    Apple writes times like '2024-10-01 07:21:32 -0400' or ISO strings.
    Normalize to timezone-aware UTC.
    """
    s = dt_str.strip().replace("T", " ").replace("Z", "+0000")
    # Example: '2024-10-01 07:21:32 -0400'
    try:
        # With explicit offset at the end
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S %z")
    except ValueError:
        try:
            # Sometimes without offset -> treat as local then assume UTC
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=timezone.utc)
        except ValueError:
            # Fallback for odd formats
            s = _ISO_RE.sub("", s)  # remove separators
            # last attempt as naive
            dt = datetime.strptime(s[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def trim_export(
    xml_path: str | os.PathLike,
    out_dir: str | os.PathLike,
    since_days: int = 365,
    record_types: list[str] | None = None,
    keep_workouts: bool = True,
) -> dict:
    """
    Stream, filter, and save Apple Health export to compact CSVs.

    Returns dict with paths to written files.
    """
    xml_path = _pl.Path(xml_path)
    out_dir = _pl.Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)

    # Normalize requested types -> HK identifiers
    if record_types:
        wanted_hk = {TYPE_MAP.get(k, k) for k in record_types}
    else:
        # sensible default set
        wanted_hk = {
            TYPE_MAP["body_mass"],
            TYPE_MAP["heart_rate"],
            TYPE_MAP["step_count"],
            TYPE_MAP["distance_walking_running"],
            TYPE_MAP["active_energy"],
        }

    # Writers (lazy-open when first row appears)
    files = {}
    rec_fp = None
    rec_writer = None
    wk_fp = None
    wk_writer = None

    try:
        context = iterparse(str(xml_path), events=("start", "end"))
        _, root = next(context)  # get root element

        for event, elem in context:
            tag = elem.tag.split("}")[-1]  # strip namespace if present

            # ---- Records (steps, weight, HR, etc.) ----
            if event == "end" and tag == "Record":
                r_type = elem.attrib.get("type")
                if r_type in wanted_hk:
                    start = _to_utc(elem.attrib.get("startDate", "1970-01-01 00:00:00 +0000"))
                    if start >= cutoff:
                        if rec_writer is None:
                            rec_fp = open(out_dir / "records.csv", "w", newline="", encoding="utf-8")
                            rec_writer = csv.DictWriter(
                                rec_fp,
                                fieldnames=[
                                    "type", "unit", "value",
                                    "start_utc", "end_utc",
                                    "sourceName", "sourceVersion", "device"
                                ],
                            )
                            rec_writer.writeheader()
                            files["records"] = str(_pl.Path(rec_fp.name))
                        rec_writer.writerow({
                            "type": r_type,
                            "unit": elem.attrib.get("unit"),
                            "value": elem.attrib.get("value"),
                            "start_utc": start.isoformat(),
                            "end_utc": _to_utc(elem.attrib.get("endDate", elem.attrib.get("startDate"))).isoformat(),
                            "sourceName": elem.attrib.get("sourceName"),
                            "sourceVersion": elem.attrib.get("sourceVersion"),
                            "device": elem.attrib.get("device"),
                        })
                # free memory
                elem.clear()
                root.clear()

            # ---- Workouts ----
            elif keep_workouts and event == "end" and tag == "Workout":
                start = _to_utc(elem.attrib.get("startDate", "1970-01-01 00:00:00 +0000"))
                if start >= cutoff:
                    if wk_writer is None:
                        wk_fp = open(out_dir / "workouts.csv", "w", newline="", encoding="utf-8")
                        wk_writer = csv.DictWriter(
                            wk_fp,
                            fieldnames=[
                                "workoutActivityType", "duration", "durationUnit",
                                "totalDistance", "totalDistanceUnit",
                                "totalEnergyBurned", "totalEnergyBurnedUnit",
                                "start_utc", "end_utc", "sourceName", "sourceVersion", "device"
                            ],
                        )
                        wk_writer.writeheader()
                        files["workouts"] = str(_pl.Path(wk_fp.name))

                    wk_writer.writerow({
                        "workoutActivityType": elem.attrib.get("workoutActivityType"),
                        "duration": elem.attrib.get("duration"),
                        "durationUnit": elem.attrib.get("durationUnit"),
                        "totalDistance": elem.attrib.get("totalDistance"),
                        "totalDistanceUnit": elem.attrib.get("totalDistanceUnit"),
                        "totalEnergyBurned": elem.attrib.get("totalEnergyBurned"),
                        "totalEnergyBurnedUnit": elem.attrib.get("totalEnergyBurnedUnit"),
                        "start_utc": start.isoformat(),
                        "end_utc": _to_utc(elem.attrib.get("endDate", elem.attrib.get("startDate"))).isoformat(),
                        "sourceName": elem.attrib.get("sourceName"),
                        "sourceVersion": elem.attrib.get("sourceVersion"),
                        "device": elem.attrib.get("device"),
                    })
                elem.clear()
                root.clear()

        return files

    finally:
        if rec_fp: rec_fp.close()
        if wk_fp: wk_fp.close()


# ---------- Simple CLI ----------
def _parse_cli(argv: list[str]) -> dict:
    import argparse
    p = argparse.ArgumentParser(description="Trim Apple Health export.xml into smaller CSVs.")
    p.add_argument("--xml", required=True, help="Path to Apple Health export.xml")
    p.add_argument("--out", default="data/processed/trimmed", help="Output directory")
    p.add_argument("--since-days", type=int, default=365, help="Keep only last N days (default 365)")
    p.add_argument("--types", nargs="*", default=None,
                   help="Record types to keep (friendly keys or HK identifiers). "
                        "Ex: body_mass heart_rate step_count")
    p.add_argument("--no-workouts", action="store_true", help="Skip workouts table")
    args = p.parse_args(argv)
    return {
        "xml_path": args.xml,
        "out_dir": args.out,
        "since_days": args.since_days,
        "record_types": args.types,
        "keep_workouts": (not args.no_workouts),
    }

if __name__ == "__main__":
    kw = _parse_cli(sys.argv[1:])
    out = trim_export(**kw)
    print("Written:", out)
