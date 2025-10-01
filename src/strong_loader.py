# src/strong_loader.py
from __future__ import annotations
import pandas as pd
import numpy as np

def _col(df, *cands):
    """Return first matching column (case/space-insensitive)."""
    norm = {c.lower().replace(" ", "").replace("_",""): c for c in df.columns}
    for c in cands:
        k = c.lower().replace(" ", "").replace("_","")
        if k in norm:
            return norm[k]
    return None

def load_strong_csv(file_like) -> dict:
    """
    Returns dict with:
      - by_day: daily totals (volume_kg, sets, reps, exercises, workout_count, duration_min*)
      - by_exercise: per-exercise totals
      - prs: simple 1RM (Epley) per exercise
    *duration_min included if present.
    """
    df = pd.read_csv(file_like, on_bad_lines="skip")

    # Try to find standard columns flexibly
    col_date   = _col(df, "Date", "Workout Date", "Start Time", "day")
    col_ex     = _col(df, "Exercise Name", "Exercise")
    col_set    = _col(df, "Set Order", "Set", "Set Number")
    col_weight = _col(df, "Weight", "Weight (kg)", "kg", "lb")
    col_unit   = _col(df, "Weight Unit", "Unit")
    col_reps   = _col(df, "Reps", "Rep")
    col_vol    = _col(df, "Volume", "Total Volume", "Volume (kg)")
    col_group  = _col(df, "Body Part", "Muscle Group", "Category")
    col_notes  = _col(df, "Notes", "Comment")
    col_dur    = _col(df, "Duration", "Total Time", "Workout Duration (min)")

    if col_date is None or col_ex is None:
        raise ValueError("Could not find required Strong columns (Date/Exercise).")

    # Clean
    out = df.copy()
    out[col_date] = pd.to_datetime(out[col_date]).dt.date
    out["date"] = pd.to_datetime(out[col_date])

    # Weight normalization to kg
    w = pd.to_numeric(out[col_weight], errors="coerce") if col_weight else np.nan
    if col_unit:
        u = out[col_unit].astype(str).str.lower()
        w = np.where(u.str.contains("lb"), w * 0.45359237, w)  # lb->kg
    out["weight_kg"] = w

    # Reps, Sets
    out["reps"] = pd.to_numeric(out[col_reps], errors="coerce") if col_reps else np.nan
    out["is_set"] = ~out["reps"].isna()
    out["sets"] = np.where(out["is_set"], 1, 0)

    # Volume
    if col_vol:
        vol = pd.to_numeric(out[col_vol], errors="coerce")
        # If volume looked like pounds, try to detect? (leave as-is; users often export kg)
    else:
        vol = out["weight_kg"] * out["reps"]
    out["volume_kg"] = vol

    # Exercise & group
    out["exercise"] = out[col_ex].astype(str)
    out["group"] = out[col_group].astype(str) if col_group else "Unknown"

    # Duration (per row; Strong exports vary)
    if col_dur:
        d = pd.to_numeric(out[col_dur], errors="coerce")
        # If duration comes like "01:05:00", convert to minutes
        if d.isna().all() and out[col_dur].astype(str).str.contains(":").any():
            d = pd.to_timedelta(out[col_dur].astype(str), errors="coerce").dt.total_seconds() / 60.0
        out["duration_min"] = d
    else:
        out["duration_min"] = np.nan

    # ---- Aggregations
    # by_day
    by_day = out.groupby("date", as_index=False).agg(
        volume_kg=("volume_kg", "sum"),
        sets=("sets", "sum"),
        reps=("reps", "sum"),
        exercises=("exercise", pd.Series.nunique),
        workout_count=(col_date, "nunique") if col_date else ("date", "nunique"),
        duration_min=("duration_min", "sum"),
    ).sort_values("date")

    # by_exercise
    by_exercise = out.groupby(["date", "exercise"], as_index=False).agg(
        top_set_kg=("weight_kg", "max"),
        sets=("sets", "sum"),
        reps=("reps", "sum"),
        volume_kg=("volume_kg", "sum"),
    ).sort_values(["date", "exercise"])

    # PRs (best est 1RM per exercise)
    # Epley: 1RM ≈ w * (1 + reps/30)
    work = out.dropna(subset=["weight_kg", "reps"]).copy()
    work["est_1rm"] = work["weight_kg"] * (1 + work["reps"] / 30.0)
    prs = work.groupby("exercise", as_index=False).agg(
        best_1rm_kg=("est_1rm", "max"),
        best_weight_kg=("weight_kg", "max"),
        best_reps=("reps", "max"),
    ).sort_values("best_1rm_kg", ascending=False)

    return {"raw": out, "by_day": by_day, "by_exercise": by_exercise, "prs": prs}
