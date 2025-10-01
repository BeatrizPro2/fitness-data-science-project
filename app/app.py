import sys
from pathlib import Path
import pandas as pd
import streamlit as st
import tempfile, os, datetime
import xml.etree.ElementTree as ET

# ---- Make src/ importable BEFORE importing your modules ----
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from strong_loader import load_strong_csv
from fitdays import save_fitdays_images

# --- Page + streamlit config ---
st.set_page_config(page_title="Fitness Data Science Dashboard", layout="wide")


# --- Page + streamlit config ---
st.set_page_config(page_title="Fitness Data Science Dashboard", layout="wide")

# --- Project paths & imports ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# optional trimmer (you added src/health_trim.py)
try:
    from health_trim import trim_export
except Exception:
    trim_export = None

# optional richer parser module (if/when you add it)
try:
    import health_parser as hp
except Exception:
    hp = None

# --- UI: header ---
st.title("🏋️ Fitness Data Science Dashboard")
st.caption("Upload your Apple Health export (XML) and Strong App CSV to visualize progress and get recommendations.")

colL, colR = st.columns([2, 1], gap="large")

with colR:
    st.subheader("Upload")
    xml_file = st.file_uploader("Apple Health `export.xml`", type=["xml"])
    strong_file = st.file_uploader("Strong App CSV (required)", type=["csv"])
    fitdays_images = st.file_uploader(
        "Fitdays screenshots (optional, jpg/png/webp)",
        type=["png", "jpg", "jpeg", "webp"],
        accept_multiple_files=True
    )

    st.subheader("Options")
    do_trim = st.checkbox("Trim Apple Health to last 12 months", value=True)
    since_days = st.number_input("Days to keep", min_value=7, max_value=3650, value=365, step=30)
    keep_workouts = st.checkbox("Include workouts", value=True)
    types_to_keep = st.multiselect(
        "Record types to keep (for trimming)",
        options=[
            "body_mass", "heart_rate", "step_count",
            "distance_walking_running", "active_energy",
            "basal_energy", "body_fat_pct", "lean_mass", "vo2max"
        ],
        default=["body_mass", "heart_rate", "step_count", "distance_walking_running", "active_energy"]
    )

    parse_btn = st.button("Parse & Build Dataset")

with colL:
    st.subheader("Status")
    st.write("• If `data/processed/health_combined_daily.csv` exists, it will be loaded automatically.")
    st.write("• Otherwise, upload files on the right and click **Parse & Build Dataset**.")

# --- Helpers ---
def load_processed(root: Path):
    p = root / "data" / "processed" / "health_combined_daily.csv"
    if p.exists():
        df = pd.read_csv(p, parse_dates=["date"])
        return df
    return None

def basic_weight_from_xml(xml_bytes: bytes):
    """
    Minimal fallback parser: pull only BodyMass records, produce date + weight_lb.
    """
    try:
        root = ET.fromstring(xml_bytes)
        rows = []
        for rec in root.iter("Record"):
            t = rec.attrib.get("type", "")
            if t.endswith("BodyMass"):
                val = rec.attrib.get("value")
                unit = (rec.attrib.get("unit") or "").lower()
                ts = rec.attrib.get("creationDate") or rec.attrib.get("startDate") or rec.attrib.get("endDate")
                if not (val and ts):
                    continue
                try:
                    v = float(val)
                    if unit == "kg":
                        v *= 2.20462
                except:
                    continue
                rows.append({"date": ts[:10], "weight_lb": v})
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        df = df.groupby("date", as_index=False)["weight_lb"].mean().sort_values("date")
        return df
    except Exception as e:
        st.error(f"Could not parse XML (fallback): {e}")
        return None

processed_dir = PROJECT_ROOT / "data" / "processed"
df = load_processed(PROJECT_ROOT)

# --- Button handler ---
if parse_btn:
    if xml_file is None:
        st.error("Please upload Apple Health `export.xml`.")
    elif strong_file is None:
        st.error("Please upload your Strong App CSV file.")
    else:
        # Save uploaded XML to a temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp:
            tmp.write(xml_file.getbuffer())
            tmp_xml_path = tmp.name

        out_stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = PROJECT_ROOT / "data" / "processed" / f"apple_trim_{out_stamp}"
        out_dir.mkdir(parents=True, exist_ok=True)

        try:
            if do_trim and trim_export is not None:
                with st.status("Trimming Apple Health export…", expanded=False):
                    result = trim_export(
                        xml_path=tmp_xml_path,
                        out_dir=out_dir,
                        since_days=int(since_days),
                        record_types=types_to_keep or None,
                        keep_workouts=bool(keep_workouts),
                    )
                st.success(f"Trim complete. Files written: {list(result.values())}")
                # Load records.csv if present, else fall back to weight-only parse from original XML
                rec_csv = out_dir / "records.csv"
                if rec_csv.exists():
                    df = pd.read_csv(rec_csv)
                    # Basic pivot to daily weight if present
                    if "type" in df.columns and "value" in df.columns:
                        # filter to BodyMass
                        mask = df["type"].str.endswith("BodyMass")
                        dfx = df.loc[mask, ["start_utc", "value", "unit"]].copy()
                        if not dfx.empty:
                            dfx["date"] = pd.to_datetime(dfx["start_utc"]).dt.date
                            dfx["value"] = pd.to_numeric(dfx["value"], errors="coerce")
                            # convert kg->lb if needed
                            dfx.loc[dfx["unit"].str.lower().eq("kg"), "value"] *= 2.20462
                            daily = dfx.groupby("date", as_index=False)["value"].mean()
                            daily.rename(columns={"value": "weight_lb"}, inplace=True)
                            daily["date"] = pd.to_datetime(daily["date"])
                            df = daily.sort_values("date")
                        else:
                            df = None
                else:
                    df = basic_weight_from_xml(xml_file.read())
            else:
                # no trimmer – parse minimally from full XML
                df = basic_weight_from_xml(xml_file.read())

            # Save standard output if df exists
            if df is not None and not df.empty:
                processed_dir.mkdir(parents=True, exist_ok=True)
                df.to_csv(processed_dir / "health_combined_daily.csv", index=False)
                st.success("Processed dataset saved to data/processed/health_combined_daily.csv")
                # ---- Strong CSV: load and persist summaries
                try:
                    st.write("Parsing Strong CSV…")
                    strong = load_strong_csv(strong_file)
                    strong_by_day = strong["by_day"]
                    strong_by_day.to_csv(processed_dir / "strong_by_day.csv", index=False)
                    strong["by_exercise"].to_csv(processed_dir / "strong_by_exercise.csv", index=False)
                    strong["prs"].to_csv(processed_dir / "strong_prs.csv", index=False)
                    st.success("Saved Strong summaries to data/processed/")
                except Exception as e:
                    st.warning(f"Strong CSV parsing failed: {e}")
                    strong_by_day = None

                # ---- Fitdays screenshots: save for reference
                saved_fitdays = []
                if fitdays_images:
                    fitdays_dir = processed_dir / "fitdays_screens"
                    saved_fitdays = save_fitdays_images(fitdays_images, fitdays_dir)
                    st.success(f"Saved {len(saved_fitdays)} Fitdays screenshot(s) to {fitdays_dir}")

                # ---- Combined daily dataset (Apple + Strong)
                combined = df.copy() if df is not None else pd.DataFrame()
                if not combined.empty:
                    combined = combined[["date", "weight_lb"]].copy()

                if 'strong_by_day' in locals() and strong_by_day is not None and not strong_by_day.empty:
                    s = strong_by_day.copy()
                    s["volume_lb"] = s["volume_kg"] * 2.20462
                    # keep sets, reps, exercises, duration
                    keep_cols = ["date", "volume_kg", "volume_lb", "sets", "reps", "exercises", "duration_min"]
                    s = s[keep_cols]
                    combined = s if combined.empty else combined.merge(s, on="date", how="outer")

                
                if not combined.empty:
                    combined = combined.sort_values("date")
                    combined.to_csv(processed_dir / "daily_combined.csv", index=False)
                    st.success("Saved combined daily dataset to data/processed/daily_combined.csv")

            else:
                st.warning("No weight records found. Try adjusting trim options or upload a different export.")

        finally:
            try:
                os.remove(tmp_xml_path)
            except Exception:
                pass

# --- If we have data, show visuals ---
if df is None or df.empty:
    st.info("No processed dataset found yet. Upload files and click **Parse & Build Dataset**.")
    st.stop()

# Try to load Strong/combined outputs if they exist
strong_by_day_path   = processed_dir / "strong_by_day.csv"
strong_by_ex_path    = processed_dir / "strong_by_exercise.csv"
strong_prs_path      = processed_dir / "strong_prs.csv"
daily_combined_path  = processed_dir / "daily_combined.csv"

strong_by_day  = pd.read_csv(strong_by_day_path, parse_dates=["date"]) if strong_by_day_path.exists() else None
strong_by_ex   = pd.read_csv(strong_by_ex_path,  parse_dates=["date"]) if strong_by_ex_path.exists() else None
strong_prs     = pd.read_csv(strong_prs_path) if strong_prs_path.exists() else None
daily_combined = pd.read_csv(daily_combined_path, parse_dates=["date"]) if daily_combined_path.exists() else None

df = df.copy()
if "date" in df.columns:
    df = df.sort_values("date")
if "weight_lb" in df.columns:
    df["weight_7d_ma"] = df["weight_lb"].rolling(7, min_periods=1).mean()
    if len(df) >= 8:
        df["weight_week_delta"] = df["weight_lb"].diff(7)

tab1, tab2, tab3 = st.tabs(["📈 Trends", "🧠 Insights", "📝 Recommendations"])

with tab1:
    import plotly.express as px
    if "weight_lb" in df.columns:
        st.subheader("Weight")
        fig = px.line(df, x="date", y=["weight_lb", "weight_7d_ma"], markers=True)
        st.plotly_chart(fig, use_container_width=True)
        # --- Strong daily totals ---
    if strong_by_day is not None and not strong_by_day.empty:
        st.subheader("Training Volume (kg)")
        st.plotly_chart(px.bar(strong_by_day, x="date", y="volume_kg"), use_container_width=True)

        if "duration_min" in strong_by_day.columns and strong_by_day["duration_min"].notna().any():
            st.subheader("Workout Duration (min)")
            st.plotly_chart(px.bar(strong_by_day, x="date", y="duration_min"), use_container_width=True)

        # --- Combined: Weight vs Volume (scaled overlay) ---
    if (daily_combined is not None and
        {"date","weight_lb","volume_kg"}.issubset(daily_combined.columns) and
        not daily_combined.empty):
        st.subheader("Weight vs Training Volume (overlay)")
        combo = daily_combined.copy()
        m = max(combo["volume_kg"].max(), 1)
        combo["volume_scaled"] = combo["volume_kg"] / m * combo["weight_lb"].max()
        fig2 = px.line(combo, x="date", y=["weight_lb","volume_scaled"], markers=True)
        fig2.update_layout(legend_title_text="Series (volume scaled)")
        st.plotly_chart(fig2, use_container_width=True)


with tab2:
    st.subheader("Progress Snapshot")
    cols = st.columns(3)
    def latest_change(colname):
        if colname not in df.columns or len(df) < 8:
            return None
        return float(df[colname].iloc[-1] - df[colname].iloc[-8])

    if fitdays_images:
        st.divider()
        st.subheader("Fitdays screenshots (reference)")
        for img in fitdays_images:
            st.image(img, use_container_width=True, caption=img.name)

    if "weight_lb" in df.columns:
        delta = latest_change("weight_lb")
        cols[0].metric("Weight (lb)", f"{df['weight_lb'].iloc[-1]:.1f}", None if delta is None else f"{delta:+.1f} vs 7d ago")

    st.divider()
    st.write("Recent rows:")
    st.dataframe(df.tail(14), use_container_width=True)
    # --- Strong PRs & recent exercises ---
    if strong_prs is not None and not strong_prs.empty:
        st.subheader("Top Estimated 1RMs (kg)")
        st.dataframe(strong_prs.head(20), use_container_width=True)

    if strong_by_ex is not None and not strong_by_ex.empty:
        st.subheader("Recent Exercises")
        st.dataframe(strong_by_ex.sort_values("date").tail(30), use_container_width=True)

with tab3:
    st.subheader("Rule-based Suggestions")
    tips = []
    if "weight_lb" in df.columns and len(df) >= 15:
        wk_delta = float(df["weight_lb"].iloc[-1] - df["weight_lb"].iloc[-8])
        if wk_delta > 0.5:
            tips.append("Weight trending up ~past week—consider a small calorie deficit (−200 to −300 kcal/day) and 1–2 cardio sessions/week.")
        elif wk_delta < -0.5:
            tips.append("Nice downward trend—keep protein ≥1.6 g/kg and continue current training.")
    if not tips:
        tips.append("Upload more days of data to unlock personalized suggestions.")
    for t in tips:
        st.write("• " + t)

st.success(f"Loaded dataset with {len(df)} daily rows.")
