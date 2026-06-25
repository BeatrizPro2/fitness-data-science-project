"""
Fitness Data Science Dashboard (Streamlit)
 
Two inputs only:
  - Strong app workout CSV  -> training volume, frequency, PRs
  - Fitdays screenshot(s)   -> weight + body-composition breakdown (via OCR)
 
Pick a goal and the app shows your trends, a body-composition snapshot,
and a short "what's going well / what to focus on" summary.
"""
import sys
from pathlib import Path
 
import pandas as pd
import streamlit as st
import plotly.express as px
 
# ---- make src/ importable -------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
 
from strong_loader import load_strong_csv
from fitdays import save_fitdays_images, parse_fitdays_images
from recommendations import generate_recommendations, GOAL_LABELS
 
st.set_page_config(page_title="Fitness Dashboard", layout="wide")
PROCESSED = PROJECT_ROOT / "data" / "processed"
 
# Friendly names + units for the body-composition snapshot table
DISPLAY = {
    "weight_lb": ("Weight", "lb"),
    "bmi": ("BMI", ""),
    "body_fat_pct": ("Body fat", "%"),
    "fat_mass_lb": ("Fat mass", "lb"),
    "fat_free_weight_lb": ("Fat-free weight", "lb"),
    "muscle_mass_lb": ("Muscle mass", "lb"),
    "muscle_rate_pct": ("Muscle rate", "%"),
    "skeletal_muscle_pct": ("Skeletal muscle", "%"),
    "bone_mass_lb": ("Bone mass", "lb"),
    "protein_mass_lb": ("Protein mass", "lb"),
    "protein_pct": ("Protein", "%"),
    "water_weight_lb": ("Water weight", "lb"),
    "body_water_pct": ("Body water", "%"),
    "subcutaneous_fat_pct": ("Subcutaneous fat", "%"),
    "visceral_fat": ("Visceral fat", ""),
    "bmr_kcal": ("BMR", "kcal"),
    "body_age": ("Body age", ""),
    "ideal_weight_lb": ("Ideal weight", "lb"),
}
 
 
# ---- header ---------------------------------------------------------------
st.title("🏋️ Fitness Dashboard")
st.caption("Track your training and body composition from your Strong CSV and Fitdays screenshots.")
 
# ---- sidebar: inputs ------------------------------------------------------
with st.sidebar:
    st.header("Your data")
    goal_key = st.selectbox(
        "Your goal",
        options=list(GOAL_LABELS.keys()),
        format_func=lambda k: GOAL_LABELS[k],
    )
    strong_file = st.file_uploader("Strong app CSV", type=["csv"])
    fitdays_images = st.file_uploader(
        "Fitdays screenshot(s)",
        type=["png", "jpg", "jpeg", "webp"],
        accept_multiple_files=True,
    )
    run = st.button("Build dashboard", type="primary")
 
# ---- processing (on button) ----------------------------------------------
if run:
    if strong_file is None and not fitdays_images:
        st.warning("Upload your Strong CSV and/or a Fitdays screenshot to begin.")
    else:
        PROCESSED.mkdir(parents=True, exist_ok=True)
 
        # Strong CSV -> training summaries
        strong = None
        if strong_file is not None:
            try:
                strong = load_strong_csv(strong_file)
                strong["by_day"].to_csv(PROCESSED / "strong_by_day.csv", index=False)
                strong["by_exercise"].to_csv(PROCESSED / "strong_by_exercise.csv", index=False)
                strong["prs"].to_csv(PROCESSED / "strong_prs.csv", index=False)
            except Exception as e:
                st.error(f"Could not parse the Strong CSV: {e}")
 
        # Fitdays screenshots -> body composition (OCR)
        fitdays_df = pd.DataFrame()
        if fitdays_images:
            with st.status("Reading Fitdays screenshot(s)...", expanded=False):
                paths = save_fitdays_images(fitdays_images, PROCESSED / "fitdays_screens")
                fitdays_df = parse_fitdays_images(paths)
            if fitdays_df.empty:
                st.warning("Couldn't read any numbers from the screenshot(s). "
                           "Make sure Tesseract is installed and the image is a clear Fitdays results screen.")
            else:
                fitdays_df.to_csv(PROCESSED / "fitdays_body.csv", index=False)
 
        # stash for rendering / goal switching without re-uploading
        st.session_state["strong"] = strong
        st.session_state["fitdays"] = fitdays_df
        st.session_state["fitdays_images"] = fitdays_images
 
# ---- render ---------------------------------------------------------------
strong = st.session_state.get("strong")
fitdays_df = st.session_state.get("fitdays", pd.DataFrame())
fitdays_images = st.session_state.get("fitdays_images")
 
if strong is None and (fitdays_df is None or fitdays_df.empty):
    st.info("Pick your goal, upload your Strong CSV and Fitdays screenshot(s), then click **Build dashboard**.")
    st.stop()
 
tab_dash, tab_advice = st.tabs(["Dashboard", "Strengths & Tips"])
 
with tab_dash:
    # ----- Training (Strong) -----
    if strong is not None:
        by_day = strong["by_day"]
        prs = strong["prs"]
 
        st.subheader("Training")
        c1, c2, c3 = st.columns(3)
        c1.metric("Training days", len(by_day))
        if "volume_kg" in by_day.columns:
            c2.metric("Total volume (kg)", f"{by_day['volume_kg'].sum():,.0f}")
        if prs is not None and not prs.empty:
            c3.metric("Exercises tracked", len(prs))
 
        if "volume_kg" in by_day.columns and not by_day.empty:
            st.plotly_chart(
                px.bar(by_day, x="date", y="volume_kg", title="Training volume by day (kg)"),
                use_container_width=True,
            )
        if prs is not None and not prs.empty:
            st.markdown("**Top estimated 1RMs (kg)**")
            st.dataframe(prs.head(15), use_container_width=True, hide_index=True)
 
    # ----- Body composition (Fitdays) -----
    if fitdays_df is not None and not fitdays_df.empty:
        st.subheader("Body composition")
        latest = fitdays_df.iloc[-1]
 
        cols = st.columns(4)
        for col, key in zip(cols, ["weight_lb", "body_fat_pct", "muscle_mass_lb", "bmi"]):
            if key in latest and pd.notna(latest[key]):
                name, unit = DISPLAY.get(key, (key, ""))
                col.metric(name, f"{latest[key]:g}{unit}")
 
        # Trends over time if there is more than one screenshot
        if len(fitdays_df) >= 2:
            for key, label in [("weight_lb", "Weight (lb)"),
                               ("body_fat_pct", "Body fat (%)"),
                               ("muscle_mass_lb", "Muscle mass (lb)")]:
                if key in fitdays_df.columns and fitdays_df[key].notna().sum() >= 2:
                    st.plotly_chart(
                        px.line(fitdays_df, x="date", y=key, markers=True, title=label),
                        use_container_width=True,
                    )
 
        # Full snapshot table with Fitdays ratings
        rows = []
        for key, (name, unit) in DISPLAY.items():
            if key in latest and pd.notna(latest[key]):
                std = latest.get(f"{key}_standard", "")
                rows.append({"Metric": name,
                             "Value": f"{latest[key]:g}{unit}",
                             "Rating": std if isinstance(std, str) else ""})
        if rows:
            st.markdown("**Latest Fitdays snapshot**")
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
 
        if fitdays_images:
            with st.expander("View uploaded screenshot(s)"):
                for img in fitdays_images:
                    st.image(img, caption=getattr(img, "name", ""), use_container_width=True)
 
with tab_advice:
    st.subheader(f"Goal: {GOAL_LABELS.get(goal_key, goal_key)}")
    rec = generate_recommendations(fitdays_df, strong, goal_key)
 
    st.markdown("### What's going well")
    for s in rec["strengths"]:
        st.markdown(f"- {s}")
 
    st.markdown("### What to focus on")
    for t in rec["tips"]:
        st.markdown(f"- {t}")
 
    st.caption("General guidance based on your data - not medical or nutritional advice.")