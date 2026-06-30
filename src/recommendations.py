"""
Rule-based fitness recommendations.

Generates personalized suggestions based on weight trends, training volume,
and the user's selected goal. Pure logic — no Streamlit dependency.
"""
from __future__ import annotations
import pandas as pd


GOAL_LABELS: dict[str, str] = {
    "lose_weight": "Lose Weight",
    "build_muscle": "Build Muscle",
    "maintain": "Maintain",
    "improve_endurance": "Improve Endurance",
}


def generate_recommendations(
    fitdays_df: pd.DataFrame | None = None,
    strong: dict | None = None,
    goal: str | None = None,
) -> dict[str, list[str]]:
    """
    Goal-aware, rule-based fitness recommendations.

    Returns a dict with two lists:
        - "strengths": things the user is doing well
        - "tips":      things to focus on / improve
    """
    strengths: list[str] = []
    tips: list[str] = []

    by_day = strong.get("by_day") if isinstance(strong, dict) else None

    # Weight trend from Fitdays body-comp data (if present)
    weight_delta = None
    if fitdays_df is not None and not fitdays_df.empty:
        wcol = next((c for c in fitdays_df.columns if "weight" in c.lower()), None)
        if wcol:
            s = pd.to_numeric(fitdays_df[wcol], errors="coerce").dropna()
            if len(s) >= 2:
                weight_delta = float(s.iloc[-1] - s.iloc[0])

    # Training-volume trend from Strong data (needs ~2 weeks of history)
    volume_trend = None  # "down", "up", or "steady"
    if by_day is not None and not by_day.empty and len(by_day) >= 14:
        recent = by_day["volume_kg"].tail(7).mean()
        prior = by_day["volume_kg"].iloc[-14:-7].mean()
        if prior > 0:
            if recent < prior * 0.7:
                volume_trend = "down"
            elif recent > prior * 1.5:
                volume_trend = "up"
            else:
                volume_trend = "steady"

    # Generic strength: logging at all
    if by_day is not None and not by_day.empty:
        strengths.append(f"You've logged {len(by_day)} training day(s) — consistency is the foundation.")
    if volume_trend == "steady":
        strengths.append("Your training volume has been steady week to week — great consistency.")

    # Goal-specific guidance
    if goal == "lose_weight":
        tips.append(
            "Keep a modest deficit (~-300 to -500 kcal/day), protein >=1.6 g/kg "
            "to protect muscle, and aim for 7k-10k steps/day."
        )
        if weight_delta is not None and weight_delta < -0.5:
            strengths.append("Your weight is trending down — the deficit is working.")
        elif weight_delta is not None and weight_delta > 0.5:
            tips.append("Weight is drifting up — tighten the deficit and add 1-2 cardio sessions.")

    elif goal == "build_muscle":
        tips.append(
            "Eat in a slight surplus (~+150 to +300 kcal/day), protein >=1.6 g/kg, "
            "and add weight or reps to key lifts each week."
        )
        if weight_delta is not None and weight_delta > 0:
            strengths.append("Your weight is trending up — good for supporting muscle gain.")
        elif weight_delta is not None and weight_delta < 0:
            tips.append("Weight is flat or dropping — for muscle gain you likely need to eat more.")

    elif goal == "maintain":
        tips.append(
            "Keep calories around maintenance and training consistent. "
            "Small weekly weigh-in swings are normal."
        )
        if weight_delta is not None and abs(weight_delta) <= 1.0:
            strengths.append("Your weight is holding steady — maintenance is on track.")

    elif goal == "improve_endurance":
        tips.append(
            "Prioritize 3-4 cardio sessions/week and build duration gradually "
            "(~10%/week). Keep 1-2 lifting days to retain strength."
        )

    # Volume-based tips (apply to every goal)
    if volume_trend == "down":
        tips.append("Training volume dropped this past week — schedule 1-2 makeup sessions.")
    elif volume_trend == "up":
        tips.append("Training volume jumped recently — watch for fatigue and prioritize recovery.")

    # Fallbacks so neither list is ever empty (the app loops over both)
    if not strengths:
        strengths.append("Upload more days of data to surface what you're doing well.")
    if not tips:
        tips.append("Pick a goal and log more sessions to unlock personalized focus areas.")

    return {"strengths": strengths, "tips": tips}