"""
Rule-based fitness recommendations.

Generates personalized suggestions based on weight trends and (optionally)
training volume. Pure logic — no Streamlit dependency, so it stays testable.
"""
from __future__ import annotations
import pandas as pd

"""
Rule-based fitness recommendations.

Generates personalized suggestions based on weight trends and (optionally)
training volume. Pure logic — no Streamlit dependency, so it stays testable.
"""
GOAL_LABELS: dict[str, str] = {
    "lose_weight": "Lose Weight",
    "build_muscle": "Build Muscle",
    "maintain": "Maintain",
    "improve_endurance": "Improve Endurance",
}


def generate_recommendations(
    df: pd.DataFrame,
    strong_by_day: pd.DataFrame | None = None,
) -> list[str]:
    """
    Generate rule-based fitness recommendations.

    Args:
        df: Daily weight dataframe with 'date' and 'weight_lb' columns.
        strong_by_day: Optional daily training summary with 'volume_kg' column.

    Returns:
        List of recommendation strings to display to the user.
    """
    tips: list[str] = []

    # --- Weight trend recommendations ---
    if "weight_lb" in df.columns and len(df) >= 15:
        wk_delta = float(df["weight_lb"].iloc[-1] - df["weight_lb"].iloc[-8])
        if wk_delta > 0.5:
            tips.append(
                "Weight trending up ~past week — consider a small calorie deficit "
                "(−200 to −300 kcal/day) and 1–2 cardio sessions/week."
            )
        elif wk_delta < -0.5:
            tips.append(
                "Nice downward trend — keep protein ≥1.6 g/kg and continue current training."
            )

    # --- Training volume recommendations ---
    if strong_by_day is not None and not strong_by_day.empty and len(strong_by_day) >= 14:
        recent_vol = strong_by_day["volume_kg"].tail(7).mean()
        prior_vol = strong_by_day["volume_kg"].iloc[-14:-7].mean()
        if recent_vol < prior_vol * 0.7:
            tips.append(
                "Training volume has dropped notably this past week — consider scheduling "
                "1–2 makeup sessions or scaling intensity back gradually."
            )
        elif recent_vol > prior_vol * 1.5:
            tips.append(
                "Training volume jumped a lot recently — watch for fatigue and prioritize "
                "sleep and recovery this week."
            )

    # --- Default ---
    if not tips:
        tips.append("Upload more days of data to unlock personalized suggestions.")

    return tips


def generate_recommendations(
    df: pd.DataFrame,
    strong_by_day: pd.DataFrame | None = None,
) -> list[str]:
    """
    Generate rule-based fitness recommendations.

    Args:
        df: Daily weight dataframe with 'date' and 'weight_lb' columns.
        strong_by_day: Optional daily training summary with 'volume_kg' column.

    Returns:
        List of recommendation strings to display to the user.
    """
    tips: list[str] = []

    # --- Weight trend recommendations ---
    if "weight_lb" in df.columns and len(df) >= 15:
        wk_delta = float(df["weight_lb"].iloc[-1] - df["weight_lb"].iloc[-8])
        if wk_delta > 0.5:
            tips.append(
                "Weight trending up ~past week — consider a small calorie deficit "
                "(−200 to −300 kcal/day) and 1–2 cardio sessions/week."
            )
        elif wk_delta < -0.5:
            tips.append(
                "Nice downward trend — keep protein ≥1.6 g/kg and continue current training."
            )

    # --- Training volume recommendations ---
    if strong_by_day is not None and not strong_by_day.empty and len(strong_by_day) >= 14:
        recent_vol = strong_by_day["volume_kg"].tail(7).mean()
        prior_vol = strong_by_day["volume_kg"].iloc[-14:-7].mean()
        if recent_vol < prior_vol * 0.7:
            tips.append(
                "Training volume has dropped notably this past week — consider scheduling "
                "1–2 makeup sessions or scaling intensity back gradually."
            )
        elif recent_vol > prior_vol * 1.5:
            tips.append(
                "Training volume jumped a lot recently — watch for fatigue and prioritize "
                "sleep and recovery this week."
            )

    # --- Default ---
    if not tips:
        tips.append("Upload more days of data to unlock personalized suggestions.")

    return tips
