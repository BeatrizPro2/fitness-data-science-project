"""
Fitness recommendations.

Primary: AI-generated, personalized recommendations via a local Ollama model.
Fallback: rule-based tips if Ollama is unavailable, so the app never breaks.
"""
from __future__ import annotations
import json
import pandas as pd

try:
    import requests
except ImportError:
    requests = None

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "qwen2.5:3b"

GOAL_LABELS: dict[str, str] = {
    "lose_weight": "Lose Weight",
    "build_muscle": "Build Muscle",
    "maintain": "Maintain",
    "improve_endurance": "Improve Endurance",
}


def _build_data_summary(strong: dict | None, fitdays_df: pd.DataFrame | None, goal: str | None) -> str:
    """Detailed text summary of the user's real data for the model."""
    lines = [f"User's goal: {GOAL_LABELS.get(goal, goal or 'not set')}"]

    by_day = strong.get("by_day") if isinstance(strong, dict) else None
    if by_day is not None and not by_day.empty:
        lines.append(f"Total training days logged: {len(by_day)}")
        if len(by_day) >= 14:
            recent = by_day["volume_kg"].tail(7).mean()
            prior = by_day["volume_kg"].iloc[-14:-7].mean()
            pct = (recent - prior) / prior * 100 if prior else 0
            lines.append(f"Recent 7-day avg volume: {recent:.0f} kg ({pct:+.0f}% vs prior week)")

    prs = strong.get("prs") if isinstance(strong, dict) else None
    if prs is not None and not prs.empty:
        raw = strong.get("raw")
        if raw is not None and "exercise" in raw.columns:
            freq = raw.groupby("exercise")["date"].nunique().sort_values(ascending=False)
            merged = prs.set_index("exercise")
            lines.append("Most-trained lifts (exercise: days trained, best weight):")
            for ex in freq.head(6).index:
                if ex in merged.index:
                    w = merged.loc[ex, "best_weight_kg"]
                    if pd.notna(w) and w > 0:
                        lines.append(f"  - {ex}: {int(freq[ex])} days, best {w:.0f} kg")
            rare = freq[freq <= 2]
            if len(rare):
                lines.append(f"Rarely trained (<=2 days): {', '.join(rare.head(5).index)}")

    raw = strong.get("raw") if isinstance(strong, dict) else None
    if raw is not None and "distance_m" in raw.columns:
        dist = pd.to_numeric(raw["distance_m"], errors="coerce").sum()
        if dist and dist > 0:
            lines.append(f"Total cardio distance: {dist:.1f} km")

    if fitdays_df is not None and not fitdays_df.empty:
        latest = fitdays_df.iloc[-1]
        for col, label in [("weight_lb", "weight (lb)"), ("body_fat_pct", "body fat %"),
                           ("muscle_mass_lb", "muscle mass (lb)")]:
            if col in fitdays_df.columns and pd.notna(latest.get(col)):
                lines.append(f"Latest {label}: {latest[col]}")

    return "\n".join(lines)


def generate_ai_recommendations(strong, fitdays_df, goal, user_prefs="", model=OLLAMA_MODEL):
    if requests is None:
        raise RuntimeError("requests not installed")
    summary = _build_data_summary(strong, fitdays_df, goal)
    prefs_block = f"\nIMPORTANT - user preferences (respect these): {user_prefs}\n" if user_prefs.strip() else ""
    prompt = (
        "You are a knowledgeable, encouraging strength coach analyzing a client's real training data. "
        "Give specific, personalized feedback that cites their actual numbers and exercise names.\n\n"
        f"{summary}\n"
        f"{prefs_block}\n"
        "Rules for your response:\n"
        "- Recommend MOVEMENT PATTERNS or muscle groups, not specific machines the user may dislike "
        '(e.g. say "add a hamstring exercise" rather than naming a specific machine).\n'
        "- Honor the user preferences above when suggesting exercises.\n"
        "- Reference SPECIFIC exercises and numbers from the data.\n"
        "- If some muscle groups or lifts are rarely trained, point that out.\n"
        "- Tie every tip to their goal.\n"
        "- Avoid generic advice without naming which lift or movement.\n\n"
        "Respond with a JSON object with exactly two keys:\n"
        '  "strengths": 5-6 strings on what they are doing well (cite specifics)\n'
        '  "tips": 5-6 specific, actionable strings (cite specifics, respect preferences)\n'
        "Each string under 35 words. No medical advice. Return only the JSON."
    )
    resp = requests.post(
        OLLAMA_URL,
        json={"model": model, "prompt": prompt, "stream": False, "format": "json"},
        timeout=120,
    )
    resp.raise_for_status()
    data = json.loads(resp.json()["response"])
    def _to_text(item):
            # Model sometimes returns {"text": "...", "citations": "..."} instead of a plain string
            if isinstance(item, dict):
                txt = item.get("text") or item.get("tip") or item.get("strength") or ""
                cite = item.get("citations") or item.get("citation") or ""
                if cite and cite not in ("Not in data", "", "N/A"):
                    return f"{txt} ({cite})"
                return txt
            return str(item)

    strengths = [_to_text(s) for s in data.get("strengths", [])]
    tips = [_to_text(t) for t in data.get("tips", [])]
    if not strengths and not tips:
        raise ValueError("Empty AI response")
    return {
        "strengths": strengths or ["Keep logging — more data unlocks better feedback."],
        "tips": tips or ["Pick a goal to get focused suggestions."],
    }



def generate_recommendations(fitdays_df=None, strong=None, goal=None, user_prefs=""):
    """AI-first; fall back to rule-based tips if Ollama is unavailable."""
    if requests is not None and isinstance(strong, dict):
        try:
            return generate_ai_recommendations(strong, fitdays_df, goal, user_prefs)
        except Exception:
            pass
    return _rule_based(fitdays_df, strong, goal)


def _rule_based(fitdays_df=None, strong=None, goal=None):
    strengths, tips = [], []
    by_day = strong.get("by_day") if isinstance(strong, dict) else None
    volume_trend = None
    if by_day is not None and not by_day.empty and len(by_day) >= 14:
        recent = by_day["volume_kg"].tail(7).mean()
        prior = by_day["volume_kg"].iloc[-14:-7].mean()
        if prior > 0:
            volume_trend = "down" if recent < prior*0.7 else "up" if recent > prior*1.5 else "steady"