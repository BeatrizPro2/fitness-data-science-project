# Fitness Tracker (Strong App) — VS Code Starter

This project analyzes your Strong app workout data.

## Quickstart (VS Code)
1. **Open Folder** → select this folder in VS Code.
2. **Create venv** (recommended):
   - Windows PowerShell:
     ```powershell
     python -m venv .venv
     .\.venv\Scripts\Activate
     ```
   - macOS/Linux:
     ```bash
     python3 -m venv .venv
     source .venv/bin/activate
     ```
3. **Install deps**:
   ```bash
   pip install -r requirements.txt
   ```
4. Open **`notebooks/strong_analysis.ipynb`** and run cells top-to-bottom.

## Data
- Place your Strong export at `data/strong.csv` (already included if you uploaded it here).
- The notebook handles common CSV quirks with Strong exports.

## What you'll get
- Weekly training volume and frequency.
- Top exercises by lifetime volume.
- Personal records (heaviest set per exercise).
- Cleaned dataset saved to `data/cleaned_strong.csv`.
