
# Fitness Data Science App (Streamlit)

## Quick start
```bash
# from the repo root (where this README sits)
python -m venv .venv && source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r app/requirements.txt

# Run the app
streamlit run app/app.py
```

## Data inputs
- Place your **Apple Health** `export.xml` at `data/export.xml`, or upload via the app.
- Optional: add your **Fitdays** CSV at `data/fitdays.csv`.

Processed outputs will be saved under `data/processed/`.
