import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

DATA_PATH = Path("data/strong.csv")

df = pd.read_csv(DATA_PATH, engine="python", on_bad_lines="skip")
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df = df.dropna(subset=['Date']).sort_values('Date').reset_index(drop=True)

for col in ['Weight','Reps','Distance','Seconds','RPE','Set Order']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

def parse_duration_to_minutes(s):
    if pd.isna(s): return np.nan
    s = str(s)
    mins = 0
    import re
    h = re.search(r"(\d+)\s*h", s)
    m = re.search(r"(\d+)\s*min", s)
    if h: mins += int(h.group(1)) * 60
    if m: mins += int(m.group(1))
    if mins == 0:
        try: return float(s)
        except: return np.nan
    return mins

if 'Duration' in df.columns:
    df['Duration_min'] = df['Duration'].apply(parse_duration_to_minutes)

df['Set_Volume'] = (df.get('Weight', 0).fillna(0) * df.get('Reps', 0).fillna(0)).replace(0, np.nan)
df['DateOnly'] = df['Date'].dt.date

session = df.groupby(['DateOnly']).agg(
    total_sets=('Set Order','count'),
    total_reps=('Reps','sum'),
    total_volume=('Set_Volume','sum'),
    duration_min=('Duration_min','max')
).reset_index()

weekly_volume = session.groupby(pd.to_datetime(session['DateOnly']).to_period('W').apply(lambda p: p.start_time.date()))['total_volume'].sum(min_count=1)

plt.figure()
weekly_volume.plot(title="Weekly Training Volume")
plt.xlabel("Week")
plt.ylabel("Total Volume (Weight x Reps)")
plt.show()
