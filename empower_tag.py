import pandas as pd

INPUT_CSV = "dataset_with_lyrics.csv"
OUTPUT_CSV = "dataset_with_empowerment.csv"

# === Mots-clés empowerment ===
KEYWORDS = [
    "girl", "woman", "queen", "ladies", "feminist",
    "strong", "power", "warrior", "anthem",
    "unstoppable", "rise", "fight", "victory", "independent",
    "diva", "baddie", "boss", "superwoman", "empower", "courage",
    "brave", "fearless", "confident", "survivor", "resilient"
]

df = pd.read_csv(INPUT_CSV)

df["empowerment"] = False
df["empowerment_hits"] = 0

for idx, row in df.iterrows():
    lyrics = str(row.get("lyrics", "")).lower()
    hits = sum(1 for kw in KEYWORDS if kw in lyrics)

    if hits > 0:
        df.at[idx, "empowerment"] = True
        df.at[idx, "empowerment_hits"] = hits

df.to_csv(OUTPUT_CSV, index=False)
print(f"Fichier enrichi avec score empowerment → {OUTPUT_CSV}")
