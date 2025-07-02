import pandas as pd
from transformers import pipeline
from tqdm import tqdm
import os

# === CONFIG ===
INPUT_CSV = "dataset_with_lastfm_tags.csv"
OUTPUT_DIR = "mood_activity_batches"
OUTPUT_FINAL = "dataset_mood_activity.csv"

BATCH_SIZE = 1000

MOOD_KEYWORDS = [
    "happy", "sad", "chill", "romantic", "calm", "melancholic"
]

ACTIVITY_KEYWORDS = [
    "party", "workout", "study", "sleep", "driving", "cooking", "relax", "cleaning"
]

ERA_KEYWORDS = ["60s", "70s", "80s", "90s", "2000s", "2010s", "2020s"]

# === Init sentiment pipeline ===
print("Chargement modèle sentiment...")
sentiment_pipeline = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")

# === Charger dataset ===
df = pd.read_csv(INPUT_CSV)

# Colonnes à créer si absentes
for col in ["mood", "activity", "era"]:
    if col not in df.columns:
        df[col] = ""

# Créer dossier batches
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Boucle Batch ===
for batch_num, start_idx in enumerate(range(0, len(df), BATCH_SIZE)):
    end_idx = min(start_idx + BATCH_SIZE, len(df))
    batch = df.iloc[start_idx:end_idx].copy()

    print(f"Traitement batch {batch_num} : lignes {start_idx} à {end_idx}")

    for idx, row in tqdm(batch.iterrows(), total=len(batch), desc=f"Batch {batch_num}"):

        # === SKIP auto ===
        if isinstance(row["mood"], str) and len(row["mood"]) > 0 and \
           isinstance(row["activity"], str) and len(row["activity"]) > 0:
            continue  # Déjà enrichi ➜ on saute

        tags = row.get("lastfm_tags", "")
        mood = ""
        activity = ""
        era = ""

        if isinstance(tags, str) and tags.strip():
            tags_list = [tag.strip().lower() for tag in tags.split(",")]

            for m in MOOD_KEYWORDS:
                if m in tags_list:
                    mood = m
                    break

            for a in ACTIVITY_KEYWORDS:
                if a in tags_list:
                    activity = a
                    break

            for e in ERA_KEYWORDS:
                if e in tags_list:
                    era = e
                    break

        # Fallback sentiment pour mood
        if not mood:
            lyrics = row.get("lyrics", "")
            if isinstance(lyrics, str) and len(lyrics) > 20:
                try:
                    result = sentiment_pipeline(lyrics[:512])[0]["label"]
                    if result in ["LABEL_1", "LABEL_2"]:
                        mood = "sad"
                    elif result in ["LABEL_4", "LABEL_5"]:
                        mood = "happy"
                    else:
                        mood = "calm"
                except Exception as e:
                    print(f"HF fallback erreur idx {idx} : {e}")

        batch.at[idx, "mood"] = mood
        batch.at[idx, "activity"] = activity
        batch.at[idx, "era"] = era

    # Sauvegarde Batch
    batch_file = os.path.join(OUTPUT_DIR, f"mood_activity_batch_{batch_num:04d}.csv")
    batch.to_csv(batch_file, index=False)
    print(f"Batch {batch_num} sauvegardé → {batch_file}")

print("Tous les lots traités ! Fusion finale...")

# === Fusion Finale ===
import glob

batch_files = glob.glob(f"{OUTPUT_DIR}/mood_activity_batch_*.csv")
dfs = [pd.read_csv(f) for f in batch_files]
final_df = pd.concat(dfs, ignore_index=True)
final_df.to_csv(OUTPUT_FINAL, index=False)

print(f"Dataset final enrichi → {OUTPUT_FINAL}")
