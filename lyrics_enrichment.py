import pandas as pd
import lyricsgenius
import dotenv
import os
import time
from tqdm import tqdm  
import glob


dotenv.load_dotenv()

# === CONFIG ===
INPUT_CSV = "dataset_avec_genres_ml.csv"
OUTPUT_DIR = "lyrics_batches"
GENIUS_API_TOKEN = os.getenv("GENIUS_API_KEY")
BATCH_SIZE = 500
PAUSE_SECONDS = 1


# Load dataset
df = pd.read_csv(INPUT_CSV)

# Init Genius API
genius = lyricsgenius.Genius(GENIUS_API_TOKEN, timeout=15, retries=3)

# Crée dossier batches
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Ajoute colonne lyrics si manquante
if "lyrics" not in df.columns:
    df["lyrics"] = ""

# Détecte les batchs déjà traités
existing_batches = set()
for f in glob.glob(os.path.join(OUTPUT_DIR, "batch_*.csv")):
    try:
        num = int(os.path.basename(f).split('_')[1].split('.')[0])
        existing_batches.add(num)
    except Exception:
        pass

# Boucle par batch
for batch_num, start_idx in enumerate(range(0, len(df), BATCH_SIZE)):
    if batch_num in existing_batches:
        continue  # Batch déjà traité, on saute
    end_idx = min(start_idx + BATCH_SIZE, len(df))
    batch = df.iloc[start_idx:end_idx].copy()

    print(f"Traitement du batch {batch_num} : lignes {start_idx} à {end_idx}")

    for idx, row in tqdm(batch.iterrows(), total=len(batch), desc=f"Batch {batch_num}"):
        if isinstance(row["lyrics"], str) and len(row["lyrics"]) > 5:
            continue  # Déjà enrichi

        artist = row["artist"]
        title = row["track_name"]

        try:
            song = genius.search_song(title, artist)
            if song and song.lyrics:
                batch.at[idx, "lyrics"] = song.lyrics
            else:
                batch.at[idx, "lyrics"] = ""
        except Exception as e:
            print(f"Erreur pour {title} - {artist}: {e}")
            batch.at[idx, "lyrics"] = ""

        time.sleep(PAUSE_SECONDS)

    # Sauvegarde du batch
    batch_file = os.path.join(OUTPUT_DIR, f"batch_{batch_num:04d}.csv")
    batch.to_csv(batch_file, index=False)
    print(f" Batch {batch_num} sauvegardé : {batch_file}")

print("Tous les lots sont traités !")

# === Fusion finale ===
batch_files = glob.glob("lyrics_batches/batch_*.csv")

dfs = [pd.read_csv(f) for f in batch_files]
final_df = pd.concat(dfs, ignore_index=True)

# Sauvegarde unique finale
final_df.to_csv("dataset_with_lyrics.csv", index=False)
print("Dataset final enrichi sauvegardé sous : dataset_with_lyrics.csv")
