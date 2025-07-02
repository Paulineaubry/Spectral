import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import dotenv
import os
from tqdm import tqdm
import time

# === Charger .env ===
dotenv.load_dotenv()

SPOTIPY_CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")

if not SPOTIPY_CLIENT_ID or not SPOTIPY_CLIENT_SECRET:
    raise ValueError("Clé Spotify API manquante. Vérifie ton .env !")

# === Init Spotify ===
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials())

# === CONFIG ===
INPUT_CSV = "dataset_avec_genres_ml.csv"
OUTPUT_DIR = "album_batches"  # Correction du nom du dossier
OUTPUT_FINAL = "dataset_with_album_cover.csv"

BATCH_SIZE = 500
PAUSE_SECONDS = 0.5

# === Charger CSV ===
df = pd.read_csv(INPUT_CSV)

# Ajouter colonne si absente
if "album_cover_url" not in df.columns:
    df["album_cover_url"] = ""

# Créer dossier batches
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Détecte les batchs déjà traités
import glob
existing_batches = set()
for f in glob.glob(os.path.join(OUTPUT_DIR, "spotify_batch_*.csv")):
    try:
        num = int(os.path.basename(f).split('_')[2].split('.')[0])
        existing_batches.add(num)
    except Exception:
        pass

# === Boucle par lots ===
for batch_num, start_idx in enumerate(range(0, len(df), BATCH_SIZE)):
    if batch_num in existing_batches:
        continue  # Batch déjà traité, on saute
    end_idx = min(start_idx + BATCH_SIZE, len(df))
    batch = df.iloc[start_idx:end_idx].copy()

    print(f"Traitement batch {batch_num} : lignes {start_idx} à {end_idx}")

    for idx, row in tqdm(batch.iterrows(), total=len(batch), desc=f"Batch {batch_num}"):
        if isinstance(row["album_cover_url"], str) and len(row["album_cover_url"]) > 5:
            continue  # Déjà enrichi

        artist = row["artist"]
        title = row["track_name"]

        try:
            query = f"track:{title} artist:{artist}"
            results = sp.search(q=query, type="track", limit=1)
            items = results["tracks"]["items"]
            if items and "album" in items[0]:
                images = items[0]["album"]["images"]
                if images:
                    batch.at[idx, "album_cover_url"] = images[0]["url"]
                else:
                    batch.at[idx, "album_cover_url"] = ""
            else:
                batch.at[idx, "album_cover_url"] = ""
        except Exception as e:
            print(f"Erreur pour {title} - {artist} : {e}")
            batch.at[idx, "album_cover_url"] = ""

        time.sleep(PAUSE_SECONDS)

    # Sauvegarder le batch
    batch_file = os.path.join(OUTPUT_DIR, f"spotify_batch_{batch_num:04d}.csv")
    batch.to_csv(batch_file, index=False)
    print(f"Batch {batch_num} sauvegardé → {batch_file}")

print("Tous les lots sont traités ! Fusionne pour finaliser.")

# === Fusion finale ===
batch_files = glob.glob(f"{OUTPUT_DIR}/spotify_batch_*.csv")
dfs = [pd.read_csv(f) for f in batch_files]
final_df = pd.concat(dfs, ignore_index=True)
final_df.to_csv(OUTPUT_FINAL, index=False)
print(f"Dataset final enrichi → {OUTPUT_FINAL}")
