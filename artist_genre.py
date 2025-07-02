import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import time
import dotenv
from tqdm import tqdm
import json
import os

dotenv.load_dotenv()

SPOTIPY_CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID')
SPOTIPY_CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET')

client_credentials_manager = SpotifyClientCredentials(
    client_id=SPOTIPY_CLIENT_ID,
    client_secret=SPOTIPY_CLIENT_SECRET
)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

INPUT_CSV = "clean.csv"
CACHE_FILE = "artist_genre_cache.json"
SAVE_EVERY = 50

df = pd.read_csv(INPUT_CSV)
artists = df['artist'].fillna('').astype(str).str.lower()

# Chargement du cache
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, 'r') as f:
        artist_genre_cache = json.load(f)
else:
    artist_genre_cache = {}

# Liste des artistes à traiter (non présents dans le cache)
unique_artists = set(artists.unique())
missing_artists = [a for a in unique_artists if a and a not in artist_genre_cache]

print(f"Artistes à traiter via l'API Spotify : {len(missing_artists)}")

def get_artist_genres(artist_name):
    try:
        results = sp.search(q='artist:' + artist_name, type='artist', limit=1)
        if results['artists']['items']:
            return results['artists']['items'][0]['genres']
    except Exception as e:
        pass
    return []

# Traitement uniquement des artistes manquants
for idx, artist in enumerate(tqdm(missing_artists, desc="Appels API Spotify artistes manquants")):
    genres = get_artist_genres(artist)
    artist_genre_cache[artist] = genres
    if idx % SAVE_EVERY == 0 and idx > 0:
        with open(CACHE_FILE, 'w') as f:
            json.dump(artist_genre_cache, f)
    time.sleep(0.1)

# Sauvegarde finale du cache
with open(CACHE_FILE, 'w') as f:
    json.dump(artist_genre_cache, f)

