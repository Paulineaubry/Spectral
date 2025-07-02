import pandas as pd
import json
import re
from sklearn.ensemble import RandomForestClassifier

INPUT_CSV = "clean.csv"
CACHE_FILE = "artist_genre_cache.json"
OUTPUT_CSV = "dataset_avec_genres_ml.csv"

# === Load ===
df = pd.read_csv(INPUT_CSV)
with open(CACHE_FILE, "r") as f:
    artist_genre_cache = json.load(f)

artistes_fr = [
    "johnny halliday", "michel sardou", "claude francois", "michel berger", "france gall", "francis cabrel",
    "mylene farmer", "zazie", "pascal obispo", "helene segara", "daniel balavoine", "jean-jacques goldman",
    "michel polnareff", "serge gainsbourg", "charlotte gainsbourg", "henri salvador", "alain souchon", "calogero",
    "renaud", "julien clerc", "charles aznavour", "alain bashung", "bernard lavilliers", "joe dassin",
    "jacques dutronc", "celine dion", "cali", "-m-", "zaho de sagazan", "eddy de pretto", "benabar",
    "christophe", "georges brassens", "jacques brel", "leo ferre", "jean-louis aubert", "angele", "patrick bruel",
    "philippe katerine", "jean ferrat", "charles trenet", "amir", "benjamin biolay", "eddy mitchell", "julien dore",
    "francoise hardi", "vianney", "ben mazué", "grand corps malade", "louane", "soprano", "les rita mitsouko",
    "les fatals picards", "izia", "stupeflip", "louise attaque", "les caméléons", "francis lalanne",
    "pierre bachelet", "bernard adamus", "kyo", "bb brunes", "arno", "emily loizeau", "alain chamfort",
    "étienne daho", "philippe lafontaine", "dominique a", "nicolas peyrac", "bénabar", "brigitte", "matmatah",
    "fishbach", "raphaël", "la grande sophie", "indochine", "tryo", "saez", "indila", "mademoiselle k",
    "grands corps malade", "catherine ringer", "les ogres de barback", "les wampas", "les cowboys fringants",
    "les hurlements d'léo", "les têtes raides", "mansfield.tya", "manu chao", "dave", "yann tiersen",
    "thomas fersen", "stromae", "dorothée"
]

rules = [
    {"pattern": ["k-pop"], "genre": "K-Pop"},
    {"pattern": ["trap latino", "argentine trap", "trap"], "genre": "Rap"},
    {"pattern": ["rap", "hip hop"], "genre": "Rap"},
    {"pattern": ["reggaeton", "urbano latino", "latin pop", "latin", "pop urbano", "colombian pop"], "genre": "Latino"},
    {"pattern": ["pop"], "genre": "Variété-Pop"},
    {"pattern": ["edm", "dance", "house", "electro"], "genre": "Dance-Electro"},
    {"pattern": ["rock", "metal"], "genre": "Rock-Metal"},
    {"pattern": ["r&b", "soul"], "genre": "Soul & R&B"},
    {"pattern": ["classical", "opera", "baroque"], "genre": "Classique"},
    {"pattern": ["jazz", "blues"], "genre": "Jazz & Blues"},
    {"pattern": ["reggae"], "genre": "Reggae"},
    {"pattern": ["folk", "singer-songwriter"], "genre": "Contemporain"},
]

def map_genre(spotify_genres, artist):
    if any(a in artist for a in artistes_fr):
        return "Variété-Pop"
    if not spotify_genres:
        return "Autre"
    for genre in spotify_genres:
        g = genre.lower()
        for rule in rules:
            for pat in rule["pattern"]:
                if pat in g:
                    return rule["genre"]
    return "Autre"

macro_genres = []

for artist in df["artist"].fillna('').astype(str).str.lower():
    spotify_genres = artist_genre_cache.get(artist, [])
    macro = map_genre(spotify_genres, artist)
    macro_genres.append(macro)

df["genre"] = macro_genres

train_df = df[df["genre"] != "Autre"]
X = train_df[["tempo", "valence", "danceability", "energy", "acousticness"]].fillna(0)
y = train_df["genre"]

clf = RandomForestClassifier()
clf.fit(X, y)

mask_autre = df["genre"] == "Autre"
X_autre = df.loc[mask_autre, ["tempo", "valence", "danceability", "energy", "acousticness"]].fillna(0)
preds = clf.predict(X_autre)
df.loc[mask_autre, "genre"] = preds

df.to_csv(OUTPUT_CSV, index=False)
print(f" Sauvegarde terminée sans sous-genre : {OUTPUT_CSV}")


