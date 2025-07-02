import streamlit as st
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# === CONFIG ===
SPOTIPY_CLIENT_ID = "SPOTIPY_CLIENT_ID"
SPOTIPY_CLIENT_SECRET = "SPOTIPY_CLIENT_SECRET"
SPOTIPY_REDIRECT_URI = "http://localhost:8501"
SCOPE = "playlist-modify-public"

# === Auth Spotify ===
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=SPOTIPY_CLIENT_ID,
    client_secret=SPOTIPY_CLIENT_SECRET,
    redirect_uri=SPOTIPY_REDIRECT_URI,
    scope=SCOPE
))

# === Charger dataset ===
df = pd.read_csv("dataset_mood_activity.csv")

# === Interface ===
st.title("Générateur de Playlist - Pole Dance / Renfo / Flex")

genres = sorted(df['genre'].dropna().unique())
selected_genre = st.selectbox("Choisis ton genre préféré :", genres)

programme = st.radio("Choisis ton programme :", ("renfo", "flex", "pole dance"))

if st.button("Générer sur Spotify"):

    genre_pool = df[df['genre'].str.lower() == selected_genre.lower()]

    cardio_pool = genre_pool[(genre_pool.energy > 0.8) & (genre_pool.danceability > 0.7) & (genre_pool.tempo > 120)]
    dynamic_pool = genre_pool[(genre_pool.energy > 0.5) & (genre_pool.energy <= 0.8) & (genre_pool.danceability > 0.6)]
    chill_pool = genre_pool[(genre_pool.energy < 0.4)]
    dansant_pool = genre_pool[(genre_pool.danceability > 0.8) & (genre_pool['activity'] == 'party')]

    track_duration = 3.5

    phases = []
    if programme == "renfo":
        phases = [("Cardio", cardio_pool, 15), ("Dynamique", dynamic_pool, 75), ("Chill", chill_pool, 15)]
    elif programme == "flex":
        phases = [("Dynamique", dynamic_pool, 15), ("Chill", chill_pool, 45)]
    elif programme == "pole dance":
        phases = [("Dynamique", dynamic_pool, 15), ("Dansant", dansant_pool, 75)]

    track_uris = []

    for name, pool, duration in phases:
        if pool.empty:
            continue
        n_tracks = int(duration / track_duration)
        sampled = pool.sample(min(len(pool), n_tracks))
        uris = sampled['track_id'].tolist()
        track_uris.extend(uris)

    if not track_uris:
        st.error("Aucun morceau trouvé pour ce combo. Essaie un autre genre !")
        st.stop()

    # === Créer playlist ===
    user = sp.current_user()
    playlist_name = f"{programme.capitalize()} - {selected_genre.capitalize()}"

    playlist = sp.user_playlist_create(user['id'], playlist_name, public=True)
    sp.playlist_add_items(playlist['id'], track_uris)

    st.success(f"Playlist créée : [{playlist_name}]({playlist['external_urls']['spotify']})")
