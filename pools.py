import pandas as pd

# === Charger le dataset ===
df = pd.read_csv("dataset_with_features.csv")

# === Créer 5 pools ===

# Cardio pool ➜ energy élevé & tempo rapide
cardio_pool = df[
    (df["energy"] > 0.8) &
    (df["tempo"] > 120)
]

# Dynamic pool ➜ energy moyen-haut
dynamic_pool = df[
    (df["energy"] > 0.5) & 
    (df["energy"] <= 0.8)
]

# Chill pool ➜ energy bas
chill_pool = df[
    (df["energy"] < 0.4)
]

# Dansant pool ➜ danceability max & activity = party (ou valence haut en backup)
dansant_pool = df[
    (df["danceability"] > 0.8) & 
    (df["valence"] > 0.5)]

# Empowerment pool ➜ empowerment True
empower_pool = df[
    (df["empowerment"] == True)
]

# === Vérifie les tailles ===
print(f"Cardio pool : {len(cardio_pool)} titres")
print(f"Dynamic pool : {len(dynamic_pool)} titres")
print(f"Chill pool : {len(chill_pool)} titres")
print(f"Dansant pool : {len(dansant_pool)} titres")
print(f"Empowerment pool : {len(empower_pool)} titres")


