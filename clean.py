import pandas as pd

def load_data(file_path: str) -> pd.DataFrame:
    """
    Loads data from a CSV file into a pandas DataFrame.
    
    Parameters:
    file_path (str): The path to the CSV file.
    
    Returns:
    pd.DataFrame: The loaded DataFrame.
    """
    # Load the CSV file into a DataFrame
    df = pd.read_csv(file_path)
    
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the input DataFrame by removing rows with missing values and duplicates.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to clean.
    
    Returns:
    pd.DataFrame: The cleaned DataFrame.
    """
    # Remove rows with any missing values
    df_cleaned = df.dropna()
    
    # Remove duplicate rows
    df_cleaned = df_cleaned.drop_duplicates()
    
    return df_cleaned


def standardize_columns(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Standardizes column names and selects relevant columns based on the dataset.
    Toutes les valeurs string sont passées en minuscules.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to standardize.
    dataset_name (str): Name of the dataset ('spotify_features', 'dataset', 'spotify_songs').
    
    Returns:
    pd.DataFrame: The DataFrame with standardized columns.
    """
    df_std = df.copy()
    
    if dataset_name == 'SpotifyFeatures':
        # Remove 'genre' column if it exists
        if 'genre' in df_std.columns:
            df_std = df_std.drop('genre', axis=1)
        
        # Rename columns to standard names
        column_mapping = {
            'artists': 'artist',
            'track_artist': 'artist',
            'artist_name': 'artist',
            'album_name': 'album',
            'track_album_name': 'album',
            'track_popularity': 'popularity'
        }
        
    elif dataset_name == 'dataset':
        # Keep 'genre' column from dataset.csv
        column_mapping = {
            'artists': 'artist',
            'track_artist': 'artist',
            'artist_name': 'artist',
            'album_name': 'album',
            'track_album_name': 'album',
            'track_popularity': 'popularity'
        }
        
    elif dataset_name == 'spotify_songs':
        # Keep track_genre, playlist_genre, playlist_subgenre
        column_mapping = {
            'artists': 'artist',
            'track_artist': 'artist',
            'artist_name': 'artist',
            'album_name': 'album',
            'track_album_name': 'album',
            'track_popularity': 'popularity'
        }
    
    # Apply column renaming
    df_std = df_std.rename(columns=column_mapping)
    
    # Define the columns to keep
    base_columns = [
        'track_id', 'artist', 'track_name', 'album', 'popularity',
        'duration_ms', 'danceability', 'energy', 'loudness', 
        'speechiness', 'acousticness', 'instrumentalness', 'liveness',
        'valence', 'tempo','track_genre', 'playlist_genre', 'playlist_subgenre'
    ]
    
    
    # Select only existing columns
    existing_columns = [col for col in base_columns if col in df_std.columns]
    df_std = df_std[existing_columns]
    # Mise en minuscules de toutes les colonnes de type string
    for col in df_std.select_dtypes(include='object').columns:
        df_std[col] = df_std[col].str.lower()
    return df_std


def merge_data(df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame) -> pd.DataFrame:
    """
    Concatène trois DataFrames et retire les doublons.
    
    Parameters:
    df1 (pd.DataFrame): The first DataFrame.
    df2 (pd.DataFrame): The second DataFrame.
    df3 (pd.DataFrame): The third DataFrame.

    Returns:
    pd.DataFrame: The merged DataFrame sans doublons.
    """
    merged_df = pd.concat([df1, df2, df3], ignore_index=True)
    merged_df = merged_df.drop_duplicates()
    return merged_df


def process_datasets(spotify_features_path: str, dataset_path: str, spotify_songs_path: str) -> pd.DataFrame:
    """
    Main function to process and merge all three datasets.
    
    Parameters:
    spotify_features_path (str): Path to SpotifyFeatures.csv
    dataset_path (str): Path to dataset.csv
    spotify_songs_path (str): Path to spotify_songs.csv
    
    Returns:
    pd.DataFrame: The final merged and processed DataFrame.
    """
    # Load the datasets
    df_spotify_features = load_data(spotify_features_path)
    df_dataset = load_data(dataset_path)
    df_spotify_songs = load_data(spotify_songs_path)
    
    # Standardize columns for each dataset
    df_spotify_features = standardize_columns(df_spotify_features, 'SpotifyFeatures')
    df_dataset = standardize_columns(df_dataset, 'dataset')
    df_spotify_songs = standardize_columns(df_spotify_songs, 'spotify_songs')
    
    # Clean the datasets
    df_spotify_features = clean_data(df_spotify_features)
    df_dataset = clean_data(df_dataset)
    df_spotify_songs = clean_data(df_spotify_songs)
    
    # Merge all datasets
    final_df = merge_data(df_spotify_features, df_dataset, df_spotify_songs)
    
    return final_df


def create_clean_dataset(spotify_features_path: str, dataset_path: str, spotify_songs_path: str) -> pd.DataFrame:
    """
    Crée un dataset propre selon les critères demandés :
    - Colonnes spécifiques
    - Pas de valeurs nulles
    - Toutes les valeurs string en minuscules
    - Noms d'artistes, titres et albums en minuscules
    - Dans 'artist', retire tout ce qui suit un point-virgule
    - Unicité sur track_id (meilleure popularité)
    - Unicité sur (artist, track_name) (meilleure popularité)
    - Filtres sur tempo, loudness, speechiness, liveness, time_signature, duration_ms
    """
    keep_cols = [
        'track_id', 'artist', 'track_name', 'album', 'popularity',
        'duration_ms', 'danceability', 'energy', 'loudness', 
        'speechiness', 'acousticness', 'instrumentalness', 'liveness',
        'valence', 'tempo', 'time_signature'
    ]

    df1 = standardize_columns(load_data(spotify_features_path), 'SpotifyFeatures')
    df2 = standardize_columns(load_data(dataset_path), 'dataset')
    df3 = standardize_columns(load_data(spotify_songs_path), 'spotify_songs')
    df = pd.concat([df1, df2, df3], ignore_index=True)
    df = df[[col for col in keep_cols if col in df.columns]]
    
    # Mise en minuscules de toutes les colonnes de type string
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.lower()
    # Retirer tout ce qui suit un point-virgule dans la colonne 'artist' (conversion en str pour éviter les erreurs)
    if 'artist' in df.columns:
        df['artist'] = df['artist'].apply(lambda x: str(x).split(';')[0].strip())
    if 'track_id' in df.columns:
        df = df.sort_values('popularity', ascending=False)
        df = df.drop_duplicates(subset=['track_id'], keep='first')
    if 'artist' in df.columns and 'track_name' in df.columns:
        df = df.sort_values('popularity', ascending=False)
        df = df.drop_duplicates(subset=['artist', 'track_name'], keep='first')
    # Conversion de time_signature en numérique si besoin
    if 'time_signature' in df.columns:
        df['time_signature'] = df['time_signature'].astype(str).str.extract(r'(\d+)').astype(float)
    # Application des filtres
    if 'tempo' in df.columns:
        df = df[(df['tempo'] >= 23.82) & (df['tempo'] <= 208.20)]
    if 'loudness' in df.columns:
        df = df[df['loudness'] <= 0]
    if 'speechiness' in df.columns:
        df = df[df['speechiness'] <= 0.66]
    if 'liveness' in df.columns:
        df = df[df['liveness'] < 0.8]
    if 'time_signature' in df.columns:
        df = df[df['time_signature'].isin([2, 3, 4, 5])]
    if 'duration_ms' in df.columns:
        df = df[(df['duration_ms'] >= 60000) & (df['duration_ms'] <= 360000)]
    df = df.reset_index(drop=True)
    return df


def save_clean_csv(spotify_features_path: str, dataset_path: str, spotify_songs_path: str, output_path: str = "clean.csv"):
    """
    Crée et sauvegarde le dataset propre dans un fichier CSV.
    """
    df = create_clean_dataset(spotify_features_path, dataset_path, spotify_songs_path)
    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    # Chemins des fichiers (adaptez si besoin)
    spotify_features_path = "SpotifyFeatures.csv"
    dataset_path = "dataset.csv"
    spotify_songs_path = "spotify_songs.csv"

    # Nettoyage et fusion
    df = create_clean_dataset(spotify_features_path, dataset_path, spotify_songs_path)
    df.to_csv("clean.csv", index=False)
    print("Fichier clean.csv généré avec succès.")