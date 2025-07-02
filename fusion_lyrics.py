import pandas as pd
import glob

# Chemin vers tous tes batchs
batch_files = glob.glob("lyrics_batches/batch_*.csv")

dfs = [pd.read_csv(f) for f in batch_files]
final_df = pd.concat(dfs, ignore_index=True)

# Sauvegarde unique finale
final_df.to_csv("dataset_with_lyrics.csv", index=False)
print("Dataset final enrichi sauvegardé sous : dataset_with_lyrics.csv")
