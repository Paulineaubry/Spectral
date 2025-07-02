import pandas as pd
import glob

batch_files = glob.glob("lastfm_batches/lastfm_batch_*.csv")
dfs = [pd.read_csv(f) for f in batch_files]
final_df = pd.concat(dfs, ignore_index=True)
final_df.to_csv("dataset_with_lastfm_tags.csv", index=False)
print("Fusion finale → dataset_with_lastfm_tags.csv")
