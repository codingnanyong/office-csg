import pandas as pd
import os

file_path = '../data/'
print("Absolute path:", os.path.abspath(file_path))

csv_files = [f for f in os.listdir(file_path) if f.endswith('.csv')]

for file in csv_files:
    df = pd.read_csv(os.path.join(file_path, file))
    df['REQUEST_PIC'] = df['REQUEST_PIC'].str.replace(":", "-")
    df.to_csv(os.path.join(file_path, file), index=False)
