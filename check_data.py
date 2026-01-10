import pandas as pd

df = pd.read_parquet("2026-01-10.parquet")

print(df.head())
print(df.info())
