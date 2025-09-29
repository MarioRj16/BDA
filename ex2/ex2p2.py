import pandas as pd

# Load the dataset
df = pd.read_csv("dias_catalogue.csv")

# Exercise 1 - Selecting Columns
selected_columns = df[["name", "RA_ICRS", "DE_ICRS", "Vr", "Plx"]]
print("Exercise 1 - Selected columns:")
print(selected_columns.head(), "\n")

# Exercise 2 - Filtering Rows (age > 1)
age_filtered = df[df["age"] > 1]
print("Exercise 2 - Clusters with age > 1:")
print(age_filtered.head(), "\n")

# Exercise 3 - Sorting by Plx descending and top 10 rows
top_plx = df.sort_values(by="Plx", ascending=False).head(10)
print("Exercise 3 - Top 10 clusters by Plx:")
print(top_plx, "\n")

# Exercise 4 - Average metallicity for clusters with N > 10
avg_feh = df[df["N"] > 10]["FeH"].mean()
print(f"Exercise 4 - Average metallicity (FeH) for clusters with N > 10: {avg_feh}\n")

# Exercise 5 - Count clusters with flagdispPM = 1 and sigPM > 2
count_flag_sig = df[(df["flagdispPM"] == 1) & (df["sigPM"] > 2)].shape[0]
print(f"Exercise 5 - Number of clusters with flagdispPM=1 and sigPM>2: {count_flag_sig}")
