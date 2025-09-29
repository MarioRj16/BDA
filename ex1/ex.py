import pandas as pd

def __main__():
    df = pd.read_csv("dias_catalogue.csv")

    # ex1
    dfex = df.dropna(subset=["age", "FeH"])
    print(dfex)
    
    # ex2
    def classify_age(age):
        if age < 100:
            return "young"
        elif age < 1000:
            return "intermediate"
        else:
            return "old"

    df["ageClass"] = df["age"].apply(classify_age)

    print(df)

    # ex3
    stats = df.groupby("ageClass")["FeH"].agg(mean="mean", std="std", count="count").reset_index()


    # ex4
    stats_sorted = stats.sort_values(by="mean", ascending=False)

    stats_sorted.to_csv("metallicity_summary.csv", index=False)


if __name__ == "__main__":
    __main__()
