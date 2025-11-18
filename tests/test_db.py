df.drop(columns=["Unnamed: 0"], inplace=True)
df.to_csv("data/FBRef_parsed/big5/match_results.csv", index=False)

stats = ["Competition_Name", "Gender", "Country", "Season_End_Year", "Wk"]
combs = pd.MultiIndex.from_product([df[x].unique() for x in stats]).sort_values()
