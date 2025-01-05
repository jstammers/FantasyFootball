from pathlib import Path
import pandas as pd
import polars as pl

data_dir = Path("data/raw")

for f in data_dir.glob("*_match_results.csv"):
    df = pd.read_csv(f)
    df2 = df.replace("NA_character_", "")
    if "Season_End_Year" in df2.columns:
        if "Date" in df2.columns:
            if df2["Date"].dtype == "object":
                date = pd.to_datetime(df2["Date"])
            else:
                date = pd.to_datetime(df2["Date"], unit="D", origin="1970-01-01")
            df2["Date"] = date
        elif "Match_Date" in df2.columns:
            date = pd.to_datetime(df2["Match_Date"])
        year: pd.Series = date.dt.year
        month = date.dt.month
        df2["Season_End_Year"] = year.where(month > 6, year + 1)
    if not df.equals(df2):
        print(f"Replacing {f}")
        df2.to_csv(f, index=False)


for f in data_dir.glob("ENG_*_match_summary.csv"):
    df = pl.read_csv(f)
    df = df.with_columns(Match_Date=pl.col("Match_Date").str.to_date())
    df = df.with_columns(
        Season_End_Year=pl.when(df["Match_Date"].dt.month() > 7)
        .then(df["Match_Date"].dt.year() + 1)
        .otherwise(df["Match_Date"].dt.year())
    )
    if "2nd" in f.name:
        tier = "2nd"

match_results = pl.read_csv(data_dir / "ENG_match_results.csv")
for tier in ["2nd", "3rd", "4th", "5th"]:
    # match_summaries = pl.read_csv(data_dir / f"ENG_*_{tier}_match_summary.csv")
    for season_end_year in range(2019, 2026):
        urls = match_results.filter(
            pl.col("Season_End_Year") == season_end_year,
            pl.col("Tier") == tier,
            pl.col("Gender") == "M",
        )["MatchURL"].unique()
        url_names = [u.split("/")[-1] for u in urls if u is not None]
        html_dir = data_dir / "html" / "ENG" / tier / str(season_end_year)
        html_files = list(html_dir.glob("*.html"))
        to_remove = [f for f in html_files if f.stem not in url_names]
        print(f"{len(to_remove)} files to remove from {html_dir}")
        for f in to_remove:
            f.unlink()
