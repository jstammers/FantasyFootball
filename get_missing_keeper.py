import polars as pl
from bayesball.ingest.fbref import get_match_stats, scrape_matches
advanced_player_stats = pl.read_parquet("data/extract/advanced_match_stats_player.parquet")
# count the number of null values in each column
t = advanced_player_stats.filter(pl.col("MatchURL").str.contains("9a6ccf8c")).sort("Player")
m = advanced_player_stats.filter(pl.col("Country") == "ITA", pl.col("Season_End_Year") == 2021)
keys = ["Country", "Gender", "Tier", "Season_End_Year"]
missing_passing = advanced_player_stats.group_by(keys).agg(pl.all().null_count() / pl.len()).filter(pl.col("Tier") == "1st").sort("Season_End_Year").unpivot(index=keys).group_by(keys).agg(Min=pl.min("value"), Max=pl.max("value"), Avg=pl.mean("value")).sort("Avg")
STAGE_DIR = "data/ingest/stage"
missing_df = pl.read_csv("/Users/jimmy/Code/FantasyFootball/data/ingest/fbref/advanced_match_stats/player/passing/FRA_M_1st_wf.csv").filter(pl.col('Att_Total').is_null())
missing_df_2 = pl.read_csv("/Users/jimmy/Code/FantasyFootball/data/ingest/fbref/advanced_match_stats/player/passing_types/FRA_M_1st_wf.csv").filter(pl.col('Att').is_null())
missing_df_3 = pl.read_csv("/Users/jimmy/Code/FantasyFootball/data/ingest/fbref/advanced_match_stats/player/possession/FRA_M_1st_wf.csv").filter(pl.col('Touches_Touches').is_null())
missing_df.select("MatchURL","Season_End_Year").unique().group_by("Season_End_Year").len().sort("Season_End_Year")

missing_matches = pl.concat([df.select("MatchURL", "Country", "Season_End_Year", "Gender", "Tier").unique() for df in [missing_df, missing_df_2, missing_df_3]]).with_columns(Min_Advanced_Season=2018).filter(pl.col("Season_End_Year") >= pl.col("Min_Advanced_Season"))

missing_matches = missing_matches.with_columns(
    filename=pl.lit(f"{STAGE_DIR}/html/")
             + pl.col("Country")
             + pl.lit("/")
             + pl.col("MatchURL").str.split("/").list.last()
             + pl.lit(".html")
)

scrape_matches(missing_matches)

match_stats = get_match_stats(missing_matches)
match_stats.player_stats.passing.write_csv("/Users/jimmy/Code/FantasyFootball/data/ingest/fbref/advanced_match_stats/player/passing/FRA_M_1st_wf_0001.csv")
match_stats.player_stats.passing_types.write_csv("/Users/jimmy/Code/FantasyFootball/data/ingest/fbref/advanced_match_stats/player/passing_types/FRA_M_1st_wf_0001.csv")
match_stats.player_stats.possession.write_csv("/Users/jimmy/Code/FantasyFootball/data/ingest/fbref/advanced_match_stats/player/possession/FRA_M_1st_wf_0001.csv")
#
# match_stats.player_stats.defense.write_csv("/Users/jimmy/Code/FantasyFootball/data/ingest/fbref/advanced_match_stats/player/defense/ITA_M_1st_wf_0001.csv")
# match_stats.player_stats.misc.write_csv("/Users/jimmy/Code/FantasyFootball/data/ingest/fbref/advanced_match_stats/player/misc/ITA_M_1st_wf_0001.csv")
# match_stats.player_stats.passing.write_csv("/Users/jimmy/Code/FantasyFootball/data/ingest/fbref/advanced_match_stats/player/passing/ITA_M_1st_wf_0001.csv")
# match_stats.player_stats.passing_types.write_csv("/Users/jimmy/Code/FantasyFootball/data/ingest/fbref/advanced_match_stats/player/passing_types/ITA_M_1st_wf_0001.csv")
# match_stats.player_stats.possession.write_csv("/Users/jimmy/Code/FantasyFootball/data/ingest/fbref/advanced_match_stats/player/possession/ITA_M_1st_wf_0001.csv")
# match_stats.player_stats.summary.write_csv("/Users/jimmy/Code/FantasyFootball/data/ingest/fbref/advanced_match_stats/player/summary/ITA_M_1st_wf_0001.csv")
# match_stats.player_stats.keeper.write_csv("/Users/jimmy/Code/FantasyFootball/data/ingest/fbref/advanced_match_stats/player/keeper/ITA_M_1st_wf_0001.csv")