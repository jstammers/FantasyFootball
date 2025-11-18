from bayesball.worldfootballr import FootballDataLoader, LOGGER
from bayesball.utils import maybe_download_file
from pathlib import Path
import pandas as pd
import ibis
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
pandas2ri.activate()

readRDS = robjects.r['readRDS']
ibis.options.interactive = True

data_dir = Path("data/raw")

if not data_dir.exists():
    data_dir.mkdir(parents=True, exist_ok=True)

worldfootballr_data = "https://github.com/JaseZiv/worldfootballR_data/releases"

# create connection to database
con = ibis.connect("duckdb://bayesball.db")

# seed competitions csv
competitions = con.read_csv(
    "https://raw.githubusercontent.com/JaseZiv/worldfootballR_data/master/raw-data/all_leages_and_cups/all_competitions.csv")

con.create_table("competitions", competitions, overwrite=True)
# seed matches config
competition_config = con.read_json(data_dir / "competition_config.json")
con.create_table("competition_config", competition_config, overwrite=True)

if not "match_results" in con.list_tables():

match_results = con.table("match_results")
# get results from worldfootball_r data
countries = competition_config.select("Country").distinct().Country.collect().execute()

for country in countries:
    match_results = f"https://github.com/JaseZiv/worldfootballR_data/releases/download/match_results/{country}_match_results.rds"
    maybe_download_file(match_results, data_dir)
    df = readRDS(str(data_dir / f"{country}_match_results.rds"))
    df["Date"] = pd.to_datetime(df["Date"], unit="D", origin="1970-01-01")
    df.sort_values(["Tier", "Gender", "Date"])
    table = ibis.memtable(df)
    if not "match_results" in con.list_tables():
        con.create_table(f"match_results", table)
    else:
        table.
    con.create_table(f"match_results", df, mo)


# infer which matches need to be scraped

# scrape matches

# scrape season stats

# scrape wages


# for file in data_dir.glob("**/**/match_stats/lineups.csv"):
#     new_name = file.with_name("lineup.csv")
#     print(f"Moving {file} to {new_name}")
#     file.rename(new_name)
#
# for file in data_dir.glob("**/**/match_stats/reports.csv"):
#     new_name = file.with_name("report.csv")
#     print(f"Moving {file} to {new_name}")
#     file.rename(new_name)
#
# for file in data_dir.glob("**/**/match_stats/summaries.csv"):
#     new_name = file.with_name("summary.csv")
#     print(f"Moving {file} to {new_name}")
#     file.rename(new_name) v '


# for season in range(2017, 2026):
#     for country in ["ENG", "GER", "ITA", "FRA", "ESP"]:
#         loader = FootballDataLoader(country=country, tier="1st", season=season, gender="M", data_dir=data_dir, reload=False)
#         # loader.scrape_matches(time_pause=4)
#         loader.get_match_stats()
#         # loader.get_league_season_stats()
#         # loader.get_match_stats()

def load_stats(loader):
    loader.get_match_stats()
    # time.sleep(5)
    # loader.get_league_season_stats()
    # time.sleep(5)


# for country in ["ENG", "GER", "ITA", "FRA", "ESP"]:
#     loader = FootballDataLoader(country=country, tier="1st", season=2025, gender="M", data_dir=data_dir, reload=False)
#     loader.get_match_stats()

# for country in ["GER", "ITA", "FRA", "ESP"]:
#     for season in range(2019, 2026):
#         loader = FootballDataLoader(country=country, tier="2nd", season=season,
# for season in range(2019, 2026):
#     loader = FootballDataLoader(country="USA", tier="1st", season=season, gender="M", data_dir=data_dir, reload=True)
#     load_stats(loader)

for tier in ["2nd", "3rd", "4th", "5th"]:
    for season in range(2019, 2026):
        loader = FootballDataLoader(country="ENG", tier=tier, season=season, gender="M",
                                    data_dir=data_dir, reload=True)
        load_stats(loader)
        # try:
        #     loader.get_match_stats()
        # except Exception as e:
        #     LOGGER.error(f"Error loading {tier} {season}")
        #     LOGGER.error(e)
        #     continue
        # try:
        #     load_stats(loader)
        # except EnvironmentError:
        #     LOGGER.error(f"Error loading {tier} {season}")
        #     # wait an hour and try again
        #     time.sleep(3600)
        #     load_stats(loader)
