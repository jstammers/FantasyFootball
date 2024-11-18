from bayesball.worldfootballr import FootballDataLoader, LOGGER
from pathlib import Path
data_dir = Path("data/raw/fbref")

if not data_dir.exists():
    data_dir.mkdir(parents=True, exist_ok=True)

for file in data_dir.glob("**/**/match_stats/lineups.csv"):
    new_name = file.with_name("lineup.csv")
    print(f"Moving {file} to {new_name}")
    file.rename(new_name)

for file in data_dir.glob("**/**/match_stats/reports.csv"):
    new_name = file.with_name("report.csv")
    print(f"Moving {file} to {new_name}")
    file.rename(new_name)

for file in data_dir.glob("**/**/match_stats/summaries.csv"):
    new_name = file.with_name("summary.csv")
    print(f"Moving {file} to {new_name}")
    file.rename(new_name)


for season in range(2017, 2026):
    for country in ["ENG", "GER", "ITA", "FRA", "ESP"]:
        loader = FootballDataLoader(country=country, tier="1st", season=season, gender = "M", data_dir=data_dir)
        # loader.scrape_matches(time_pause=4)
        loader.get_match_stats()
        # loader.get_league_season_stats()
        # loader.get_match_stats()

# league_stats = loader.get_league_season_stats()
# match_stats = loader.get_match_stats()
# player_stats = loader.get_player_stats()