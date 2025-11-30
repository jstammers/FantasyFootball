from datetime import datetime
from enum import Enum
from urllib.error import HTTPError

import pandas as pd
from pandas.errors import ParserError
from pathlib import Path
import urllib.request
import bs4
class League(Enum):
    premier = "E0"
    championship = "E1"
    league_1 = "E2"
    league_2 = "E3"
    conference = "EC"


def load_historical_data(season: str, league: League):
    local_path = Path(f"data/s_{season}_{league}.csv")
    url = f"http://www.football-data.co.uk/mmz4281/{season}/{league}.csv"
    try:
        if local_path.exists():
            df = pd.read_csv(local_path)
        else:
            data = urllib.request.urlretrieve(url, local_path)
            df = pd.read_csv(data)
    except:
        print(f"Failed to parse {season} - {league}")
        return None
    df["Season"] = season
    return df.dropna(axis=1, how='all')


def load_all_data():
    def short_year(x):
        return str(x)[-2:]
    years = pd.date_range("01-01-1993", end=datetime.today(), freq='Y').year
    seasons = [f"{short_year(y)}{short_year(y + 1)}" for y in years]
    for season in seasons:
        for league in League:
            try:
                yield load_historical_data(season, league.value)
            except Exception:
                print(f"Failed for {season} - {league}")

def parse_lineup(url:str):
    page = bs4.BeautifulSoup()


