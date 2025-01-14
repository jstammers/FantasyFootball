SEASON_PLAYER_STATS = [
    "standard",
    "shooting",
    "passing",
    "passing_types",
    "gca",
    "defense",
    "possession",
    "playing_time",
    "misc",
    "keepers",
    "keepers_adv",
]
SEASON_TEAM_STATS = [
    "league_table",
    "league_table_home_away",
    "standard",
    "keeper",
    "keeper_adv",
    "shooting",
    "passing",
    "passing_types",
    "goal_shot_creation",
    "defense",
    "possession",
    "misc",
    "playing_time",
]
ADVANCED_MATCH_STATS = [
    "summary",
    "keeper",
    "passing",
    "passing_types",
    "possession",
    "defense",
    "misc",
]

COUNTRIES = ["ENG", "GER", "ITA", "FRA", "ESP", "USA"]
TIERS = {
    "ENG": ["1st", "2nd", "3rd", "4th", "5th"],
    "GER": ["1st"],
    "ITA": [
        "1st",
    ],
    "FRA": [
        "1st",
    ],
    "ESP": ["1st"],
    "USA": ["1st"],
}

LEAGUE_STATS = [
    {"Country": "ENG", "Tier": "1st", "Min_Advanced_Season": 2018},
    {"Country": "GER", "Tier": "1st", "Min_Advanced_Season": 2018},
    {"Country": "ITA", "Tier": "1st", "Min_Advanced_Season": 2018},
    {"Country": "FRA", "Tier": "1st", "Min_Advanced_Season": 2018},
    {"Country": "ESP", "Tier": "1st", "Min_Advanced_Season": 2018},
    {"Country": "USA", "Tier": "1st", "Min_Advanced_Season": 2018},
    {"Country": "ENG", "Tier": "2nd", "Min_Advanced_Season": 2019},
    {"Country": "ENG", "Tier": "3rd", "Min_Advanced_Season": None},
    {"Country": "ENG", "Tier": "4th", "Min_Advanced_Season": None},
    {"Country": "ENG", "Tier": "5th", "Min_Advanced_Season": None},
]

MIN_SEASON_END_YEAR = 2018
