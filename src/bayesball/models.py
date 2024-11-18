"""models.py - data models for stats per entity"""
from pydantic import BaseModel
class SeasonPlayerStats:
    pass


class SeasonTeamStats:
    pass

class MatchPlayerStats:
    pass

class MatchTeamStats:
    pass

class Player:
    player_id: int
    name: str
    dob: str


class Team:
    pass

class Season:
    pass

class Match:
    pass