from datetime import datetime, time, date
from typing import Optional, List
from pydantic import BaseModel, Field

class Country(BaseModel):
	id: int
	name: str

class Referee(BaseModel):
	id: int
	name: str


class CompetitionStage(BaseModel):
	id: int
	name: str


class MetaData(BaseModel):
	data_version: Optional[str]
	shot_fidelity_version: Optional[str]
	xy_fidelity_version: Optional[str]


class Manager(BaseModel):
	id: int
	name: str
	nickname: Optional[str]
	dob: Optional[str]
	country: Optional[Country]


class HomeTeam(BaseModel):
	id: int = Field(alias="home_team_id")
	name: str = Field(alias="home_team_name")
	gender: str = Field(alias="home_team_gender")
	group: Optional[str] = Field(alias="home_team_group")
	country: Country
	managers: Optional[List[Manager]]

class AwayTeam(HomeTeam):
	id: int = Field(alias="away_team_id")
	name: str = Field(alias="away_team_name")
	gender: str = Field(alias="away_team_gender")
	group: Optional[str] = Field(alias="away_team_group")

class Season(BaseModel):
	season_id: int
	season_name: str


class Competition(BaseModel):
	competition_id: int
	country_name: str
	competition_name: str


class Match(BaseModel):
	match_id: int
	match_date: Optional[date]
	kick_off: Optional[time]
	competition: Optional[Competition]
	season: Optional[Season]
	home_team: HomeTeam
	away_team: AwayTeam
	home_score: int = 0
	away_score: int = 0
	match_status: Optional[str]
	match_status_360: Optional[str]
	last_updated: Optional[datetime]
	last_updated_360: Optional[datetime]
	metadata: Optional[MetaData]
	match_week: Optional[int]
	competition_stage: Optional[CompetitionStage]
	referee: Optional[Referee]



class Matches(BaseModel):
	matches: List[Match]

class Player(BaseModel):
	player_id: int
	player_name: str
	jersey_number: int
	country: Country


class Lineup(BaseModel):
	team_id: int
	team_name: str
	lineup: List[Player]

class MatchLineup(BaseModel):
	home_team: Lineup
	away_team: Lineup
	home_score: Optional[int]
	away_score: Optional[int]
	season: Optional[Season]
	competition: Optional[Competition]