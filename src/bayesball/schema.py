import pandas as pd
from pydantic import BaseModel
import pandera as pa
from pandera.typing import Series, DataFrame

MatchSummarySchema = pa.DataFrameSchema(
    {
        "MatchURL": pa.Column(str),
        "League": pa.Column(str),
        "Match_Date": pa.Column(pa.Date),
        "Matchweek": pa.Column(str),
        "Home_Team": pa.Column(str),
        "Home_Formation": pa.Column(str, nullable=True),
        "Home_Score": pa.Column(int, default=0),
        "Home_xG": pa.Column(float, nullable=True),
        "Home_Goals": pa.Column(str, nullable=True),
        "Home_Yellow_Cards": pa.Column(int),
        "Home_Red_Cards": pa.Column(int),
        "Away_Team": pa.Column(str),
        "Away_Formation": pa.Column(str, nullable=True),
        "Away_Score": pa.Column(int, nullable=True, default=0),
        "Away_xG": pa.Column(float, nullable=True),
        "Away_Goals": pa.Column(str, nullable=True),
        "Away_Yellow_Cards": pa.Column(int),
        "Away_Red_Cards": pa.Column(int),
        "Game_URL": pa.Column(str),
        "Team": pa.Column(str),
        "Home_Away": pa.Column(str),
        "Event_Time": pa.Column(str),
        "Is_Pens": pa.Column(str),
        "Event_Half": pa.Column(str),
        "Event_Type": pa.Column(str),
        "Event_Players": pa.Column(str),
        "Score_Progression": pa.Column(str, nullable=True),
        "Penalty_Number": pa.Column(str, nullable=True),
        "Competition_Name": pa.Column(str),
        "Gender": pa.Column(str),
        "Country": pa.Column(str),
        "Tier": pa.Column(str),
        "Season_End_Year": pa.Column(int),
    },
    coerce=True,
    strict=True,
)

MatchResultsSchema = pa.DataFrameSchema(
    {
        "Competition_Name": pa.Column(str),
        "Gender": pa.Column(str),
        "Country": pa.Column(str),
        "Season_End_Year": pa.Column("Int64"),
        "Tier": pa.Column(str),
        "Round": pa.Column(str, nullable=True),
        "Wk": pa.Column("Int64", nullable=True),
        "Day": pa.Column(str, nullable=True),
        "Date": pa.Column(pa.Date),
        "Time": pa.Column(str, nullable=True),
        "Home": pa.Column(str),
        "HomeGoals": pa.Column("Int64", nullable=True),
        "Home_xG": pa.Column("Float64", nullable=True),
        "Away": pa.Column(str),
        "AwayGoals": pa.Column("Int64", nullable=True),
        "Away_xG": pa.Column("Float64", nullable=True),
        "Attendance": pa.Column("Int64", nullable=True),
        "Venue": pa.Column(str, nullable=True),
        "Referee": pa.Column(str, nullable=True),
        "Notes": pa.Column(str, nullable=True),
        "MatchURL": pa.Column(str, nullable=True),
    },
    coerce=True,
    strict=True,
)

MatchShootingSchema = pa.DataFrameSchema(
    {
        "MatchURL": pa.Column(str),
        "Date": pa.Column(pa.Date),
        "Squad": pa.Column(str),
        "Home_Away": pa.Column(str),
        "Match_Half": pa.Column(str),
        "Minute": pa.Column("Int64"),
        "Player": pa.Column(str),
        "Player_Href": pa.Column(str),
        "xG": pa.Column("Float64"),
        "PSxG": pa.Column("Float64"),
        "Outcome": pa.Column(str),
        "Distance": pa.Column("Float64"),
        "Body Part": pa.Column(str),
        "Notes": pa.Column(str),
        "Player_SCA_1": pa.Column(str),
        "Event_SCA_1": pa.Column(str),
        "Player_SCA_2": pa.Column(str),
        "Event_SCA_2": pa.Column(str),
        "Competition_Name": pa.Column(str),
        "Gender": pa.Column(str),
        "Country": pa.Column(str),
        "Tier": pa.Column(str),
        "Season_End_Year": pa.Column("Int64"),
    },
    coerce=True,
    strict=True,
)

misc_stats = {
    "CrdY": int,
    "CrdR": int,
    "2CrdY": int,
    "Fls": int,
    "Fld": int,
    "Off": int,
    "Crs": int,
    "Int": int,
    "TklW": int,
    "PKwon": int,
    "PKcon": int,
    "OG": int,
    "Recov": int,
    "Won_Aerial_Duels": int,
    "Lost_Aerial_Duels": int,
    "Won_percent_Aerial_Duels": float,  # Leave as float since it's a percentage
}

passing_stats = {
    "Att": int,
    "Live_Pass_Types": int,
    "Dead_Pass_Types": int,
    "FK_Pass_Types": int,
    "TB_Pass_Types": int,
    "Sw_Pass_Types": int,
    "Crs_Pass_Types": int,
    "TI_Pass_Types": int,
    "CK_Pass_Types": int,
    "In_Corner_Kicks": int,
    "Out_Corner_Kicks": int,
    "Str_Corner_Kicks": int,
    "Cmp_Outcomes": int,
    "Off_Outcomes": int,
    "Blocks_Outcomes": int,
    "Cmp_Total": int,
    "Att_Total": int,
    "Cmp_percent_Total": float,  # Leave as float since it's a percentage
    "TotDist_Total": int,
    "PrgDist_Total": int,
    "Cmp_Short": int,
    "Att_Short": int,
    "Cmp_percent_Short": float,  # Leave as float since it's a percentage
    "Cmp_Medium": int,
    "Att_Medium": int,
    "Cmp_percent_Medium": float,  # Leave as float since it's a percentage
    "Cmp_Long": int,
    "Att_Long": int,
    "Cmp_percent_Long": float,  # Leave as float since it's a percentage
    "Ast": int,
    "xAG": float,  # Leave as float since it's a metric
    "xA": float,  # Leave as float since it's a metric
    "KP": int,
    "Final_Third": int,
    "PPA": int,
    "CrsPA": int,
    "PrgP": int,
}

passing_types_stats = {
    "Att": int,
    "Live_Pass_Types": int,
    "Dead_Pass_Types": int,
    "FK_Pass_Types": int,
    "TB_Pass_Types": int,
    "Sw_Pass_Types": int,
    "Crs_Pass_Types": int,
    "TI_Pass_Types": int,
    "CK_Pass_Types": int,
    "In_Corner_Kicks": int,
    "Out_Corner_Kicks": int,
    "Str_Corner_Kicks": int,
    "Cmp_Outcomes": int,
    "Off_Outcomes": int,
    "Blocks_Outcomes": int,
}

possession_stats = {
    "Touches_Touches": int,
    "Def Pen_Touches": int,
    "Def 3rd_Touches": int,
    "Mid 3rd_Touches": int,
    "Att 3rd_Touches": int,
    "Att Pen_Touches": int,
    "Live_Touches": int,
    "Att_Take_Ons": int,
    "Succ_Take_Ons": int,
    "Succ_percent_Take_Ons": float,  # Leave as float since it's a percentage
    "Tkld_Take_Ons": int,
    "Tkld_percent_Take_Ons": float,  # Leave as float since it's a percentage
    "Carries_Carries": int,
    "TotDist_Carries": int,
    "PrgDist_Carries": int,
    "PrgC_Carries": int,
    "Final_Third_Carries": int,
    "CPA_Carries": int,
    "Mis_Carries": int,
    "Dis_Carries": int,
    "Rec_Receiving": int,
    "PrgR_Receiving": int,
}

summary_stats = {
    "Gls": int,
    "Ast": int,
    "PK": int,
    "PKatt": int,
    "Sh": int,
    "SoT": int,
    "CrdY": int,
    "CrdR": int,
    "Touches": int,
    "Tkl": int,
    "Int": int,
    "Blocks": int,
    "xG_Expected": float,  # Leave as float since it's a metric
    "npxG_Expected": float,  # Leave as float since it's a metric
    "xAG_Expected": float,  # Leave as float since it's a metric
    "SCA_SCA": int,
    "GCA_SCA": int,
    "Cmp_Passes": int,
    "Att_Passes": int,
    "Cmp_percent_Passes": float,  # Leave as float since it's a percentage
    "PrgP_Passes": int,
    "Carries_Carries": int,
    "PrgC_Carries": int,
    "Att_Take_Ons": int,
    "Succ_Take_Ons": int,
}

keeper_stats = {
    "SoTA_Shot_Stopping": int,
    "GA_Shot_Stopping": int,
    "Saves_Shot_Stopping": int,
    "Save_percent_Shot_Stopping": float,  # Leave as float since it's a percentage
    "PSxG_Shot_Stopping": float,  # Leave as float since it's a metric
    "Cmp_Launched": int,
    "Att_Launched": int,
    "Cmp_percent_Launched": float,  # Leave as float since it's a percentage
    "Att (GK)_Passes": int,
    "Thr_Passes": int,
    "Launch_percent_Passes": float,  # Leave as float since it's a percentage
    "AvgLen_Passes": float,  # Leave as float since it's an average
    "Att_Goal_Kicks": int,
    "Launch_percent_Goal_Kicks": float,  # Leave as float since it's a percentage
    "AvgLen_Goal_Kicks": float,  # Leave as float since it's an average
    "Opp_Crosses": int,
    "Stp_Crosses": int,
    "Stp_percent_Crosses": float,  # Leave as float since it's a percentage
    "Player_NumOPA_Sweeper": int,
    "AvgDist_Sweeper": float,  # Leave as float since it's an average
}

defense_stats = {
    "Tkl_Tackles": int,
    "TklW_Tackles": int,
    "Def 3rd_Tackles": int,
    "Mid 3rd_Tackles": int,
    "Att 3rd_Tackles": int,
    "Tkl_Challenges": int,
    "Att_Challenges": int,
    "Tkl_percent_Challenges": float,  # Leave as float since it's a percentage
    "Lost_Challenges": int,
    "Block_Blocks": int,
    "Sh_Blocks": int,
    "Pass_Blocks": int,
    "Int": int,
    "Tkl+Int": int,
    "Clr": int,
    "Err": int,
}

stat_schema = {
    "defense": defense_stats,
    "keeper": keeper_stats,
    "misc": misc_stats,
    "passing": passing_stats,
    "passing_types": passing_types_stats,
    "possession": possession_stats,
    "summary": summary_stats,
}
