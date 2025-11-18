""" A python module to join csvs in a directory that have the same columns """

import pandas as pd
from pathlib import Path
import argparse
def join_csvs(regex_pattern, directory):
    """ Join all csvs in a directory that match a regex pattern in their filename """
    csvs = sorted(list(Path(directory).rglob(regex_pattern)))
    if len(csvs) == 0:
        raise FileNotFoundError(f"No files found in {directory} that match {regex_pattern}")
    df = pd.concat([pd.read_csv(csv) for csv in csvs])
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])
    return df


def save_csv(df, path):
    """ Save a dataframe to a csv """
    df.to_csv(path, index=False)

def iterate_stats(input_dir, output_dir):
    files = [str(x) for x in input_dir.iterdir()]
    team_stats = set(["_".join(k.split("_")[2:]) for k in files if 'team' in k])
    player_stats = set(["_".join(k.split("_")[2:]) for k in files if 'player' in k])
    match_results = set(["_".join(k.split("_")[2:]) for k in files if 'match_results' in k])
    match_logs = set(["_".join(k.split("_")[2:]) for k in files if 'match_log' in k])
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "team").mkdir(parents=True, exist_ok=True)
    (output_dir / "player").mkdir(parents=True, exist_ok=True)
    for team_stat in team_stats:
        team_df = join_csvs(f"*{team_stat}", input_dir)
        save_csv(team_df, output_dir / "team" / f"{team_stat.replace('_team', '')}")
    for player_stat in player_stats:
        player_df = join_csvs(f"*{player_stat}", input_dir)
        save_csv(player_df, output_dir / "player" / f"{player_stat.replace('_player', '')}")
    for match_result in match_results:
        match_df = join_csvs(f"*{match_result}", input_dir)
        save_csv(match_df, output_dir / "match_results.csv")
    for match_log in match_logs:
        (output_dir / "match_log").mkdir(parents=True, exist_ok=True)
        match_df = join_csvs(f"*{match_log}", input_dir)
        save_csv(match_df, output_dir / "match_log"/ f"{match_log.replace('_match_log', '')}")



if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--input_dir", type=str, required=True)
    args.add_argument("--output_dir", type=str, required=True)
    args = args.parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    iterate_stats(input_dir, output_dir)


