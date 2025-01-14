from bayesball.ingest.wf import read_match_results, maybe_download_file
import os
import pandas as pd


def test_read_match_results(tmpdir):
    url = f"https://github.com/JaseZiv/worldfootballR_data/releases/download/match_results/USA_match_results.rds"
    maybe_download_file(url, "../data")
    df = read_match_results(os.path.join("../data", f"USA_match_results.rds"))
    assert isinstance(df, pd.DataFrame)
