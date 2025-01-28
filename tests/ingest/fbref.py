from bayesball.ingest.fbref import extract_match_data


def test_ingest_advanced_match_stats_fb():
    f = "../data/Sporting-KC-Portland-Timbers-August-18-2018-Major-League-Soccer.html"
    download_paths = [f]
    data = extract_match_data(download_paths, advanced_stats=True, base_dir="../data")
    summaries = data.match_summary
    assert "Country" in summaries.columns
    assert "Tier" in summaries.columns
    assert "Season_End_Year" in summaries.columns
    assert "MatchURL" in summaries.columns
    assert "Gender" in summaries.columns


# def test_ingest_advanced_match_stats_basic():
#     f = "../data/Boreham-Wood-Macclesfield-Town-October-6-2020-National-League.html"
#     download_paths = [f]
#     data = extract_match_data(download_paths, advanced_stats=False)
#     summaries = data.match_summary
#     assert "Country" in summaries.columns
#     assert "Tier" in summaries.columns
#     assert "Season_End_Year" in summaries.columns
#     assert "MatchURL" in summaries.columns
#     assert "Gender" in summaries.columns


def test_ingest_advanced_stats_team_player_split():
    f = "../data/Aston-Villa-Brentford-December-4-2024-Premier-League.html"
    download_paths = [f]
    data = extract_match_data(download_paths, advanced_stats=True, base_dir="../data")
    assert data.team_stats.defense != data.player_stats.defense
