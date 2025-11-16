"""Unit tests for the FBRef scraper module"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import polars as pl
from bs4 import BeautifulSoup

from bayesball import fbref_scraper


class TestGetLeagueUrl:
    """Tests for get_league_url function"""
    
    def test_premier_league_url(self):
        url = fbref_scraper.get_league_url("ENG", "M", 2024, "1st")
        assert "fbref.com" in url
        assert "2023-2024" in url
        assert "Premier-League" in url
    
    def test_la_liga_url(self):
        url = fbref_scraper.get_league_url("ESP", "M", 2023, "1st")
        assert "fbref.com" in url
        assert "2022-2023" in url
        assert "La-Liga" in url
    
    def test_invalid_country(self):
        with pytest.raises(ValueError):
            fbref_scraper.get_league_url("INVALID", "M", 2024, "1st")


class TestParseTableToPolars:
    """Tests for parse_table_to_polars function"""
    
    def test_empty_table(self):
        df = fbref_scraper.parse_table_to_polars(None)
        assert df.is_empty()
    
    def test_simple_table(self):
        html = """
        <table>
            <thead>
                <tr>
                    <th data-stat="player">Player</th>
                    <th data-stat="age">Age</th>
                    <th data-stat="goals">Goals</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td data-stat="player">John Doe</td>
                    <td data-stat="age">25</td>
                    <td data-stat="goals">10</td>
                </tr>
                <tr>
                    <td data-stat="player">Jane Smith</td>
                    <td data-stat="age">28</td>
                    <td data-stat="goals">15</td>
                </tr>
            </tbody>
        </table>
        """
        soup = BeautifulSoup(html, 'lxml')
        table = soup.find('table')
        df = fbref_scraper.parse_table_to_polars(table)
        
        assert not df.is_empty()
        assert len(df) == 2
        assert "player" in df.columns
        assert "age" in df.columns
        assert "goals" in df.columns
    
    def test_table_with_thead_rows(self):
        html = """
        <table>
            <thead>
                <tr>
                    <th data-stat="player">Player</th>
                    <th data-stat="team">Team</th>
                </tr>
            </thead>
            <tbody>
                <tr class="thead">
                    <th>Header Row</th>
                    <th>Should Skip</th>
                </tr>
                <tr>
                    <td data-stat="player">John Doe</td>
                    <td data-stat="team">Team A</td>
                </tr>
            </tbody>
        </table>
        """
        soup = BeautifulSoup(html, 'lxml')
        table = soup.find('table')
        df = fbref_scraper.parse_table_to_polars(table)
        
        # Should skip the thead row in tbody
        assert len(df) == 1


class TestFbLeagueStats:
    """Tests for fb_league_stats function"""
    
    @patch('bayesball.fbref_scraper.fetch_page')
    def test_player_standard_stats(self, mock_fetch):
        # Mock HTML response
        html = """
        <html>
            <table id="stats_standard">
                <thead>
                    <tr>
                        <th data-stat="player">Player</th>
                        <th data-stat="goals">Goals</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td data-stat="player">Test Player</td>
                        <td data-stat="goals">5</td>
                    </tr>
                </tbody>
            </table>
        </html>
        """
        mock_fetch.return_value = BeautifulSoup(html, 'lxml')
        
        df = fbref_scraper.fb_league_stats(
            country="ENG",
            gender="M",
            season_end_year=2024,
            tier="1st",
            stat_type="standard",
            team_or_player="player"
        )
        
        assert not df.is_empty()
        assert "Season_End_Year" in df.columns
        assert df["Season_End_Year"][0] == 2024
        assert "Country" in df.columns
        assert df["Country"][0] == "ENG"
    
    @patch('bayesball.fbref_scraper.fetch_page')
    def test_no_table_found(self, mock_fetch):
        # Mock HTML with no tables
        html = "<html><body>No tables here</body></html>"
        mock_fetch.return_value = BeautifulSoup(html, 'lxml')
        
        df = fbref_scraper.fb_league_stats(
            country="ENG",
            gender="M",
            season_end_year=2024,
            tier="1st",
            stat_type="standard",
            team_or_player="player"
        )
        
        assert df.is_empty()


class TestFbLeagueUrls:
    """Tests for fb_league_urls function"""
    
    def test_returns_list(self):
        urls = fbref_scraper.fb_league_urls("ENG", "M", 2024, "1st")
        assert isinstance(urls, list)
        assert len(urls) == 1
        assert "fbref.com" in urls[0]


class TestFbTeamsUrls:
    """Tests for fb_teams_urls function"""
    
    @patch('bayesball.fbref_scraper.fetch_page')
    def test_extract_team_urls(self, mock_fetch):
        html = """
        <html>
            <table>
                <tr>
                    <td><a href="/en/squads/18bb7c10/Arsenal-Stats">Arsenal</a></td>
                </tr>
                <tr>
                    <td><a href="/en/squads/361ca564/Tottenham-Hotspur-Stats">Tottenham</a></td>
                </tr>
            </table>
        </html>
        """
        mock_fetch.return_value = BeautifulSoup(html, 'lxml')
        
        urls = fbref_scraper.fb_teams_urls("https://fbref.com/en/comps/9/Premier-League-Stats")
        
        assert len(urls) == 2
        assert all("fbref.com" in url for url in urls)
        assert all("/squads/" in url for url in urls)


class TestFbSquadWages:
    """Tests for fb_squad_wages function"""
    
    @patch('bayesball.fbref_scraper.fetch_page')
    def test_wages_data(self, mock_fetch):
        html = """
        <html>
            <table id="wages">
                <thead>
                    <tr>
                        <th data-stat="player">Player</th>
                        <th data-stat="wages">Weekly Wages</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td data-stat="player">Player 1</td>
                        <td data-stat="wages">£100,000</td>
                    </tr>
                </tbody>
            </table>
        </html>
        """
        mock_fetch.return_value = BeautifulSoup(html, 'lxml')
        
        urls = ["https://fbref.com/en/squads/test"]
        df = fbref_scraper.fb_squad_wages(urls)
        
        assert not df.is_empty()
        assert "Team_URL" in df.columns
    
    @patch('bayesball.fbref_scraper.fetch_page')
    def test_no_wages_table(self, mock_fetch):
        html = "<html><body>No wages table</body></html>"
        mock_fetch.return_value = BeautifulSoup(html, 'lxml')
        
        urls = ["https://fbref.com/en/squads/test"]
        df = fbref_scraper.fb_squad_wages(urls)
        
        assert df.is_empty()


class TestParseMatchHtml:
    """Tests for parse_match_html function"""
    
    def test_parse_basic_match(self, tmp_path):
        # Create a temporary HTML file
        html = """
        <html>
            <div class="scorebox">
                <div class="team"><a>Arsenal</a></div>
                <div class="team"><a>Chelsea</a></div>
                <div class="score">2</div>
                <div class="score">1</div>
                <span class="venuetime" data-venue-date="2024-01-15"></span>
            </div>
            <table id="stats_summary">
                <thead>
                    <tr>
                        <th data-stat="player">Player</th>
                        <th data-stat="goals">Goals</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td data-stat="player">Test Player</td>
                        <td data-stat="goals">1</td>
                    </tr>
                </tbody>
            </table>
        </html>
        """
        html_file = tmp_path / "match.html"
        html_file.write_text(html)
        
        result = fbref_scraper.parse_match_html(
            str(html_file),
            stat_types=["summary"],
            shooting=False
        )
        
        assert "match_summary" in result
        assert not result["match_summary"].is_empty()
        assert result["match_summary"]["Home"][0] == "Arsenal"
        assert result["match_summary"]["Away"][0] == "Chelsea"
    
    def test_parse_with_shooting(self, tmp_path):
        html = """
        <html>
            <div class="scorebox">
                <div class="team"><a>Team A</a></div>
                <div class="team"><a>Team B</a></div>
                <div class="score">3</div>
                <div class="score">2</div>
            </div>
            <table id="shots_all">
                <thead>
                    <tr>
                        <th data-stat="player">Player</th>
                        <th data-stat="shots">Shots</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td data-stat="player">Shooter</td>
                        <td data-stat="shots">5</td>
                    </tr>
                </tbody>
            </table>
        </html>
        """
        html_file = tmp_path / "match_shooting.html"
        html_file.write_text(html)
        
        result = fbref_scraper.parse_match_html(
            str(html_file),
            stat_types=[],
            shooting=True
        )
        
        assert not result["shooting_data"].is_empty()


class TestFbParseMatchData:
    """Tests for fb_parse_match_data function"""
    
    def test_multiple_matches(self, tmp_path):
        # Create two match HTML files
        html1 = """
        <html>
            <div class="scorebox">
                <div class="team"><a>Team A</a></div>
                <div class="team"><a>Team B</a></div>
                <div class="score">1</div>
                <div class="score">0</div>
            </div>
        </html>
        """
        html2 = """
        <html>
            <div class="scorebox">
                <div class="team"><a>Team C</a></div>
                <div class="team"><a>Team D</a></div>
                <div class="score">2</div>
                <div class="score">2</div>
            </div>
        </html>
        """
        
        file1 = tmp_path / "match1.html"
        file2 = tmp_path / "match2.html"
        file1.write_text(html1)
        file2.write_text(html2)
        
        result = fbref_scraper.fb_parse_match_data(
            [str(file1), str(file2)],
            stat_types=[],
            shooting=False
        )
        
        # Result should be tuple of (match_data, shooting_data, lineups, match_summaries)
        assert isinstance(result, tuple)
        assert len(result) == 4
        
        # Check match summaries
        match_summaries = result[3]
        assert not match_summaries.is_empty()
        assert len(match_summaries) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
