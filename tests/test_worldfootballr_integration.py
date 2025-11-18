"""Integration test to verify the refactored worldfootballr module works"""

import pytest
from pathlib import Path
from unittest.mock import patch
from bs4 import BeautifulSoup

from bayesball.worldfootballr import call_wf_function


class TestCallWfFunction:
    """Integration tests for call_wf_function"""
    
    @patch('bayesball.fbref_scraper.fetch_page')
    def test_fb_league_stats_integration(self, mock_fetch):
        """Test that call_wf_function correctly routes to fb_league_stats"""
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
                        <td data-stat="goals">10</td>
                    </tr>
                </tbody>
            </table>
        </html>
        """
        mock_fetch.return_value = BeautifulSoup(html, 'lxml')
        
        # Call through the wrapper function
        result = call_wf_function(
            "fb_league_stats",
            country="ENG",
            gender="M",
            season_end_year=2024,
            tier="1st",
            stat_type="standard",
            team_or_player="player"
        )
        
        assert result is not None
        assert not result.is_empty()
        assert "Season_End_Year" in result.columns
        assert result["Season_End_Year"][0] == 2024
    
    def test_fb_league_urls_integration(self):
        """Test that call_wf_function correctly routes to fb_league_urls"""
        result = call_wf_function(
            "fb_league_urls",
            country="ENG",
            gender="M",
            season_end_year=2024,
            tier="1st"
        )
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert "fbref.com" in result[0]
    
    @patch('bayesball.fbref_scraper.fetch_page')
    def test_fb_teams_urls_integration(self, mock_fetch):
        """Test that call_wf_function correctly routes to fb_teams_urls"""
        html = """
        <html>
            <table>
                <tr>
                    <td><a href="/en/squads/18bb7c10/Arsenal-Stats">Arsenal</a></td>
                </tr>
            </table>
        </html>
        """
        mock_fetch.return_value = BeautifulSoup(html, 'lxml')
        
        result = call_wf_function(
            "fb_teams_urls",
            league_url="https://fbref.com/en/comps/9/Premier-League-Stats"
        )
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert "/squads/" in result[0]
    
    @patch('bayesball.fbref_scraper.fetch_page')
    def test_fb_squad_wages_integration(self, mock_fetch):
        """Test that call_wf_function correctly routes to fb_squad_wages"""
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
        
        result = call_wf_function(
            "fb_squad_wages",
            team_urls=["https://fbref.com/en/squads/test"]
        )
        
        assert result is not None
        assert not result.is_empty()
        assert "Team_URL" in result.columns
    
    def test_invalid_function(self):
        """Test that invalid function names are handled properly"""
        result = call_wf_function("invalid_function")
        
        # Should return empty DataFrame on error
        assert result.is_empty()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
