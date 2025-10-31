import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import re

from src.data_clients.odds import get_odds


class TestOddsCollector(unittest.TestCase):
    """Tests for odds collection ensuring all market types are retrieved"""

    def test_get_upcoming_nfl_odds_requests_all_markets(self):
        """Verify that the API requests include h2h, spreads, and totals markets"""
        with patch('src.data_clients.odds.get_odds.requests.request') as mock_request:
            # Setup mock response with valid data structure
            mock_response = MagicMock()
            mock_response.json.return_value = [{
                "id": "test",
                "commence_time": "2025-10-30T20:00:00Z",
                "home_team": "Team A",
                "away_team": "Team B",
                "bookmakers": []
            }]
            mock_response.headers.get.return_value = "0"
            mock_request.return_value = mock_response

            # Call the public function
            try:
                get_odds.get_upcoming_nfl_odds()
            except Exception:
                pass  # We don't care if it fails, just want to check the URLs

            # Verify at least one API call was made
            self.assertGreaterEqual(mock_request.call_count, 1,
                           "Should make at least 1 API call")

            # Check that all calls include all three market types
            for call_args in mock_request.call_args_list:
                url = call_args[0][1]  # GET request, URL is second argument
                self.assertIn("markets=h2h,spreads,totals", url,
                             f"API request must include all three market types. URL: {url}")
                # Verify totals is specifically present
                self.assertIn("totals", url,
                             f"API request must include 'totals' market. URL: {url}")

    def test_response_to_df_handles_all_market_types(self):
        """Test that response processing properly handles all three market types"""
        # Mock API response with all three market types
        mock_response = [
            {
                "id": "test_game_1",
                "commence_time": "2025-10-30T20:00:00Z",
                "home_team": "Kansas City Chiefs",
                "away_team": "Las Vegas Raiders",
                "bookmakers": [
                    {
                        "key": "fanduel",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Kansas City Chiefs", "price": -200},
                                    {"name": "Las Vegas Raiders", "price": 180}
                                ]
                            },
                            {
                                "key": "spreads",
                                "outcomes": [
                                    {"name": "Kansas City Chiefs", "price": -110, "point": -7.5},
                                    {"name": "Las Vegas Raiders", "price": -110, "point": 7.5}
                                ]
                            },
                            {
                                "key": "totals",
                                "outcomes": [
                                    {"name": "Over", "price": -110, "point": 45.5},
                                    {"name": "Under", "price": -110, "point": 45.5}
                                ]
                            }
                        ]
                    }
                ]
            }
        ]

        with patch('src.data_clients.odds.get_odds.requests.request') as mock_request:
            # Setup mock for both API calls
            mock_response_obj = MagicMock()
            mock_response_obj.json.return_value = mock_response
            mock_response_obj.headers.get.return_value = "0"
            mock_request.return_value = mock_response_obj

            # Call the public function
            df = get_odds.get_upcoming_nfl_odds()

            # Verify all three market types are present
            markets_found = set(df['market'].unique())
            expected_markets = {'h2h', 'spreads', 'totals'}

            self.assertEqual(markets_found, expected_markets,
                            f"Expected markets {expected_markets} but got {markets_found}")

            # Verify totals market has proper structure
            totals_df = df[df['market'] == 'totals']
            self.assertGreater(len(totals_df), 0, "Totals market should have data")
            # Should have Over/Under outcomes
            totals_outcomes = set(totals_df['outcome'].unique())
            self.assertTrue({'Over', 'Under'}.issubset(totals_outcomes),
                          f"Totals should include 'Over' and 'Under' outcomes, got {totals_outcomes}")

    def test_url_regex_pattern_for_markets(self):
        """Test that the URL pattern in the source code includes all required markets"""
        # Read the source file to verify the URL contains all markets
        with open('src/data_clients/odds/get_odds.py', 'r') as f:
            source_code = f.read()

        # Find all markets= parameters in URLs
        market_patterns = re.findall(r'markets=([^&"\']+)', source_code)

        # Verify we found the market parameters
        self.assertGreater(len(market_patterns), 0,
                          "Should find market parameters in source code")

        # Check that each occurrence includes all three markets
        for markets_str in market_patterns:
            markets = set(markets_str.split(','))
            expected_markets = {'h2h', 'spreads', 'totals'}
            self.assertEqual(markets, expected_markets,
                           f"URL should include all three markets. Found: {markets_str}")

    def test_integration_with_mocked_api(self):
        """Integration test with fully mocked API response"""
        mock_api_response = [
            {
                "id": "game123",
                "commence_time": "2025-11-03T18:00:00Z",
                "home_team": "Team A",
                "away_team": "Team B",
                "bookmakers": [
                    {
                        "key": "draftkings",
                        "markets": [
                            {"key": "h2h", "outcomes": [
                                {"name": "Team A", "price": -150},
                                {"name": "Team B", "price": 130}
                            ]},
                            {"key": "spreads", "outcomes": [
                                {"name": "Team A", "price": -110, "point": -3.5},
                                {"name": "Team B", "price": -110, "point": 3.5}
                            ]},
                            {"key": "totals", "outcomes": [
                                {"name": "Over", "price": -105, "point": 47.5},
                                {"name": "Under", "price": -115, "point": 47.5}
                            ]}
                        ]
                    }
                ]
            }
        ]

        with patch('src.data_clients.odds.get_odds.requests.request') as mock_request:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_api_response
            mock_response.headers.get.return_value = "0"
            mock_request.return_value = mock_response

            df = get_odds.get_upcoming_nfl_odds()

            # Verify the dataframe has all expected columns
            expected_columns = {'game_id', 'game_time', 'home_team', 'away_team',
                              'book', 'market', 'outcome', 'price', 'point'}
            self.assertEqual(set(df.columns), expected_columns)

            # Verify totals market is present
            self.assertIn('totals', df['market'].values,
                         "Totals market must be present in the output")

            # Verify totals has both Over and Under
            totals_df = df[df['market'] == 'totals']
            outcomes = set(totals_df['outcome'].values)
            self.assertTrue({'Over', 'Under'}.issubset(outcomes),
                          "Totals market should have both Over and Under outcomes")


if __name__ == '__main__':
    unittest.main()
