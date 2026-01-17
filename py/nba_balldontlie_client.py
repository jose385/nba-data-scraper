#!/usr/bin/env python3
"""
BallDontLie API Client - GOAT Tier ($39.99/month)
Supports 600 requests/minute with all premium endpoints
"""

import requests
import time
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import pandas as pd

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class BallDontLieClient:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("BALLDONTLIE_API_KEY")
        if not self.api_key:
            raise ValueError("API key required. Set BALLDONTLIE_API_KEY env var or pass api_key parameter")
        
        self.base_url = "https://api.balldontlie.io"
        self.headers = {"Authorization": self.api_key}
        self.min_request_interval = 0.1
        self.last_request_time = 0
        self.request_count = 0
        
    def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()
        self.request_count += 1
        
    def _request(self, endpoint: str, params: Optional[Dict] = None, version: str = "v1") -> Optional[Dict]:
        self._rate_limit()
        url = f"{self.base_url}/{version}/{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print(f"   ⚠️ Rate limited, waiting 60s...")
                time.sleep(60)
                return self._request(endpoint, params, version)
            elif response.status_code == 401:
                print(f"   ❌ Authentication failed - check API key")
                return None
            else:
                print(f"   ❌ Error {response.status_code}: {response.text[:200]}")
                return None
        except requests.exceptions.Timeout:
            print(f"   ⚠️ Timeout on {endpoint}, retrying...")
            time.sleep(2)
            return self._request(endpoint, params, version)
        except Exception as e:
            print(f"   ❌ Request error: {e}")
            return None
    
    def _paginate(self, endpoint: str, params: Dict, max_pages: int = 50, version: str = "v1") -> List[Dict]:
        all_data = []
        params = params.copy()
        params["per_page"] = 100
        for page in range(1, max_pages + 1):
            params["cursor"] = page - 1 if page > 1 else 0
            result = self._request(endpoint, params, version)
            if not result or not result.get("data"):
                break
            all_data.extend(result["data"])
            meta = result.get("meta", {})
            if not meta.get("next_cursor"):
                break
        return all_data
    
    # === CORE ENDPOINTS ===
    
    def get_teams(self) -> List[Dict]:
        result = self._request("teams")
        return result.get("data", []) if result else []
    
    def get_players(self, search: Optional[str] = None, team_ids: Optional[List[int]] = None) -> List[Dict]:
        params = {"per_page": 100}
        if search:
            params["search"] = search
        if team_ids:
            params["team_ids[]"] = team_ids
        return self._paginate("players", params)
    
    def get_games(self, start_date: str, end_date: str, team_ids: Optional[List[int]] = None) -> List[Dict]:
        params = {"start_date": start_date, "end_date": end_date, "per_page": 100}
        if team_ids:
            params["team_ids[]"] = team_ids
        return self._paginate("games", params)
    
    def get_stats(self, game_ids: Optional[List[int]] = None, player_ids: Optional[List[int]] = None,
                  start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict]:
        params = {"per_page": 100}
        if game_ids:
            params["game_ids[]"] = game_ids
        if player_ids:
            params["player_ids[]"] = player_ids
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._paginate("stats", params)
    
    def get_box_scores(self, date: str) -> List[Dict]:
        result = self._request("box_scores", {"date": date})
        return result.get("data", []) if result else []
    
    # === GOAT TIER ENDPOINTS ===
    
    def get_advanced_stats(self, player_ids: Optional[List[int]] = None, game_ids: Optional[List[int]] = None,
                           start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict]:
        params = {"per_page": 100}
        if player_ids:
            params["player_ids[]"] = player_ids
        if game_ids:
            params["game_ids[]"] = game_ids
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._paginate("stats/advanced", params)
    
    def get_season_averages(self, season: int, player_ids: Optional[List[int]] = None) -> List[Dict]:
        params = {"season": season}
        if player_ids:
            params["player_ids[]"] = player_ids
        result = self._request("season_averages", params)
        return result.get("data", []) if result else []
    
    def get_standings(self, season: int) -> List[Dict]:
        result = self._request("standings", {"season": season})
        return result.get("data", []) if result else []
    
    def get_injuries(self) -> List[Dict]:
        result = self._request("injuries")
        return result.get("data", []) if result else []
    
    def get_leaders(self, season: int, stat_type: str = "pts") -> List[Dict]:
        result = self._request("leaders", {"season": season, "stat_type": stat_type})
        return result.get("data", []) if result else []
    
    def get_odds(self, game_id: int) -> List[Dict]:
        result = self._request(f"games/{game_id}/odds")
        return result.get("data", []) if result else []
    
    def get_play_by_play(self, game_id: int) -> List[Dict]:
        result = self._request(f"games/{game_id}/play_by_play")
        return result.get("data", []) if result else []
    
    def get_lineups(self, game_id: int) -> List[Dict]:
        result = self._request(f"games/{game_id}/lineups")
        return result.get("data", []) if result else []
    
    def test_connection(self) -> bool:
        print("🔌 Testing BallDontLie API connection...")
        teams = self.get_teams()
        if not teams:
            print("❌ Failed to fetch teams - check API key")
            return False
        print(f"✅ Basic access confirmed - {len(teams)} teams")
        injuries = self.get_injuries()
        if injuries is not None:
            print(f"✅ GOAT tier confirmed - injuries endpoint accessible")
        print(f"📊 Total requests made: {self.request_count}")
        return True


# === FLATTEN FUNCTIONS ===

def flatten_game(game: Dict) -> Dict:
    return {
        "game_id": game.get("id"),
        "date": game.get("date"),
        "season": game.get("season"),
        "status": game.get("status"),
        "period": game.get("period"),
        "time": game.get("time"),
        "postseason": game.get("postseason"),
        "home_team_id": game.get("home_team", {}).get("id"),
        "home_team": game.get("home_team", {}).get("full_name"),
        "home_team_abbr": game.get("home_team", {}).get("abbreviation"),
        "home_score": game.get("home_team_score"),
        "visitor_team_id": game.get("visitor_team", {}).get("id"),
        "visitor_team": game.get("visitor_team", {}).get("full_name"),
        "visitor_team_abbr": game.get("visitor_team", {}).get("abbreviation"),
        "visitor_score": game.get("visitor_team_score"),
    }


def flatten_player_stats(stat: Dict) -> Dict:
    player = stat.get("player", {})
    team = stat.get("team", {})
    game = stat.get("game", {})
    return {
        "stat_id": stat.get("id"),
        "player_id": player.get("id"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "player_position": player.get("position"),
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "game_id": game.get("id"),
        "game_date": game.get("date"),
        "min": stat.get("min"),
        "pts": stat.get("pts"),
        "reb": stat.get("reb"),
        "ast": stat.get("ast"),
        "stl": stat.get("stl"),
        "blk": stat.get("blk"),
        "turnover": stat.get("turnover"),
        "pf": stat.get("pf"),
        "fgm": stat.get("fgm"),
        "fga": stat.get("fga"),
        "fg_pct": stat.get("fg_pct"),
        "fg3m": stat.get("fg3m"),
        "fg3a": stat.get("fg3a"),
        "fg3_pct": stat.get("fg3_pct"),
        "ftm": stat.get("ftm"),
        "fta": stat.get("fta"),
        "ft_pct": stat.get("ft_pct"),
        "oreb": stat.get("oreb"),
        "dreb": stat.get("dreb"),
    }


def flatten_advanced_stats(stat: Dict) -> Dict:
    player = stat.get("player", {})
    team = stat.get("team", {})
    game = stat.get("game", {})
    return {
        "stat_id": stat.get("id"),
        "player_id": player.get("id"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "game_id": game.get("id"),
        "game_date": game.get("date"),
        "pie": stat.get("pie"),
        "pace": stat.get("pace"),
        "assist_percentage": stat.get("assist_percentage"),
        "assist_ratio": stat.get("assist_ratio"),
        "assist_to_turnover": stat.get("assist_to_turnover"),
        "defensive_rating": stat.get("defensive_rating"),
        "defensive_rebound_percentage": stat.get("defensive_rebound_percentage"),
        "effective_field_goal_percentage": stat.get("effective_field_goal_percentage"),
        "net_rating": stat.get("net_rating"),
        "offensive_rating": stat.get("offensive_rating"),
        "offensive_rebound_percentage": stat.get("offensive_rebound_percentage"),
        "rebound_percentage": stat.get("rebound_percentage"),
        "true_shooting_percentage": stat.get("true_shooting_percentage"),
        "turnover_ratio": stat.get("turnover_ratio"),
        "usage_percentage": stat.get("usage_percentage"),
    }


if __name__ == "__main__":
    try:
        client = BallDontLieClient()
        client.test_connection()
    except ValueError as e:
        print(f"❌ {e}")
        print("\nTo fix: echo 'BALLDONTLIE_API_KEY=your_key_here' > .env")