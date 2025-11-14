#!/usr/bin/env python3
"""
BallDontLie API Client - Clean NBA Data Collection
Much simpler and more reliable than nba_api!
"""

import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json


class BallDontLieClient:
    """
    Clean client for BallDontLie API
    Free tier: 30 requests/minute
    Docs: https://docs.balldontlie.io/
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.base_url = "https://api.balldontlie.io/v1"
        self.api_key = api_key
        
        # Rate limiting (30 requests/minute free tier)
        self.requests_per_minute = 30 if not api_key else 60  # Higher with API key
        self.min_delay = 60.0 / self.requests_per_minute  # 2 seconds for free tier
        self.last_request = 0
        
        # Headers
        self.headers = {
            'Accept': 'application/json',
        }
        if api_key:
            self.headers['Authorization'] = api_key
        
        # Stats
        self.total_requests = 0
        self.failed_requests = 0
        
        print(f"🏀 BallDontLie API initialized")
        print(f"   Rate limit: {self.requests_per_minute} requests/minute")
        print(f"   Min delay: {self.min_delay:.1f}s between requests")
    
    def _rate_limit(self):
        """Ensure we respect rate limits"""
        time_since_last = time.time() - self.last_request
        if time_since_last < self.min_delay:
            wait_time = self.min_delay - time_since_last
            time.sleep(wait_time)
        self.last_request = time.time()
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make API request with rate limiting and error handling"""
        self._rate_limit()
        
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            self.total_requests += 1
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print(f"   ⚠️ Rate limit hit, waiting 60s...")
                time.sleep(60)
                return self._make_request(endpoint, params)  # Retry
            else:
                print(f"   ❌ API error {response.status_code}: {response.text[:200]}")
                self.failed_requests += 1
                return None
                
        except requests.exceptions.Timeout:
            print(f"   ⚠️ Request timeout for {endpoint}")
            self.failed_requests += 1
            return None
        except Exception as e:
            print(f"   ❌ Request failed: {e}")
            self.failed_requests += 1
            return None
    
    def get_teams(self) -> List[Dict]:
        """
        Get all NBA teams
        Returns: List of team dicts with id, name, abbreviation, etc.
        """
        print("📋 Fetching NBA teams...")
        
        all_teams = []
        page = 1
        
        while True:
            data = self._make_request("teams", params={"page": page})
            
            if not data or 'data' not in data:
                break
            
            teams = data['data']
            all_teams.extend(teams)
            
            # Check if more pages
            meta = data.get('meta', {})
            if page >= meta.get('total_pages', 1):
                break
            
            page += 1
        
        print(f"   ✅ Retrieved {len(all_teams)} teams")
        return all_teams
    
    def get_games(self, 
                  start_date: str,
                  end_date: Optional[str] = None,
                  team_ids: Optional[List[int]] = None,
                  seasons: Optional[List[int]] = None) -> List[Dict]:
        """
        Get games for date range
        
        Args:
            start_date: YYYY-MM-DD format
            end_date: YYYY-MM-DD format (defaults to start_date)
            team_ids: Optional list of team IDs to filter
            seasons: Optional list of season years (e.g., [2024])
        
        Returns: List of game dicts
        """
        if not end_date:
            end_date = start_date
        
        print(f"📅 Fetching games from {start_date} to {end_date}...")
        
        all_games = []
        page = 1
        
        while True:
            params = {
                "start_date": start_date,
                "end_date": end_date,
                "page": page,
                "per_page": 100  # Max per page
            }
            
            if team_ids:
                params['team_ids[]'] = team_ids
            if seasons:
                params['seasons[]'] = seasons
            
            data = self._make_request("games", params=params)
            
            if not data or 'data' not in data:
                break
            
            games = data['data']
            all_games.extend(games)
            
            # Check if more pages
            meta = data.get('meta', {})
            if page >= meta.get('total_pages', 1):
                break
            
            page += 1
            
            print(f"   📄 Page {page-1}/{meta.get('total_pages', '?')}: {len(games)} games")
        
        print(f"   ✅ Retrieved {len(all_games)} total games")
        return all_games
    
    def get_game_stats(self, game_id: int) -> List[Dict]:
        """
        Get box scores (player stats) for a specific game
        
        Args:
            game_id: Game ID from games endpoint
        
        Returns: List of player stat dicts
        """
        print(f"📊 Fetching stats for game {game_id}...")
        
        all_stats = []
        page = 1
        
        while True:
            params = {
                "game_ids[]": [game_id],
                "page": page,
                "per_page": 100
            }
            
            data = self._make_request("stats", params=params)
            
            if not data or 'data' not in data:
                break
            
            stats = data['data']
            all_stats.extend(stats)
            
            # Check if more pages
            meta = data.get('meta', {})
            if page >= meta.get('total_pages', 1):
                break
            
            page += 1
        
        print(f"   ✅ Retrieved {len(all_stats)} player stats")
        return all_stats
    
    def get_stats_for_date(self, date: str) -> List[Dict]:
        """
        Get all player stats for games on a specific date
        More efficient than fetching each game individually
        
        Args:
            date: YYYY-MM-DD format
        
        Returns: List of player stat dicts
        """
        print(f"📊 Fetching all stats for {date}...")
        
        all_stats = []
        page = 1
        
        while True:
            params = {
                "start_date": date,
                "end_date": date,
                "page": page,
                "per_page": 100
            }
            
            data = self._make_request("stats", params=params)
            
            if not data or 'data' not in data:
                break
            
            stats = data['data']
            all_stats.extend(stats)
            
            # Check if more pages
            meta = data.get('meta', {})
            if page >= meta.get('total_pages', 1):
                break
            
            page += 1
            
            print(f"   📄 Page {page-1}/{meta.get('total_pages', '?')}: {len(stats)} stats")
        
        print(f"   ✅ Retrieved {len(all_stats)} total player stats")
        return all_stats
    
    def get_players(self, search: Optional[str] = None) -> List[Dict]:
        """
        Get players (optionally filtered by search term)
        
        Args:
            search: Optional search string for player name
        
        Returns: List of player dicts
        """
        print(f"👥 Fetching players{f' matching {search}' if search else ''}...")
        
        all_players = []
        page = 1
        
        while True:
            params = {
                "page": page,
                "per_page": 100
            }
            
            if search:
                params['search'] = search
            
            data = self._make_request("players", params=params)
            
            if not data or 'data' not in data:
                break
            
            players = data['data']
            all_players.extend(players)
            
            # Check if more pages
            meta = data.get('meta', {})
            if page >= meta.get('total_pages', 1):
                break
            
            page += 1
        
        print(f"   ✅ Retrieved {len(all_players)} players")
        return all_players
    
    def get_season_averages(self, season: int, player_ids: List[int]) -> List[Dict]:
        """
        Get season averages for specific players
        
        Args:
            season: Season year (e.g., 2024 for 2024-25 season)
            player_ids: List of player IDs
        
        Returns: List of season average dicts
        """
        print(f"📈 Fetching season averages for {len(player_ids)} players...")
        
        # BallDontLie requires player IDs in query params
        params = {
            "season": season,
            "player_ids[]": player_ids
        }
        
        data = self._make_request("season_averages", params=params)
        
        if data and 'data' in data:
            averages = data['data']
            print(f"   ✅ Retrieved {len(averages)} season averages")
            return averages
        
        return []
    
    def print_stats(self):
        """Print API usage statistics"""
        success_rate = ((self.total_requests - self.failed_requests) / 
                       max(1, self.total_requests)) * 100
        
        print(f"\n📊 BallDontLie API Statistics:")
        print(f"   📡 Total requests: {self.total_requests}")
        print(f"   ✅ Successful: {self.total_requests - self.failed_requests}")
        print(f"   ❌ Failed: {self.failed_requests}")
        print(f"   📈 Success rate: {success_rate:.1f}%")


# Convenience function
def create_client(api_key: Optional[str] = None) -> BallDontLieClient:
    """Create BallDontLie client with optional API key"""
    return BallDontLieClient(api_key=api_key)


if __name__ == "__main__":
    # Test the client
    print("🧪 Testing BallDontLie API Client")
    print("=" * 50)
    
    # Create client (no API key for testing)
    client = create_client()
    
    # Test 1: Get teams
    teams = client.get_teams()
    if teams:
        print(f"\n✅ Teams test passed: {len(teams)} teams")
        print(f"   Sample: {teams[0]['full_name']}")
    
    # Test 2: Get recent games
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    games = client.get_games(start_date=yesterday)
    if games:
        print(f"\n✅ Games test passed: {len(games)} games on {yesterday}")
    else:
        print(f"\n⚠️ No games found for {yesterday} (might be off-season)")
    
    # Test 3: Get stats if games exist
    if games:
        game_id = games[0]['id']
        stats = client.get_game_stats(game_id)
        if stats:
            print(f"\n✅ Stats test passed: {len(stats)} player stats")
    
    # Print stats
    client.print_stats()
    
    print("\n✅ BallDontLie client test complete!")