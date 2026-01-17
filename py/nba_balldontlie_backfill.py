#!/usr/bin/env python3
"""
NBA BallDontLie Data Backfill Script (Comprehensive)
=====================================================
Collects core NBA data for win probability simulations.
All nested JSON structures are flattened for easy analysis.

Endpoints included:
  - Games (with home/visitor team details)
  - Player Stats (full box scores with player/team/game details)
  - Advanced Stats (pace, ratings, efficiency metrics)
  - Standings (with full team details)
  - League Leaders (with full player details)
  - Teams (flat reference data)
  - Players (with team assignment details)

Data NOT included (Claude supplements via web search):
  - Injuries (real-time from web)
  - Betting Odds (real-time from web)
  - Season Averages (derived from player_stats or web)

Usage:
  # Full season backfill
  python nba_balldontlie_backfill.py --start 2025-10-22 --end 2026-01-17 --season 2025 --full

  # Daily update
  python nba_balldontlie_backfill.py --start 2026-01-17 --end 2026-01-17 --daily

  # Specific data types
  python nba_balldontlie_backfill.py --start 2026-01-16 --end 2026-01-16 --games --player-stats

Author: Jose
Last Updated: January 17, 2026
"""

import os
import time
import argparse
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv("BALLDONTLIE_API_KEY")
BASE_URL = "https://api.balldontlie.io/v1"
OUTPUT_DIR = "data"
RATE_LIMIT_DELAY = 0.1  # 100ms between requests (600/min = 10/sec, conservative)


class BallDontLieClient:
    """API client for BallDontLie GOAT tier endpoints."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": api_key,
            "Content-Type": "application/json"
        })
        self.request_count = 0

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make API request with rate limiting and error handling."""
        url = f"{BASE_URL}/{endpoint}"
        
        try:
            response = self.session.get(url, params=params)
            self.request_count += 1
            
            if response.status_code == 429:
                print("  ⚠️  Rate limited, waiting 60 seconds...")
                time.sleep(60)
                return self._make_request(endpoint, params)
            
            response.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Request error: {e}")
            return {"data": []}

    def _paginate(self, endpoint: str, params: Optional[Dict] = None, max_pages: int = 500) -> List[Dict]:
        """Paginate through all results."""
        all_data = []
        params = params or {}
        params["per_page"] = 100
        cursor = None
        page = 0
        
        while page < max_pages:
            page += 1
            if cursor:
                params["cursor"] = cursor
            
            response = self._make_request(endpoint, params)
            data = response.get("data", [])
            
            if not data:
                break
                
            all_data.extend(data)
            
            # Check for next page
            meta = response.get("meta", {})
            cursor = meta.get("next_cursor")
            
            if not cursor:
                break
                
            if page % 10 == 0:
                print(f"    Page {page}, {len(all_data)} records...")
        
        return all_data

    # ===================
    # API ENDPOINTS
    # ===================

    def get_games(self, start_date: str, end_date: str, season: Optional[int] = None) -> List[Dict]:
        """Get games within date range."""
        params = {
            "start_date": start_date,
            "end_date": end_date
        }
        if season:
            params["seasons[]"] = season
        return self._paginate("games", params)

    def get_player_stats(self, game_ids: List[int] = None, start_date: str = None, 
                         end_date: str = None, season: int = None) -> List[Dict]:
        """Get player box score stats."""
        if game_ids:
            all_stats = []
            batch_size = 25
            for i in range(0, len(game_ids), batch_size):
                batch_ids = game_ids[i:i+batch_size]
                params = {"game_ids[]": batch_ids}
                stats = self._paginate("stats", params)
                all_stats.extend(stats)
                print(f"    Games {i+1}-{min(i+batch_size, len(game_ids))}: {len(stats)} player stats")
            return all_stats
        else:
            params = {}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            if season:
                params["seasons[]"] = season
            return self._paginate("stats", params)

    def get_advanced_stats(self, game_ids: List[int] = None, start_date: str = None,
                           end_date: str = None, season: int = None) -> List[Dict]:
        """Get advanced player stats."""
        if game_ids:
            all_stats = []
            batch_size = 25
            for i in range(0, len(game_ids), batch_size):
                batch_ids = game_ids[i:i+batch_size]
                params = {"game_ids[]": batch_ids}
                stats = self._paginate("stats/advanced", params)
                all_stats.extend(stats)
                print(f"    Games {i+1}-{min(i+batch_size, len(game_ids))}: {len(stats)} advanced stats")
            return all_stats
        else:
            params = {}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            if season:
                params["seasons[]"] = season
            return self._paginate("stats/advanced", params)

    def get_standings(self, season: int) -> List[Dict]:
        """Get team standings for a season."""
        params = {"season": season}
        return self._paginate("standings", params)

    def get_leaders(self, season: int, stat_type: str = None) -> List[Dict]:
        """Get league leaders for a season."""
        params = {"season": season}
        if stat_type:
            params["stat_type"] = stat_type
        return self._paginate("leaders", params)

    def get_teams(self) -> List[Dict]:
        """Get all teams."""
        response = self._make_request("teams")
        return response.get("data", [])

    def get_players(self) -> List[Dict]:
        """Get all players (paginated)."""
        return self._paginate("players", params={})


# ==============================
# FLATTEN FUNCTIONS (ALL FIELDS)
# ==============================

def flatten_game(game: Dict) -> Dict:
    """Flatten a game record - extract all fields from nested team objects."""
    home = game.get("home_team", {}) or {}
    visitor = game.get("visitor_team", {}) or {}
    
    return {
        # Game info
        "game_id": game.get("id"),
        "date": game.get("date"),
        "season": game.get("season"),
        "status": game.get("status"),
        "period": game.get("period"),
        "time": game.get("time"),
        "postseason": game.get("postseason"),
        
        # Home team (flattened)
        "home_team_id": home.get("id"),
        "home_team_name": home.get("full_name"),
        "home_team_abbr": home.get("abbreviation"),
        "home_team_city": home.get("city"),
        "home_team_conference": home.get("conference"),
        "home_team_division": home.get("division"),
        "home_score": game.get("home_team_score"),
        
        # Visitor team (flattened)
        "visitor_team_id": visitor.get("id"),
        "visitor_team_name": visitor.get("full_name"),
        "visitor_team_abbr": visitor.get("abbreviation"),
        "visitor_team_city": visitor.get("city"),
        "visitor_team_conference": visitor.get("conference"),
        "visitor_team_division": visitor.get("division"),
        "visitor_score": game.get("visitor_team_score"),
    }


def flatten_player_stat(stat: Dict) -> Dict:
    """Flatten a player stat record - extract all fields from nested objects."""
    player = stat.get("player", {}) or {}
    team = stat.get("team", {}) or {}
    game = stat.get("game", {}) or {}
    
    return {
        "stat_id": stat.get("id"),
        
        # Player info (flattened)
        "player_id": player.get("id"),
        "player_first_name": player.get("first_name"),
        "player_last_name": player.get("last_name"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "player_position": player.get("position"),
        "player_height": player.get("height"),
        "player_weight": player.get("weight"),
        "player_jersey": player.get("jersey_number"),
        "player_college": player.get("college"),
        "player_country": player.get("country"),
        "player_draft_year": player.get("draft_year"),
        "player_draft_round": player.get("draft_round"),
        "player_draft_number": player.get("draft_number"),
        
        # Team info (flattened)
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "team_name": team.get("full_name"),
        "team_city": team.get("city"),
        "team_conference": team.get("conference"),
        "team_division": team.get("division"),
        
        # Game info
        "game_id": game.get("id"),
        "game_date": game.get("date"),
        
        # Box score stats (all fields)
        "min": stat.get("min"),
        "pts": stat.get("pts"),
        "reb": stat.get("reb"),
        "oreb": stat.get("oreb"),
        "dreb": stat.get("dreb"),
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
        "plus_minus": stat.get("plus_minus"),
    }


def flatten_advanced_stat(stat: Dict) -> Dict:
    """Flatten an advanced stat record - extract all fields from nested objects."""
    player = stat.get("player", {}) or {}
    team = stat.get("team", {}) or {}
    game = stat.get("game", {}) or {}
    
    return {
        "stat_id": stat.get("id"),
        
        # Player info (flattened)
        "player_id": player.get("id"),
        "player_first_name": player.get("first_name"),
        "player_last_name": player.get("last_name"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "player_position": player.get("position"),
        
        # Team info (flattened)
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "team_name": team.get("full_name"),
        
        # Game info
        "game_id": game.get("id"),
        "game_date": game.get("date"),
        
        # Advanced stats (all fields)
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


def flatten_standing(standing: Dict) -> Dict:
    """Flatten a standing record - extract all fields from nested team object."""
    team = standing.get("team", {}) or {}
    
    wins = standing.get("wins", 0) or 0
    losses = standing.get("losses", 0) or 0
    win_pct = wins / (wins + losses) if (wins + losses) > 0 else 0.0
    
    home_record = standing.get("home_record", "0-0")
    road_record = standing.get("road_record", "0-0")
    
    def parse_record(record_str):
        try:
            if not record_str or record_str == "0-0":
                return 0, 0
            parts = str(record_str).strip().split("-")
            return int(parts[0]), int(parts[1])
        except:
            return 0, 0
    
    home_wins, home_losses = parse_record(home_record)
    road_wins, road_losses = parse_record(road_record)
    
    return {
        "team_id": team.get("id"),
        "team_name": team.get("full_name"),
        "team_abbr": team.get("abbreviation"),
        "team_city": team.get("city"),
        "team_conference": team.get("conference"),
        "team_division": team.get("division"),
        "wins": wins,
        "losses": losses,
        "win_pct": round(win_pct, 3),
        "conference_rank": standing.get("conference_rank"),
        "conference_record": standing.get("conference_record"),
        "division_rank": standing.get("division_rank"),
        "division_record": standing.get("division_record"),
        "home_record": home_record,
        "home_wins": home_wins,
        "home_losses": home_losses,
        "road_record": road_record,
        "road_wins": road_wins,
        "road_losses": road_losses,
        "season": standing.get("season"),
    }


def flatten_leader(leader: Dict) -> Dict:
    """Flatten a leader record - extract all fields from nested player object."""
    player = leader.get("player", {}) or {}
    
    return {
        "player_id": player.get("id"),
        "player_first_name": player.get("first_name"),
        "player_last_name": player.get("last_name"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "player_position": player.get("position"),
        "player_height": player.get("height"),
        "player_weight": player.get("weight"),
        "player_jersey": player.get("jersey_number"),
        "player_college": player.get("college"),
        "player_country": player.get("country"),
        "player_team_id": player.get("team_id"),
        "player_draft_year": player.get("draft_year"),
        "player_draft_round": player.get("draft_round"),
        "player_draft_number": player.get("draft_number"),
        "stat_type": leader.get("stat_type"),
        "value": leader.get("value"),
        "games_played": leader.get("games_played"),
        "rank": leader.get("rank"),
        "season": leader.get("season"),
    }


def flatten_player(player: Dict) -> Dict:
    """Flatten a player record - extract all fields from nested team object."""
    team = player.get("team", {}) or {}
    
    return {
        "player_id": player.get("id"),
        "first_name": player.get("first_name"),
        "last_name": player.get("last_name"),
        "full_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "position": player.get("position"),
        "height": player.get("height"),
        "weight": player.get("weight"),
        "jersey_number": player.get("jersey_number"),
        "college": player.get("college"),
        "country": player.get("country"),
        "draft_year": player.get("draft_year"),
        "draft_round": player.get("draft_round"),
        "draft_number": player.get("draft_number"),
        "team_id": team.get("id"),
        "team_name": team.get("full_name"),
        "team_abbr": team.get("abbreviation"),
        "team_city": team.get("city"),
        "team_conference": team.get("conference"),
        "team_division": team.get("division"),
    }


def flatten_team(team: Dict) -> Dict:
    """Teams are already flat - just standardize column names."""
    return {
        "team_id": team.get("id"),
        "name": team.get("name"),
        "full_name": team.get("full_name"),
        "abbreviation": team.get("abbreviation"),
        "city": team.get("city"),
        "conference": team.get("conference"),
        "division": team.get("division"),
    }


# ==============================
# SAVE AND BACKFILL FUNCTIONS
# ==============================

def save_dataframe(df: pd.DataFrame, filename: str, output_dir: str):
    """Save DataFrame to both CSV and Parquet formats."""
    if df.empty:
        print(f"  ⚠️  No data to save for {filename}")
        return
    
    os.makedirs(output_dir, exist_ok=True)
    
    csv_path = os.path.join(output_dir, f"{filename}.csv")
    parquet_path = os.path.join(output_dir, f"{filename}.parquet")
    
    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)
    
    print(f"  ✅ Saved {len(df):,} records → {filename}.csv/.parquet")


def backfill_games(client: BallDontLieClient, start_date: str, end_date: str, 
                   season: int, output_dir: str) -> pd.DataFrame:
    """Backfill games data."""
    print(f"\n📅 GAMES: {start_date} to {end_date}")
    
    games = client.get_games(start_date, end_date, season)
    
    if not games:
        print("  No games found")
        return pd.DataFrame()
    
    print(f"  Found {len(games)} games, flattening...")
    df = pd.DataFrame([flatten_game(g) for g in games])
    save_dataframe(df, f"games_{start_date}_{end_date}", output_dir)
    
    return df


def backfill_player_stats(client: BallDontLieClient, output_dir: str, game_ids: List[int] = None,
                          start_date: str = None, end_date: str = None,
                          season: int = None) -> pd.DataFrame:
    """Backfill player stats data."""
    print(f"\n📊 PLAYER STATS")
    
    stats = client.get_player_stats(game_ids, start_date, end_date, season)
    
    if not stats:
        print("  No player stats found")
        return pd.DataFrame()
    
    print(f"  Found {len(stats):,} stat records, flattening...")
    df = pd.DataFrame([flatten_player_stat(s) for s in stats])
    
    filename = f"player_stats_{start_date}_{end_date}" if start_date else "player_stats"
    save_dataframe(df, filename, output_dir)
    
    return df


def backfill_advanced_stats(client: BallDontLieClient, output_dir: str, game_ids: List[int] = None,
                            start_date: str = None, end_date: str = None,
                            season: int = None) -> pd.DataFrame:
    """Backfill advanced stats data."""
    print(f"\n📈 ADVANCED STATS")
    
    stats = client.get_advanced_stats(game_ids, start_date, end_date, season)
    
    if not stats:
        print("  No advanced stats found")
        return pd.DataFrame()
    
    print(f"  Found {len(stats):,} stat records, flattening...")
    df = pd.DataFrame([flatten_advanced_stat(s) for s in stats])
    
    filename = f"advanced_stats_{start_date}_{end_date}" if start_date else "advanced_stats"
    save_dataframe(df, filename, output_dir)
    
    return df


def backfill_standings(client: BallDontLieClient, season: int, output_dir: str) -> pd.DataFrame:
    """Backfill standings data."""
    print(f"\n🏆 STANDINGS: Season {season}")
    
    standings = client.get_standings(season)
    
    if not standings:
        print("  No standings found")
        return pd.DataFrame()
    
    print(f"  Found {len(standings)} team standings, flattening...")
    df = pd.DataFrame([flatten_standing(s) for s in standings])
    df = df.sort_values(["team_conference", "wins"], ascending=[True, False])
    
    save_dataframe(df, f"standings_{season}", output_dir)
    
    return df


def backfill_leaders(client: BallDontLieClient, season: int, output_dir: str) -> pd.DataFrame:
    """Backfill league leaders data for all stat categories."""
    print(f"\n🌟 LEAGUE LEADERS: Season {season}")
    
    stat_types = ["pts", "reb", "ast", "stl", "blk", "fg_pct", "fg3_pct", "ft_pct"]
    all_leaders = []
    
    for stat_type in stat_types:
        print(f"    Fetching {stat_type} leaders...")
        leaders = client.get_leaders(season, stat_type)
        for leader in leaders:
            leader["stat_type"] = stat_type
        all_leaders.extend(leaders)
    
    if not all_leaders:
        print("  No leaders found")
        return pd.DataFrame()
    
    print(f"  Found {len(all_leaders):,} leader records, flattening...")
    df = pd.DataFrame([flatten_leader(l) for l in all_leaders])
    save_dataframe(df, f"leaders_{season}", output_dir)
    
    return df


def backfill_teams(client: BallDontLieClient, output_dir: str) -> pd.DataFrame:
    """Backfill teams reference data."""
    print(f"\n🏀 TEAMS")
    
    teams = client.get_teams()
    
    if not teams:
        print("  No teams found")
        return pd.DataFrame()
    
    print(f"  Found {len(teams)} teams, flattening...")
    df = pd.DataFrame([flatten_team(t) for t in teams])
    
    df_current = df[df["team_id"] <= 30].copy()
    df_historical = df[df["team_id"] > 30].copy()
    
    save_dataframe(df_current, "teams", output_dir)
    if not df_historical.empty:
        save_dataframe(df_historical, "teams_historical", output_dir)
    
    return df


def backfill_players(client: BallDontLieClient, output_dir: str) -> pd.DataFrame:
    """Backfill players reference data."""
    print(f"\n👤 PLAYERS")
    
    players = client.get_players()
    
    if not players:
        print("  No players found")
        return pd.DataFrame()
    
    print(f"  Found {len(players):,} players, flattening...")
    df = pd.DataFrame([flatten_player(p) for p in players])
    save_dataframe(df, "players", output_dir)
    
    return df


# ==============================
# MAIN BACKFILL MODES
# ==============================

def run_full_backfill(client: BallDontLieClient, start_date: str, end_date: str, 
                      season: int, output_dir: str):
    """Run complete backfill of all data types."""
    print("=" * 70)
    print(f"🚀 FULL BACKFILL: {start_date} to {end_date} (Season {season})")
    print("=" * 70)
    
    start_time = time.time()
    
    # 1. Games (needed for game_ids)
    games_df = backfill_games(client, start_date, end_date, season, output_dir)
    
    if not games_df.empty:
        game_ids = games_df["game_id"].tolist()
        
        # 2. Player Stats
        backfill_player_stats(client, output_dir, game_ids=game_ids, 
                              start_date=start_date, end_date=end_date)
        
        # 3. Advanced Stats
        backfill_advanced_stats(client, output_dir, game_ids=game_ids, 
                                start_date=start_date, end_date=end_date)
    
    # 4. Standings
    backfill_standings(client, season, output_dir)
    
    # 5. Leaders
    backfill_leaders(client, season, output_dir)
    
    # 6. Teams (reference data)
    backfill_teams(client, output_dir)
    
    # 7. Players (reference data)
    backfill_players(client, output_dir)
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"✅ FULL BACKFILL COMPLETE")
    print(f"   Time elapsed: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    print(f"   API requests: {client.request_count:,}")
    print(f"   Output: {output_dir}/")
    print("=" * 70)


def run_daily_backfill(client: BallDontLieClient, start_date: str, end_date: str, 
                       season: int, output_dir: str):
    """Run daily backfill (games, player stats, advanced stats, standings)."""
    print("=" * 70)
    print(f"📅 DAILY BACKFILL: {start_date} to {end_date}")
    print("=" * 70)
    
    start_time = time.time()
    
    # 1. Games
    games_df = backfill_games(client, start_date, end_date, season, output_dir)
    
    if not games_df.empty:
        game_ids = games_df["game_id"].tolist()
        
        # 2. Player Stats
        backfill_player_stats(client, output_dir, game_ids=game_ids, 
                              start_date=start_date, end_date=end_date)
        
        # 3. Advanced Stats
        backfill_advanced_stats(client, output_dir, game_ids=game_ids, 
                                start_date=start_date, end_date=end_date)
    
    # 4. Standings (always current)
    backfill_standings(client, season, output_dir)
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"✅ DAILY BACKFILL COMPLETE")
    print(f"   Time elapsed: {elapsed:.1f} seconds")
    print(f"   API requests: {client.request_count:,}")
    print(f"   Output: {output_dir}/")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="NBA BallDontLie Data Backfill - Comprehensive data collection with flattened output",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full season backfill
  python nba_balldontlie_backfill.py --start 2025-10-22 --end 2026-01-17 --season 2025 --full

  # Daily update
  python nba_balldontlie_backfill.py --start 2026-01-17 --end 2026-01-17 --daily

  # Just games and stats
  python nba_balldontlie_backfill.py --start 2026-01-16 --end 2026-01-16 --games --player-stats --advanced-stats
        """
    )
    
    # Date range
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--season", type=int, default=2025, help="Season year (default: 2025 for 2025-26)")
    
    # Backfill modes
    parser.add_argument("--full", action="store_true", help="Full backfill (all data types)")
    parser.add_argument("--daily", action="store_true", help="Daily backfill (games, stats, standings)")
    
    # Individual data types
    parser.add_argument("--games", action="store_true", help="Backfill games")
    parser.add_argument("--player-stats", action="store_true", help="Backfill player stats")
    parser.add_argument("--advanced-stats", action="store_true", help="Backfill advanced stats")
    parser.add_argument("--standings", action="store_true", help="Backfill standings")
    parser.add_argument("--leaders", action="store_true", help="Backfill leaders")
    parser.add_argument("--teams", action="store_true", help="Backfill teams")
    parser.add_argument("--players", action="store_true", help="Backfill players")
    
    # Output
    parser.add_argument("--output", type=str, default="data", help="Output directory (default: data)")
    
    args = parser.parse_args()
    
    # Validate API key
    if not API_KEY:
        print("❌ Error: BALLDONTLIE_API_KEY not found")
        print("   Create a .env file with: BALLDONTLIE_API_KEY=your_key_here")
        return
    
    # Initialize client
    client = BallDontLieClient(API_KEY)
    
    # Set output directory
    output_dir = args.output
    os.makedirs(output_dir, exist_ok=True)
    
    # Default dates
    today = datetime.now().strftime("%Y-%m-%d")
    start_date = args.start or today
    end_date = args.end or today
    
    # Run appropriate backfill
    if args.full:
        run_full_backfill(client, start_date, end_date, args.season, output_dir)
    elif args.daily:
        run_daily_backfill(client, start_date, end_date, args.season, output_dir)
    else:
        # Individual data types
        game_ids = None
        
        if args.games:
            games_df = backfill_games(client, start_date, end_date, args.season, output_dir)
            if not games_df.empty:
                game_ids = games_df["game_id"].tolist()
        
        if args.player_stats:
            backfill_player_stats(client, output_dir, game_ids=game_ids, 
                                  start_date=start_date, end_date=end_date, season=args.season)
        
        if args.advanced_stats:
            backfill_advanced_stats(client, output_dir, game_ids=game_ids,
                                    start_date=start_date, end_date=end_date, season=args.season)
        
        if args.standings:
            backfill_standings(client, args.season, output_dir)
        
        if args.leaders:
            backfill_leaders(client, args.season, output_dir)
        
        if args.teams:
            backfill_teams(client, output_dir)
        
        if args.players:
            backfill_players(client, output_dir)
        
        # If no flags, show help
        if not any([args.games, args.player_stats, args.advanced_stats, 
                    args.standings, args.leaders, args.teams, args.players]):
            parser.print_help()


if __name__ == "__main__":
    main()