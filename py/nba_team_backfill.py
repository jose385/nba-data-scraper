#!/usr/bin/env python3
"""
NBA BallDontLie Team-Specific Data Collection
==============================================
Pull games and stats for a specific team only.

Usage:
  # By team abbreviation
  python nba_team_backfill.py --team LAL --start 2025-10-22 --end 2025-01-18

  # By team ID
  python nba_team_backfill.py --team-id 14 --start 2025-10-22 --end 2025-01-18

  # With advanced stats
  python nba_team_backfill.py --team BOS --start 2025-10-22 --end 2025-01-18 --advanced

Author: Jose
"""

import os
import time
import argparse
from datetime import datetime
from typing import Optional, List, Dict

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("BALLDONTLIE_API_KEY")
BASE_URL = "https://api.balldontlie.io/v1"
RATE_LIMIT_DELAY = 0.1

# Team abbreviation to ID mapping
TEAM_IDS = {
    "ATL": 1,  "BOS": 2,  "BKN": 3,  "CHA": 4,  "CHI": 5,
    "CLE": 6,  "DAL": 7,  "DEN": 8,  "DET": 9,  "GSW": 10,
    "HOU": 11, "IND": 12, "LAC": 13, "LAL": 14, "MEM": 15,
    "MIA": 16, "MIL": 17, "MIN": 18, "NOP": 19, "NYK": 20,
    "OKC": 21, "ORL": 22, "PHI": 23, "PHX": 24, "POR": 25,
    "SAC": 26, "SAS": 27, "TOR": 28, "UTA": 29, "WAS": 30,
}

TEAM_NAMES = {v: k for k, v in TEAM_IDS.items()}


class BallDontLieClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"Authorization": api_key})
        self.request_count = 0

    def _request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        url = f"{BASE_URL}/{endpoint}"
        try:
            response = self.session.get(url, params=params)
            self.request_count += 1
            
            if response.status_code == 429:
                print("  ⚠️ Rate limited, waiting 60s...")
                time.sleep(60)
                return self._request(endpoint, params)
            
            response.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return response.json()
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return {"data": []}

    def _paginate(self, endpoint: str, params: Dict, max_pages: int = 100) -> List[Dict]:
        all_data = []
        params = params.copy()
        params["per_page"] = 100
        cursor = None
        
        for page in range(1, max_pages + 1):
            if cursor:
                params["cursor"] = cursor
            
            response = self._request(endpoint, params)
            data = response.get("data", [])
            
            if not data:
                break
            
            all_data.extend(data)
            cursor = response.get("meta", {}).get("next_cursor")
            
            if not cursor:
                break
        
        return all_data

    def get_team_games(self, team_id: int, start_date: str, end_date: str) -> List[Dict]:
        params = {
            "team_ids[]": [team_id],
            "start_date": start_date,
            "end_date": end_date,
        }
        return self._paginate("games", params)

    def get_stats_for_games(self, game_ids: List[int]) -> List[Dict]:
        all_stats = []
        batch_size = 25
        
        for i in range(0, len(game_ids), batch_size):
            batch = game_ids[i:i+batch_size]
            params = {"game_ids[]": batch}
            stats = self._paginate("stats", params)
            all_stats.extend(stats)
        
        return all_stats

    def get_advanced_stats_for_games(self, game_ids: List[int]) -> List[Dict]:
        all_stats = []
        batch_size = 25
        
        for i in range(0, len(game_ids), batch_size):
            batch = game_ids[i:i+batch_size]
            params = {"game_ids[]": batch}
            stats = self._paginate("stats/advanced", params)
            all_stats.extend(stats)
        
        return all_stats


def flatten_game(game: Dict) -> Dict:
    home = game.get("home_team", {}) or {}
    visitor = game.get("visitor_team", {}) or {}
    
    return {
        "game_id": game.get("id"),
        "date": game.get("date"),
        "season": game.get("season"),
        "status": game.get("status"),
        "home_team_id": home.get("id"),
        "home_team_abbr": home.get("abbreviation"),
        "home_team_name": home.get("full_name"),
        "home_score": game.get("home_team_score"),
        "visitor_team_id": visitor.get("id"),
        "visitor_team_abbr": visitor.get("abbreviation"),
        "visitor_team_name": visitor.get("full_name"),
        "visitor_score": game.get("visitor_team_score"),
    }


def flatten_stat(stat: Dict) -> Dict:
    player = stat.get("player", {}) or {}
    team = stat.get("team", {}) or {}
    game = stat.get("game", {}) or {}
    
    return {
        "game_id": game.get("id"),
        "game_date": game.get("date"),
        "player_id": player.get("id"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "player_position": player.get("position"),
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "min": stat.get("min"),
        "pts": stat.get("pts"),
        "reb": stat.get("reb"),
        "ast": stat.get("ast"),
        "stl": stat.get("stl"),
        "blk": stat.get("blk"),
        "turnover": stat.get("turnover"),
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


def flatten_advanced_stat(stat: Dict) -> Dict:
    player = stat.get("player", {}) or {}
    team = stat.get("team", {}) or {}
    game = stat.get("game", {}) or {}
    
    return {
        "game_id": game.get("id"),
        "game_date": game.get("date"),
        "player_id": player.get("id"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "pie": stat.get("pie"),
        "pace": stat.get("pace"),
        "offensive_rating": stat.get("offensive_rating"),
        "defensive_rating": stat.get("defensive_rating"),
        "net_rating": stat.get("net_rating"),
        "true_shooting_percentage": stat.get("true_shooting_percentage"),
        "effective_field_goal_percentage": stat.get("effective_field_goal_percentage"),
        "usage_percentage": stat.get("usage_percentage"),
        "assist_percentage": stat.get("assist_percentage"),
        "rebound_percentage": stat.get("rebound_percentage"),
        "turnover_ratio": stat.get("turnover_ratio"),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Pull NBA data for a specific team",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Team Abbreviations:
  ATL BOS BKN CHA CHI CLE DAL DEN DET GSW
  HOU IND LAC LAL MEM MIA MIL MIN NOP NYK
  OKC ORL PHI PHX POR SAC SAS TOR UTA WAS

Examples:
  python nba_team_backfill.py --team LAL --start 2025-10-22 --end 2025-01-18
  python nba_team_backfill.py --team BOS --start 2025-01-01 --end 2025-01-18 --advanced
  python nba_team_backfill.py --team-id 10 --start 2025-10-22 --end 2025-01-18
        """
    )
    
    parser.add_argument("--team", type=str, help="Team abbreviation (e.g., LAL, BOS, GSW)")
    parser.add_argument("--team-id", type=int, help="Team ID (1-30)")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--advanced", action="store_true", help="Include advanced stats")
    parser.add_argument("--output", default="data", help="Output directory (default: data)")
    
    args = parser.parse_args()
    
    # Resolve team ID
    if args.team:
        team_abbr = args.team.upper()
        if team_abbr not in TEAM_IDS:
            print(f"❌ Unknown team: {team_abbr}")
            print(f"   Valid teams: {', '.join(sorted(TEAM_IDS.keys()))}")
            return
        team_id = TEAM_IDS[team_abbr]
    elif args.team_id:
        if args.team_id < 1 or args.team_id > 30:
            print(f"❌ Invalid team ID: {args.team_id} (must be 1-30)")
            return
        team_id = args.team_id
        team_abbr = TEAM_NAMES.get(team_id, str(team_id))
    else:
        print("❌ Must specify --team or --team-id")
        parser.print_help()
        return
    
    if not API_KEY:
        print("❌ BALLDONTLIE_API_KEY not found in .env")
        return
    
    print("=" * 60)
    print(f"🏀 {team_abbr} Data Collection")
    print(f"   Date range: {args.start} to {args.end}")
    print("=" * 60)
    
    client = BallDontLieClient(API_KEY)
    os.makedirs(args.output, exist_ok=True)
    
    # Get games
    print(f"\n📅 Fetching {team_abbr} games...")
    games = client.get_team_games(team_id, args.start, args.end)
    
    if not games:
        print(f"  No games found for {team_abbr}")
        return
    
    print(f"  Found {len(games)} games")
    games_df = pd.DataFrame([flatten_game(g) for g in games])
    
    games_file = f"{team_abbr}_games_{args.start}_{args.end}"
    games_df.to_csv(f"{args.output}/{games_file}.csv", index=False)
    games_df.to_parquet(f"{args.output}/{games_file}.parquet", index=False)
    print(f"  ✅ Saved {games_file}")
    
    # Get player stats
    game_ids = games_df["game_id"].tolist()
    
    print(f"\n📊 Fetching player stats for {len(game_ids)} games...")
    stats = client.get_stats_for_games(game_ids)
    
    if stats:
        stats_df = pd.DataFrame([flatten_stat(s) for s in stats])
        
        # Filter to just this team's players
        team_stats_df = stats_df[stats_df["team_id"] == team_id].copy()
        
        stats_file = f"{team_abbr}_player_stats_{args.start}_{args.end}"
        team_stats_df.to_csv(f"{args.output}/{stats_file}.csv", index=False)
        team_stats_df.to_parquet(f"{args.output}/{stats_file}.parquet", index=False)
        print(f"  ✅ Saved {stats_file} ({len(team_stats_df)} records)")
        
        # Also save opponent stats
        opp_stats_df = stats_df[stats_df["team_id"] != team_id].copy()
        opp_file = f"{team_abbr}_opponent_stats_{args.start}_{args.end}"
        opp_stats_df.to_csv(f"{args.output}/{opp_file}.csv", index=False)
        opp_stats_df.to_parquet(f"{args.output}/{opp_file}.parquet", index=False)
        print(f"  ✅ Saved {opp_file} ({len(opp_stats_df)} records)")
    
    # Advanced stats (optional)
    if args.advanced:
        print(f"\n📈 Fetching advanced stats...")
        adv_stats = client.get_advanced_stats_for_games(game_ids)
        
        if adv_stats:
            adv_df = pd.DataFrame([flatten_advanced_stat(s) for s in adv_stats])
            team_adv_df = adv_df[adv_df["team_id"] == team_id].copy()
            
            adv_file = f"{team_abbr}_advanced_stats_{args.start}_{args.end}"
            team_adv_df.to_csv(f"{args.output}/{adv_file}.csv", index=False)
            team_adv_df.to_parquet(f"{args.output}/{adv_file}.parquet", index=False)
            print(f"  ✅ Saved {adv_file} ({len(team_adv_df)} records)")
    
    print("\n" + "=" * 60)
    print(f"✅ Complete! API requests: {client.request_count}")
    print(f"   Output: {args.output}/")
    print("=" * 60)


if __name__ == "__main__":
    main()