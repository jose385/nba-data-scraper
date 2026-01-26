#!/usr/bin/env python3
"""
NBA BallDontLie Data Backfill Script (V2 - Complete)
=====================================================
Collects ALL available NBA data from BallDontLie API (GOAT tier)
Supports team-specific filtering for faster, targeted collection.

Author: Jose
Last Updated: January 2026
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

# Configuration
API_KEY = os.getenv("BALLDONTLIE_API_KEY")
BASE_URL_V1 = "https://api.balldontlie.io/v1"
BASE_URL_V2 = "https://api.balldontlie.io/v2"
BASE_URL_NBA_V1 = "https://api.balldontlie.io/nba/v1"
BASE_URL_NBA_V2 = "https://api.balldontlie.io/nba/v2"
OUTPUT_DIR = "data"
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
    """API client for BallDontLie - V1 and V2 endpoints."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": api_key,
            "Content-Type": "application/json"
        })
        self.request_count = 0

    def _request(self, url: str, params: Optional[Dict] = None) -> Dict:
        """Make API request with rate limiting."""
        try:
            response = self.session.get(url, params=params)
            self.request_count += 1
            
            if response.status_code == 429:
                print("  ⚠️  Rate limited, waiting 60s...")
                time.sleep(60)
                return self._request(url, params)
            
            if response.status_code == 401:
                print(f"  ❌ Unauthorized - check API key or tier access")
                return {"data": []}
            
            response.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Request error: {e}")
            return {"data": []}

    def _paginate(self, url: str, params: Optional[Dict] = None, max_pages: int = 500) -> List[Dict]:
        """Paginate through results."""
        all_data = []
        params = params or {}
        params["per_page"] = 100
        cursor = None
        page = 0
        
        while page < max_pages:
            page += 1
            if cursor:
                params["cursor"] = cursor
            
            response = self._request(url, params)
            data = response.get("data", [])
            
            if not data:
                break
            
            all_data.extend(data)
            cursor = response.get("meta", {}).get("next_cursor")
            
            if not cursor:
                break
            
            if page % 10 == 0:
                print(f"    Page {page}, {len(all_data)} records...")
        
        return all_data

    # ===========================
    # V1 ENDPOINTS
    # ===========================

    def get_teams(self) -> List[Dict]:
        return self._request(f"{BASE_URL_V1}/teams").get("data", [])

    def get_players(self, team_ids: List[int] = None) -> List[Dict]:
        params = {}
        if team_ids:
            params["team_ids[]"] = team_ids
        return self._paginate(f"{BASE_URL_V1}/players", params)

    def get_games(self, start_date: str, end_date: str, season: int = None, 
                  team_id: int = None) -> List[Dict]:
        params = {"start_date": start_date, "end_date": end_date}
        if season:
            params["seasons[]"] = season
        if team_id:
            params["team_ids[]"] = [team_id]
        return self._paginate(f"{BASE_URL_V1}/games", params)

    def get_stats(self, game_ids: List[int] = None, start_date: str = None, 
                  end_date: str = None, season: int = None, 
                  player_ids: List[int] = None) -> List[Dict]:
        if game_ids:
            all_stats = []
            batch_size = 25
            for i in range(0, len(game_ids), batch_size):
                batch = game_ids[i:i+batch_size]
                params = {"game_ids[]": batch}
                if player_ids:
                    params["player_ids[]"] = player_ids
                stats = self._paginate(f"{BASE_URL_V1}/stats", params)
                all_stats.extend(stats)
                if (i + batch_size) % 50 == 0 or i + batch_size >= len(game_ids):
                    print(f"    Games {i+1}-{min(i+batch_size, len(game_ids))}: {len(all_stats)} stats")
            return all_stats
        else:
            params = {}
            if start_date: params["start_date"] = start_date
            if end_date: params["end_date"] = end_date
            if season: params["seasons[]"] = season
            if player_ids: params["player_ids[]"] = player_ids
            return self._paginate(f"{BASE_URL_V1}/stats", params)

    def get_standings(self, season: int) -> List[Dict]:
        return self._paginate(f"{BASE_URL_V1}/standings", {"season": season})

    def get_leaders(self, season: int, stat_type: str = None) -> List[Dict]:
        params = {"season": season}
        if stat_type:
            params["stat_type"] = stat_type
        return self._paginate(f"{BASE_URL_V1}/leaders", params)

    def get_injuries(self, team_ids: List[int] = None) -> List[Dict]:
        params = {}
        if team_ids:
            params["team_ids[]"] = team_ids
        return self._paginate(f"{BASE_URL_V1}/player_injuries", params)

    def get_active_players(self, team_ids: List[int] = None) -> List[Dict]:
        params = {}
        if team_ids:
            params["team_ids[]"] = team_ids
        return self._paginate(f"{BASE_URL_V1}/players/active", params)

    # ===========================
    # V2 ENDPOINTS
    # ===========================

    def get_advanced_stats_v2(self, game_ids: List[int] = None, start_date: str = None,
                               end_date: str = None, season: int = None, 
                               player_ids: List[int] = None, period: int = 0) -> List[Dict]:
        """Get comprehensive advanced stats (V2) - 100+ metrics."""
        if game_ids:
            all_stats = []
            batch_size = 25
            for i in range(0, len(game_ids), batch_size):
                batch = game_ids[i:i+batch_size]
                params = {"game_ids[]": batch, "period": period}
                if player_ids:
                    params["player_ids[]"] = player_ids
                stats = self._paginate(f"{BASE_URL_NBA_V2}/stats/advanced", params)
                all_stats.extend(stats)
                if (i + batch_size) % 50 == 0 or i + batch_size >= len(game_ids):
                    print(f"    Games {i+1}-{min(i+batch_size, len(game_ids))}: {len(all_stats)} adv stats")
            return all_stats
        else:
            params = {"period": period}
            if start_date: params["start_date"] = start_date
            if end_date: params["end_date"] = end_date
            if season: params["seasons[]"] = season
            if player_ids: params["player_ids[]"] = player_ids
            return self._paginate(f"{BASE_URL_NBA_V2}/stats/advanced", params)

    def get_lineups(self, game_ids: List[int]) -> List[Dict]:
        """Get starting lineups and bench players (2025+ only)."""
        all_lineups = []
        batch_size = 10
        for i in range(0, len(game_ids), batch_size):
            batch = game_ids[i:i+batch_size]
            params = {"game_ids[]": batch}
            lineups = self._paginate(f"{BASE_URL_V1}/lineups", params)
            all_lineups.extend(lineups)
        return all_lineups

    def get_play_by_play(self, game_id: int) -> List[Dict]:
        """Get play-by-play data for a game (2025+ only)."""
        result = self._request(f"{BASE_URL_V1}/plays", {"game_id": game_id})
        return result.get("data", [])

    def get_player_props(self, game_id: int, player_id: int = None) -> List[Dict]:
        """Get live player prop betting lines."""
        params = {"game_id": game_id}
        if player_id:
            params["player_id"] = player_id
        result = self._request(f"{BASE_URL_V2}/odds/player_props", params)
        return result.get("data", [])

    def get_betting_odds(self, game_ids: List[int] = None, dates: List[str] = None) -> List[Dict]:
        """Get betting odds (spreads, moneylines, totals) from multiple sportsbooks."""
        params = {}
        if game_ids:
            params["game_ids[]"] = game_ids
        if dates:
            params["dates[]"] = dates
        return self._paginate(f"{BASE_URL_V2}/odds", params)

    def get_season_averages(self, season: int, category: str, stat_type: str = None,
                            player_ids: List[int] = None, season_type: str = "regular") -> List[Dict]:
        """Get expanded season averages (15+ categories)."""
        params = {"season": season, "season_type": season_type}
        if stat_type:
            params["type"] = stat_type
        if player_ids:
            params["player_ids[]"] = player_ids
        return self._paginate(f"{BASE_URL_NBA_V1}/season_averages/{category}", params)

    def get_team_season_averages(self, season: int, category: str, stat_type: str = None,
                                  team_ids: List[int] = None, season_type: str = "regular") -> List[Dict]:
        """Get team season averages."""
        params = {"season": season, "season_type": season_type}
        if stat_type:
            params["type"] = stat_type
        if team_ids:
            params["team_ids[]"] = team_ids
        return self._paginate(f"{BASE_URL_NBA_V1}/team_season_averages/{category}", params)


# ===========================
# FLATTEN FUNCTIONS
# ===========================

def flatten_game(game: Dict) -> Dict:
    home = game.get("home_team", {}) or {}
    visitor = game.get("visitor_team", {}) or {}
    
    return {
        "game_id": game.get("id"),
        "date": game.get("date"),
        "datetime": game.get("datetime"),
        "season": game.get("season"),
        "status": game.get("status"),
        "period": game.get("period"),
        "time": game.get("time"),
        "postseason": game.get("postseason"),
        "postponed": game.get("postponed"),
        "ist_stage": game.get("ist_stage"),
        "home_team_id": home.get("id"),
        "home_team_abbr": home.get("abbreviation"),
        "home_team_name": home.get("full_name"),
        "home_score": game.get("home_team_score"),
        "home_q1": game.get("home_q1"),
        "home_q2": game.get("home_q2"),
        "home_q3": game.get("home_q3"),
        "home_q4": game.get("home_q4"),
        "home_ot1": game.get("home_ot1"),
        "home_ot2": game.get("home_ot2"),
        "home_ot3": game.get("home_ot3"),
        "home_timeouts": game.get("home_timeouts_remaining"),
        "home_in_bonus": game.get("home_in_bonus"),
        "visitor_team_id": visitor.get("id"),
        "visitor_team_abbr": visitor.get("abbreviation"),
        "visitor_team_name": visitor.get("full_name"),
        "visitor_score": game.get("visitor_team_score"),
        "visitor_q1": game.get("visitor_q1"),
        "visitor_q2": game.get("visitor_q2"),
        "visitor_q3": game.get("visitor_q3"),
        "visitor_q4": game.get("visitor_q4"),
        "visitor_ot1": game.get("visitor_ot1"),
        "visitor_ot2": game.get("visitor_ot2"),
        "visitor_ot3": game.get("visitor_ot3"),
        "visitor_timeouts": game.get("visitor_timeouts_remaining"),
        "visitor_in_bonus": game.get("visitor_in_bonus"),
    }


def flatten_stat(stat: Dict) -> Dict:
    player = stat.get("player", {}) or {}
    team = stat.get("team", {}) or {}
    game = stat.get("game", {}) or {}
    
    return {
        "stat_id": stat.get("id"),
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
        "plus_minus": stat.get("plus_minus"),
    }


def flatten_advanced_stat_v2(stat: Dict) -> Dict:
    player = stat.get("player", {}) or {}
    team = stat.get("team", {}) or {}
    game = stat.get("game", {}) or {}
    
    return {
        "stat_id": stat.get("id"),
        "game_id": game.get("id"),
        "game_date": game.get("date"),
        "player_id": player.get("id"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "period": stat.get("period"),
        # Core
        "pie": stat.get("pie"),
        "pace": stat.get("pace"),
        "pace_per_40": stat.get("pace_per_40"),
        "possessions": stat.get("possessions"),
        "offensive_rating": stat.get("offensive_rating"),
        "defensive_rating": stat.get("defensive_rating"),
        "net_rating": stat.get("net_rating"),
        "usage_percentage": stat.get("usage_percentage"),
        "true_shooting_percentage": stat.get("true_shooting_percentage"),
        "effective_field_goal_percentage": stat.get("effective_field_goal_percentage"),
        "assist_percentage": stat.get("assist_percentage"),
        "assist_ratio": stat.get("assist_ratio"),
        "assist_to_turnover": stat.get("assist_to_turnover"),
        "turnover_ratio": stat.get("turnover_ratio"),
        "offensive_rebound_percentage": stat.get("offensive_rebound_percentage"),
        "defensive_rebound_percentage": stat.get("defensive_rebound_percentage"),
        "rebound_percentage": stat.get("rebound_percentage"),
        # Misc
        "blocks_against": stat.get("blocks_against"),
        "fouls_drawn": stat.get("fouls_drawn"),
        "points_fast_break": stat.get("points_fast_break"),
        "points_off_turnovers": stat.get("points_off_turnovers"),
        "points_paint": stat.get("points_paint"),
        "points_second_chance": stat.get("points_second_chance"),
        # Scoring
        "pct_assisted_fgm": stat.get("pct_assisted_fgm"),
        "pct_unassisted_fgm": stat.get("pct_unassisted_fgm"),
        "pct_fga_2pt": stat.get("pct_fga_2pt"),
        "pct_fga_3pt": stat.get("pct_fga_3pt"),
        "pct_pts_2pt": stat.get("pct_pts_2pt"),
        "pct_pts_3pt": stat.get("pct_pts_3pt"),
        "pct_pts_paint": stat.get("pct_pts_paint"),
        "pct_pts_fast_break": stat.get("pct_pts_fast_break"),
        # Hustle
        "box_outs": stat.get("box_outs"),
        "charges_drawn": stat.get("charges_drawn"),
        "contested_shots": stat.get("contested_shots"),
        "deflections": stat.get("deflections"),
        "loose_balls_recovered_total": stat.get("loose_balls_recovered_total"),
        "screen_assists": stat.get("screen_assists"),
        "screen_assist_points": stat.get("screen_assist_points"),
        # Defense
        "matchup_minutes": stat.get("matchup_minutes"),
        "matchup_fg_pct": stat.get("matchup_fg_pct"),
        "matchup_player_points": stat.get("matchup_player_points"),
        "defended_at_rim_fg_pct": stat.get("defended_at_rim_fg_pct"),
        # Tracking
        "speed": stat.get("speed"),
        "distance": stat.get("distance"),
        "touches": stat.get("touches"),
        "passes": stat.get("passes"),
        "contested_fg_pct": stat.get("contested_fg_pct"),
        "uncontested_fg_pct": stat.get("uncontested_fg_pct"),
        # Usage
        "pct_fga": stat.get("pct_fga"),
        "pct_points": stat.get("pct_points"),
        "pct_rebounds_total": stat.get("pct_rebounds_total"),
    }


def flatten_lineup(lineup: Dict) -> Dict:
    player = lineup.get("player", {}) or {}
    team = lineup.get("team", {}) or {}
    
    return {
        "lineup_id": lineup.get("id"),
        "game_id": lineup.get("game_id"),
        "starter": lineup.get("starter"),
        "position": lineup.get("position"),
        "player_id": player.get("id"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "player_position": player.get("position"),
        "jersey_number": player.get("jersey_number"),
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
    }


def flatten_play(play: Dict) -> Dict:
    team = play.get("team", {}) or {}
    
    return {
        "game_id": play.get("game_id"),
        "order": play.get("order"),
        "type": play.get("type"),
        "text": play.get("text"),
        "home_score": play.get("home_score"),
        "away_score": play.get("away_score"),
        "period": play.get("period"),
        "clock": play.get("clock"),
        "scoring_play": play.get("scoring_play"),
        "shooting_play": play.get("shooting_play"),
        "score_value": play.get("score_value"),
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "coordinate_x": play.get("coordinate_x"),
        "coordinate_y": play.get("coordinate_y"),
    }


def flatten_player_prop(prop: Dict) -> Dict:
    market = prop.get("market", {}) or {}
    
    flat = {
        "prop_id": prop.get("id"),
        "game_id": prop.get("game_id"),
        "player_id": prop.get("player_id"),
        "vendor": prop.get("vendor"),
        "prop_type": prop.get("prop_type"),
        "line_value": prop.get("line_value"),
        "market_type": market.get("type"),
        "over_odds": market.get("over_odds"),
        "under_odds": market.get("under_odds"),
        "milestone_odds": market.get("odds"),
        "updated_at": prop.get("updated_at"),
    }
    return flat


def flatten_betting_odds(odds: Dict) -> Dict:
    return {
        "odds_id": odds.get("id"),
        "game_id": odds.get("game_id"),
        "vendor": odds.get("vendor"),
        "spread_home_value": odds.get("spread_home_value"),
        "spread_home_odds": odds.get("spread_home_odds"),
        "spread_away_value": odds.get("spread_away_value"),
        "spread_away_odds": odds.get("spread_away_odds"),
        "moneyline_home_odds": odds.get("moneyline_home_odds"),
        "moneyline_away_odds": odds.get("moneyline_away_odds"),
        "total_value": odds.get("total_value"),
        "total_over_odds": odds.get("total_over_odds"),
        "total_under_odds": odds.get("total_under_odds"),
        "updated_at": odds.get("updated_at"),
    }


def flatten_season_average(avg: Dict, category: str, stat_type: str) -> Dict:
    player = avg.get("player", {}) or {}
    stats = avg.get("stats", {}) or {}
    
    flat = {
        "player_id": player.get("id"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "season": avg.get("season"),
        "season_type": avg.get("season_type"),
        "category": category,
        "stat_type": stat_type,
    }
    for key, value in stats.items():
        flat[key] = value
    return flat


def flatten_team_season_average(avg: Dict, category: str, stat_type: str) -> Dict:
    team = avg.get("team", {}) or {}
    stats = avg.get("stats", {}) or {}
    
    flat = {
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "team_name": team.get("full_name"),
        "season": avg.get("season"),
        "season_type": avg.get("season_type"),
        "category": category,
        "stat_type": stat_type,
    }
    for key, value in stats.items():
        flat[key] = value
    return flat


def flatten_standing(standing: Dict) -> Dict:
    team = standing.get("team", {}) or {}
    return {
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "team_name": team.get("full_name"),
        "team_conference": team.get("conference"),
        "wins": standing.get("wins"),
        "losses": standing.get("losses"),
        "conference_rank": standing.get("conference_rank"),
        "division_rank": standing.get("division_rank"),
        "home_record": standing.get("home_record"),
        "road_record": standing.get("road_record"),
        "season": standing.get("season"),
    }


def flatten_injury(injury: Dict) -> Dict:
    player = injury.get("player", {}) or {}
    return {
        "player_id": player.get("id"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "team_id": player.get("team_id"),
        "status": injury.get("status"),
        "return_date": injury.get("return_date"),
        "description": injury.get("description"),
    }


def flatten_leader(leader: Dict) -> Dict:
    player = leader.get("player", {}) or {}
    return {
        "player_id": player.get("id"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "team_id": player.get("team_id"),
        "stat_type": leader.get("stat_type"),
        "value": leader.get("value"),
        "rank": leader.get("rank"),
        "games_played": leader.get("games_played"),
        "season": leader.get("season"),
    }


def flatten_team(team: Dict) -> Dict:
    return {
        "team_id": team.get("id"),
        "abbreviation": team.get("abbreviation"),
        "full_name": team.get("full_name"),
        "city": team.get("city"),
        "conference": team.get("conference"),
        "division": team.get("division"),
    }


# ===========================
# SAVE FUNCTION
# ===========================

def save_df(df: pd.DataFrame, filename: str, output_dir: str):
    if df.empty:
        print(f"  ⚠️  No data for {filename}")
        return
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(f"{output_dir}/{filename}.csv", index=False)
    df.to_parquet(f"{output_dir}/{filename}.parquet", index=False)
    print(f"  ✅ {len(df):,} records → {filename}")


# ===========================
# BACKFILL FUNCTIONS
# ===========================

def backfill_games(client, start_date, end_date, season, output_dir, team_id=None, team_abbr=None):
    label = f" ({team_abbr})" if team_abbr else ""
    print(f"\n📅 GAMES{label}: {start_date} to {end_date}")
    
    games = client.get_games(start_date, end_date, season, team_id)
    if not games:
        print("  No games found")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_game(g) for g in games])
    suffix = f"_{team_abbr}" if team_abbr else ""
    save_df(df, f"games{suffix}_{start_date}_{end_date}", output_dir)
    return df


def backfill_stats(client, output_dir, game_ids=None, start_date=None, end_date=None, 
                   season=None, team_id=None, team_abbr=None):
    label = f" ({team_abbr})" if team_abbr else ""
    print(f"\n📊 PLAYER STATS{label}")
    
    stats = client.get_stats(game_ids, start_date, end_date, season)
    if not stats:
        print("  No stats found")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_stat(s) for s in stats])
    
    # Filter to team if specified
    if team_id:
        team_df = df[df["team_id"] == team_id].copy()
        opp_df = df[df["team_id"] != team_id].copy()
        suffix = f"_{team_abbr}" if team_abbr else ""
        save_df(team_df, f"player_stats{suffix}_{start_date}_{end_date}", output_dir)
        save_df(opp_df, f"opponent_stats{suffix}_{start_date}_{end_date}", output_dir)
        return team_df
    else:
        save_df(df, f"player_stats_{start_date}_{end_date}", output_dir)
        return df


def backfill_advanced_stats_v2(client, output_dir, game_ids=None, start_date=None, end_date=None, 
                                season=None, team_id=None, team_abbr=None):
    label = f" ({team_abbr})" if team_abbr else ""
    print(f"\n📈 ADVANCED STATS V2{label}")
    
    stats = client.get_advanced_stats_v2(game_ids, start_date, end_date, season, period=0)
    if not stats:
        print("  No advanced stats found")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_advanced_stat_v2(s) for s in stats])
    
    if team_id:
        df = df[df["team_id"] == team_id].copy()
    
    suffix = f"_{team_abbr}" if team_abbr else ""
    save_df(df, f"advanced_stats_v2{suffix}_{start_date}_{end_date}", output_dir)
    return df


def backfill_lineups(client, game_ids, output_dir, start_date=None, end_date=None, 
                     team_id=None, team_abbr=None):
    label = f" ({team_abbr})" if team_abbr else ""
    print(f"\n👥 LINEUPS{label} for {len(game_ids)} games")
    
    lineups = client.get_lineups(game_ids)
    if not lineups:
        print("  No lineups found")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_lineup(l) for l in lineups])
    
    if team_id:
        df = df[df["team_id"] == team_id].copy()
    
    suffix = f"_{team_abbr}" if team_abbr else ""
    save_df(df, f"lineups{suffix}_{start_date}_{end_date}", output_dir)
    return df


def backfill_play_by_play(client, game_ids, output_dir, start_date=None, end_date=None,
                          team_id=None, team_abbr=None):
    label = f" ({team_abbr})" if team_abbr else ""
    print(f"\n🎬 PLAY-BY-PLAY{label} for {len(game_ids)} games")
    
    all_plays = []
    for i, game_id in enumerate(game_ids):
        plays = client.get_play_by_play(game_id)
        all_plays.extend(plays)
        if (i + 1) % 10 == 0:
            print(f"    {i+1}/{len(game_ids)} games ({len(all_plays):,} plays)")
    
    if not all_plays:
        print("  No play-by-play found")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_play(p) for p in all_plays])
    suffix = f"_{team_abbr}" if team_abbr else ""
    save_df(df, f"play_by_play{suffix}_{start_date}_{end_date}", output_dir)
    return df


def backfill_player_props(client, game_ids, output_dir, start_date=None, end_date=None,
                          team_abbr=None):
    label = f" ({team_abbr})" if team_abbr else ""
    print(f"\n💰 PLAYER PROPS{label} for {len(game_ids)} games")
    
    all_props = []
    for i, game_id in enumerate(game_ids):
        props = client.get_player_props(game_id)
        all_props.extend(props)
        if (i + 1) % 5 == 0:
            print(f"    {i+1}/{len(game_ids)} games ({len(all_props):,} props)")
    
    if not all_props:
        print("  No props found (removed after games end)")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_player_prop(p) for p in all_props])
    suffix = f"_{team_abbr}" if team_abbr else ""
    save_df(df, f"player_props{suffix}_{start_date}_{end_date}", output_dir)
    return df


def backfill_betting_odds(client, game_ids, output_dir, start_date=None, end_date=None,
                          dates=None, team_abbr=None):
    label = f" ({team_abbr})" if team_abbr else ""
    print(f"\n📈 BETTING ODDS{label}")
    
    odds = client.get_betting_odds(game_ids, dates)
    if not odds:
        print("  No betting odds found")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_betting_odds(o) for o in odds])
    suffix = f"_{team_abbr}" if team_abbr else ""
    save_df(df, f"betting_odds{suffix}_{start_date}_{end_date}", output_dir)
    return df


def backfill_season_averages(client, season, output_dir, team_id=None, team_abbr=None):
    label = f" ({team_abbr})" if team_abbr else ""
    print(f"\n📊 SEASON AVERAGES{label} for {season}")
    
    categories = [
        ("general", "base"), ("general", "advanced"), ("general", "scoring"),
        ("general", "defense"), ("general", "usage"),
        ("clutch", "base"), ("clutch", "advanced"),
        ("shooting", "5ft_range"), ("shooting", "by_zone"),
        ("playtype", "isolation"), ("playtype", "prballhandler"), 
        ("playtype", "spotup"), ("playtype", "transition"),
        ("tracking", "drives"), ("tracking", "passing"), 
        ("tracking", "rebounding"), ("tracking", "defense"),
        ("hustle", None),
    ]
    
    # Get player IDs for team if filtering
    player_ids = None
    if team_id:
        players = client.get_active_players([team_id])
        player_ids = [p.get("id") for p in players if p.get("id")]
        print(f"    Found {len(player_ids)} players for {team_abbr}")
    
    all_avgs = []
    for category, stat_type in categories:
        try:
            print(f"    {category}/{stat_type or 'default'}...")
            avgs = client.get_season_averages(season, category, stat_type, player_ids)
            for avg in avgs:
                all_avgs.append(flatten_season_average(avg, category, stat_type or "default"))
        except Exception as e:
            print(f"    ⚠️  {category}/{stat_type}: {e}")
    
    if not all_avgs:
        print("  No season averages found")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_avgs)
    suffix = f"_{team_abbr}" if team_abbr else ""
    save_df(df, f"season_averages{suffix}_{season}", output_dir)
    return df


def backfill_team_season_averages(client, season, output_dir, team_id=None, team_abbr=None):
    label = f" ({team_abbr})" if team_abbr else ""
    print(f"\n🏀 TEAM SEASON AVERAGES{label} for {season}")
    
    categories = [
        ("general", "base"), ("general", "advanced"), ("general", "scoring"),
        ("general", "opponent"), ("general", "defense"),
        ("shooting", "5ft_range_base"), ("shooting", "by_zone_base"),
        ("playtype", "isolation"), ("playtype", "transition"),
        ("tracking", "defense"), ("tracking", "rebounding"),
        ("hustle", None),
    ]
    
    team_ids = [team_id] if team_id else None
    
    all_avgs = []
    for category, stat_type in categories:
        try:
            print(f"    {category}/{stat_type or 'default'}...")
            avgs = client.get_team_season_averages(season, category, stat_type, team_ids)
            for avg in avgs:
                all_avgs.append(flatten_team_season_average(avg, category, stat_type or "default"))
        except Exception as e:
            print(f"    ⚠️  {category}/{stat_type}: {e}")
    
    if not all_avgs:
        return pd.DataFrame()
    
    df = pd.DataFrame(all_avgs)
    suffix = f"_{team_abbr}" if team_abbr else ""
    save_df(df, f"team_season_averages{suffix}_{season}", output_dir)
    return df


def backfill_standings(client, season, output_dir):
    print(f"\n🏆 STANDINGS for {season}")
    standings = client.get_standings(season)
    if not standings:
        return pd.DataFrame()
    df = pd.DataFrame([flatten_standing(s) for s in standings])
    save_df(df, f"standings_{season}", output_dir)
    return df


def backfill_injuries(client, output_dir, team_id=None, team_abbr=None):
    label = f" ({team_abbr})" if team_abbr else ""
    print(f"\n🏥 INJURIES{label}")
    
    team_ids = [team_id] if team_id else None
    injuries = client.get_injuries(team_ids)
    if not injuries:
        print("  No injuries found")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_injury(i) for i in injuries])
    suffix = f"_{team_abbr}" if team_abbr else ""
    today = datetime.now().strftime("%Y-%m-%d")
    save_df(df, f"injuries{suffix}_{today}", output_dir)
    return df


def backfill_leaders(client, season, output_dir):
    print(f"\n🌟 LEADERS for {season}")
    
    stat_types = ["pts", "reb", "ast", "stl", "blk", "fg_pct", "fg3_pct", "ft_pct"]
    all_leaders = []
    
    for stat_type in stat_types:
        print(f"    {stat_type}...")
        leaders = client.get_leaders(season, stat_type)
        all_leaders.extend(leaders)
    
    if not all_leaders:
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_leader(l) for l in all_leaders])
    save_df(df, f"leaders_{season}", output_dir)
    return df


def backfill_teams(client, output_dir):
    print(f"\n🏀 TEAMS")
    teams = client.get_teams()
    if not teams:
        return pd.DataFrame()
    df = pd.DataFrame([flatten_team(t) for t in teams])
    df = df[df["team_id"] <= 30]  # Current teams only
    save_df(df, "teams", output_dir)
    return df


# ===========================
# MAIN MODES
# ===========================

def run_full_backfill(client, start_date, end_date, season, output_dir, team_id=None, team_abbr=None):
    label = f" for {team_abbr}" if team_abbr else ""
    print("=" * 70)
    print(f"🚀 FULL BACKFILL{label}: {start_date} to {end_date} (Season {season})")
    print("=" * 70)
    
    start_time = time.time()
    
    # Games
    games_df = backfill_games(client, start_date, end_date, season, output_dir, team_id, team_abbr)
    game_ids = games_df["game_id"].tolist() if not games_df.empty else []
    
    if game_ids:
        backfill_stats(client, output_dir, game_ids, start_date, end_date, season, team_id, team_abbr)
        backfill_advanced_stats_v2(client, output_dir, game_ids, start_date, end_date, season, team_id, team_abbr)
        backfill_lineups(client, game_ids, output_dir, start_date, end_date, team_id, team_abbr)
        backfill_play_by_play(client, game_ids, output_dir, start_date, end_date, team_id, team_abbr)
        backfill_player_props(client, game_ids, output_dir, start_date, end_date, team_abbr)
        backfill_betting_odds(client, game_ids, output_dir, start_date, end_date, team_abbr=team_abbr)
    
    backfill_standings(client, season, output_dir)
    backfill_injuries(client, output_dir, team_id, team_abbr)
    backfill_season_averages(client, season, output_dir, team_id, team_abbr)
    backfill_team_season_averages(client, season, output_dir, team_id, team_abbr)
    
    if not team_id:
        backfill_leaders(client, season, output_dir)
        backfill_teams(client, output_dir)
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"✅ COMPLETE | {elapsed:.1f}s | {client.request_count:,} requests | {output_dir}/")
    print("=" * 70)


def run_daily_backfill(client, start_date, end_date, season, output_dir, team_id=None, team_abbr=None):
    label = f" for {team_abbr}" if team_abbr else ""
    print("=" * 70)
    print(f"📅 DAILY BACKFILL{label}: {start_date} to {end_date}")
    print("=" * 70)
    
    start_time = time.time()
    
    games_df = backfill_games(client, start_date, end_date, season, output_dir, team_id, team_abbr)
    game_ids = games_df["game_id"].tolist() if not games_df.empty else []
    
    if game_ids:
        backfill_stats(client, output_dir, game_ids, start_date, end_date, season, team_id, team_abbr)
        backfill_advanced_stats_v2(client, output_dir, game_ids, start_date, end_date, season, team_id, team_abbr)
        backfill_betting_odds(client, game_ids, output_dir, start_date, end_date, team_abbr=team_abbr)
    
    backfill_standings(client, season, output_dir)
    backfill_injuries(client, output_dir, team_id, team_abbr)
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"✅ COMPLETE | {elapsed:.1f}s | {client.request_count:,} requests")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="NBA BallDontLie V2 - Complete data collection with team filtering",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
TEAM ABBREVIATIONS:
  ATL BOS BKN CHA CHI CLE DAL DEN DET GSW
  HOU IND LAC LAL MEM MIA MIL MIN NOP NYK
  OKC ORL PHI PHX POR SAC SAS TOR UTA WAS

EXAMPLES:
  # Full backfill - all teams
  python nba_balldontlie_backfill_v2.py --start 2025-10-22 --end 2025-01-26 --season 2025 --full

  # Full backfill - Lakers only  
  python nba_balldontlie_backfill_v2.py --start 2025-10-22 --end 2025-01-26 --season 2025 --full --team LAL

  # Daily update
  python nba_balldontlie_backfill_v2.py --start 2025-01-26 --end 2025-01-26 --season 2025 --daily

  # Specific endpoints
  python nba_balldontlie_backfill_v2.py --start 2025-01-26 --end 2025-01-26 --season 2025 --games --stats --odds

  # Season averages for Celtics
  python nba_balldontlie_backfill_v2.py --season 2025 --season-averages --team BOS
        """
    )
    
    # Date/season
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--season", type=int, default=2025, help="Season year (default: 2025)")
    
    # Team filter
    parser.add_argument("--team", type=str, help="Team abbreviation (e.g., LAL, BOS, GSW)")
    parser.add_argument("--team-id", type=int, help="Team ID (1-30)")
    
    # Modes
    parser.add_argument("--full", action="store_true", help="Full backfill (all endpoints)")
    parser.add_argument("--daily", action="store_true", help="Daily backfill (essential endpoints)")
    
    # Individual endpoints
    parser.add_argument("--games", action="store_true", help="Games")
    parser.add_argument("--stats", action="store_true", help="Player stats")
    parser.add_argument("--advanced-v2", action="store_true", help="Advanced stats V2 (100+ metrics)")
    parser.add_argument("--lineups", action="store_true", help="Lineups")
    parser.add_argument("--pbp", action="store_true", help="Play-by-play")
    parser.add_argument("--player-props", action="store_true", help="Player props")
    parser.add_argument("--odds", action="store_true", help="Betting odds")
    parser.add_argument("--standings", action="store_true", help="Standings")
    parser.add_argument("--injuries", action="store_true", help="Injuries")
    parser.add_argument("--leaders", action="store_true", help="Leaders")
    parser.add_argument("--season-averages", action="store_true", help="Season averages")
    parser.add_argument("--team-season-averages", action="store_true", help="Team season averages")
    parser.add_argument("--teams", action="store_true", help="Teams reference")
    
    # Output
    parser.add_argument("--output", type=str, default="data", help="Output directory")
    
    args = parser.parse_args()
    
    if not API_KEY:
        print("❌ BALLDONTLIE_API_KEY not found in .env")
        return
    
    # Resolve team
    team_id = None
    team_abbr = None
    
    if args.team:
        team_abbr = args.team.upper()
        if team_abbr not in TEAM_IDS:
            print(f"❌ Unknown team: {team_abbr}")
            print(f"   Valid: {', '.join(sorted(TEAM_IDS.keys()))}")
            return
        team_id = TEAM_IDS[team_abbr]
    elif args.team_id:
        if args.team_id < 1 or args.team_id > 30:
            print(f"❌ Invalid team ID: {args.team_id}")
            return
        team_id = args.team_id
        team_abbr = TEAM_NAMES.get(team_id)
    
    client = BallDontLieClient(API_KEY)
    output_dir = args.output
    os.makedirs(output_dir, exist_ok=True)
    
    today = datetime.now().strftime("%Y-%m-%d")
    start_date = args.start or today
    end_date = args.end or today
    
    # Modes
    if args.full:
        run_full_backfill(client, start_date, end_date, args.season, output_dir, team_id, team_abbr)
    elif args.daily:
        run_daily_backfill(client, start_date, end_date, args.season, output_dir, team_id, team_abbr)
    else:
        # Individual endpoints
        game_ids = []
        
        if args.games:
            games_df = backfill_games(client, start_date, end_date, args.season, output_dir, team_id, team_abbr)
            if not games_df.empty:
                game_ids = games_df["game_id"].tolist()
        
        if args.stats:
            backfill_stats(client, output_dir, game_ids or None, start_date, end_date, args.season, team_id, team_abbr)
        
        if args.advanced_v2:
            backfill_advanced_stats_v2(client, output_dir, game_ids or None, start_date, end_date, args.season, team_id, team_abbr)
        
        if args.lineups and game_ids:
            backfill_lineups(client, game_ids, output_dir, start_date, end_date, team_id, team_abbr)
        
        if args.pbp and game_ids:
            backfill_play_by_play(client, game_ids, output_dir, start_date, end_date, team_id, team_abbr)
        
        if args.player_props and game_ids:
            backfill_player_props(client, game_ids, output_dir, start_date, end_date, team_abbr)
        
        if args.odds and game_ids:
            backfill_betting_odds(client, game_ids, output_dir, start_date, end_date, team_abbr=team_abbr)
        
        if args.standings:
            backfill_standings(client, args.season, output_dir)
        
        if args.injuries:
            backfill_injuries(client, output_dir, team_id, team_abbr)
        
        if args.leaders:
            backfill_leaders(client, args.season, output_dir)
        
        if args.season_averages:
            backfill_season_averages(client, args.season, output_dir, team_id, team_abbr)
        
        if args.team_season_averages:
            backfill_team_season_averages(client, args.season, output_dir, team_id, team_abbr)
        
        if args.teams:
            backfill_teams(client, output_dir)
        
        # No flags = help
        if not any([args.games, args.stats, args.advanced_v2, args.lineups, args.pbp,
                    args.player_props, args.odds, args.standings, args.injuries,
                    args.leaders, args.season_averages, args.team_season_averages, args.teams]):
            parser.print_help()


if __name__ == "__main__":
    main()