#!/usr/bin/env python3
"""
NBA BallDontLie Data Backfill Script (V2 - Comprehensive)
==========================================================
Collects ALL available NBA data from BallDontLie API (GOAT tier)

ENDPOINTS INCLUDED:
  Core Data:
  - Games (with quarter scores, OT, timeouts)
  - Player Stats (box scores)
  - Teams, Players, Active Players
  - Standings, Leaders, Injuries

  V2 Advanced (NEW):
  - Advanced Stats V2 (100+ metrics: hustle, tracking, defense)
  - Lineups (starters + bench, 2025+ only)
  - Play-by-Play (shot locations, play types, 2025+ only)
  - Player Props (live betting lines)
  - Season Averages (15+ categories)
  - Team Season Averages

Usage:
  # Full backfill with all new endpoints
  python nba_balldontlie_backfill_v2.py --start 2025-10-22 --end 2025-01-26 --season 2025 --full

  # Daily update
  python nba_balldontlie_backfill_v2.py --start 2025-01-26 --end 2025-01-26 --season 2025 --daily

  # Just new V2 endpoints
  python nba_balldontlie_backfill_v2.py --start 2025-01-26 --end 2025-01-26 --season 2025 --advanced-v2 --lineups --pbp --player-props

  # Season averages only
  python nba_balldontlie_backfill_v2.py --season 2025 --season-averages --team-season-averages

Author: Jose
Last Updated: January 2026
"""

import os
import time
import argparse
from datetime import datetime
from typing import Optional, List, Dict, Any

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


class BallDontLieClient:
    """API client for BallDontLie - supports V1 and V2 endpoints."""

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
    # V1 ENDPOINTS (Original)
    # ===========================

    def get_teams(self) -> List[Dict]:
        return self._request(f"{BASE_URL_V1}/teams").get("data", [])

    def get_games(self, start_date: str, end_date: str, season: int = None) -> List[Dict]:
        params = {"start_date": start_date, "end_date": end_date}
        if season:
            params["seasons[]"] = season
        return self._paginate(f"{BASE_URL_V1}/games", params)

    def get_stats(self, game_ids: List[int] = None, start_date: str = None, 
                  end_date: str = None, season: int = None) -> List[Dict]:
        if game_ids:
            all_stats = []
            batch_size = 25
            for i in range(0, len(game_ids), batch_size):
                batch = game_ids[i:i+batch_size]
                params = {"game_ids[]": batch}
                stats = self._paginate(f"{BASE_URL_V1}/stats", params)
                all_stats.extend(stats)
                print(f"    Games {i+1}-{min(i+batch_size, len(game_ids))}: {len(stats)} stats")
            return all_stats
        else:
            params = {}
            if start_date: params["start_date"] = start_date
            if end_date: params["end_date"] = end_date
            if season: params["seasons[]"] = season
            return self._paginate(f"{BASE_URL_V1}/stats", params)

    def get_standings(self, season: int) -> List[Dict]:
        return self._paginate(f"{BASE_URL_V1}/standings", {"season": season})

    def get_leaders(self, season: int, stat_type: str = None) -> List[Dict]:
        params = {"season": season}
        if stat_type:
            params["stat_type"] = stat_type
        return self._paginate(f"{BASE_URL_V1}/leaders", params)

    def get_injuries(self) -> List[Dict]:
        return self._paginate(f"{BASE_URL_V1}/player_injuries", {})

    def get_active_players(self) -> List[Dict]:
        return self._paginate(f"{BASE_URL_V1}/players/active", {})

    # ===========================
    # V2 ENDPOINTS (NEW)
    # ===========================

    def get_advanced_stats_v2(self, game_ids: List[int] = None, start_date: str = None,
                               end_date: str = None, season: int = None, period: int = 0) -> List[Dict]:
        """Get comprehensive advanced stats (V2) - 100+ metrics."""
        if game_ids:
            all_stats = []
            batch_size = 25
            for i in range(0, len(game_ids), batch_size):
                batch = game_ids[i:i+batch_size]
                params = {"game_ids[]": batch, "period": period}
                stats = self._paginate(f"{BASE_URL_NBA_V2}/stats/advanced", params)
                all_stats.extend(stats)
                print(f"    Games {i+1}-{min(i+batch_size, len(game_ids))}: {len(stats)} adv stats v2")
            return all_stats
        else:
            params = {"period": period}
            if start_date: params["start_date"] = start_date
            if end_date: params["end_date"] = end_date
            if season: params["seasons[]"] = season
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
            if (i + batch_size) % 50 == 0:
                print(f"    Processed {min(i+batch_size, len(game_ids))}/{len(game_ids)} games")
        return all_lineups

    def get_play_by_play(self, game_id: int) -> List[Dict]:
        """Get play-by-play data for a game (2025+ only)."""
        result = self._request(f"{BASE_URL_V1}/plays", {"game_id": game_id})
        return result.get("data", [])

    def get_player_props(self, game_id: int) -> List[Dict]:
        """Get live player prop betting lines."""
        result = self._request(f"{BASE_URL_V2}/odds/player_props", {"game_id": game_id})
        return result.get("data", [])

    def get_betting_odds(self, game_ids: List[int] = None, dates: List[str] = None) -> List[Dict]:
        """Get betting odds from multiple sportsbooks."""
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
    """Flatten game with all new fields (quarter scores, OT, timeouts)."""
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
        # Home team
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
        # Visitor team
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
    """Flatten player stat."""
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
    """Flatten V2 advanced stats - 100+ metrics."""
    player = stat.get("player", {}) or {}
    team = stat.get("team", {}) or {}
    game = stat.get("game", {}) or {}
    
    flat = {
        "stat_id": stat.get("id"),
        "game_id": game.get("id"),
        "game_date": game.get("date"),
        "player_id": player.get("id"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "period": stat.get("period"),
        # Core advanced
        "pie": stat.get("pie"),
        "pace": stat.get("pace"),
        "pace_per_40": stat.get("pace_per_40"),
        "possessions": stat.get("possessions"),
        "offensive_rating": stat.get("offensive_rating"),
        "defensive_rating": stat.get("defensive_rating"),
        "net_rating": stat.get("net_rating"),
        "estimated_offensive_rating": stat.get("estimated_offensive_rating"),
        "estimated_defensive_rating": stat.get("estimated_defensive_rating"),
        "estimated_net_rating": stat.get("estimated_net_rating"),
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
        # Scoring breakdown
        "pct_assisted_2pt": stat.get("pct_assisted_2pt"),
        "pct_assisted_3pt": stat.get("pct_assisted_3pt"),
        "pct_assisted_fgm": stat.get("pct_assisted_fgm"),
        "pct_unassisted_2pt": stat.get("pct_unassisted_2pt"),
        "pct_unassisted_3pt": stat.get("pct_unassisted_3pt"),
        "pct_fga_2pt": stat.get("pct_fga_2pt"),
        "pct_fga_3pt": stat.get("pct_fga_3pt"),
        "pct_pts_2pt": stat.get("pct_pts_2pt"),
        "pct_pts_3pt": stat.get("pct_pts_3pt"),
        "pct_pts_fast_break": stat.get("pct_pts_fast_break"),
        "pct_pts_free_throw": stat.get("pct_pts_free_throw"),
        "pct_pts_paint": stat.get("pct_pts_paint"),
        # Hustle
        "box_outs": stat.get("box_outs"),
        "charges_drawn": stat.get("charges_drawn"),
        "contested_shots": stat.get("contested_shots"),
        "contested_shots_2pt": stat.get("contested_shots_2pt"),
        "contested_shots_3pt": stat.get("contested_shots_3pt"),
        "deflections": stat.get("deflections"),
        "loose_balls_recovered_def": stat.get("loose_balls_recovered_def"),
        "loose_balls_recovered_off": stat.get("loose_balls_recovered_off"),
        "loose_balls_recovered_total": stat.get("loose_balls_recovered_total"),
        "screen_assists": stat.get("screen_assists"),
        "screen_assist_points": stat.get("screen_assist_points"),
        # Defense matchup
        "matchup_minutes": stat.get("matchup_minutes"),
        "matchup_fg_pct": stat.get("matchup_fg_pct"),
        "matchup_fga": stat.get("matchup_fga"),
        "matchup_fgm": stat.get("matchup_fgm"),
        "matchup_player_points": stat.get("matchup_player_points"),
        "defended_at_rim_fga": stat.get("defended_at_rim_fga"),
        "defended_at_rim_fgm": stat.get("defended_at_rim_fgm"),
        "defended_at_rim_fg_pct": stat.get("defended_at_rim_fg_pct"),
        # Tracking
        "speed": stat.get("speed"),
        "distance": stat.get("distance"),
        "touches": stat.get("touches"),
        "passes": stat.get("passes"),
        "secondary_assists": stat.get("secondary_assists"),
        "contested_fga": stat.get("contested_fga"),
        "contested_fgm": stat.get("contested_fgm"),
        "contested_fg_pct": stat.get("contested_fg_pct"),
        "uncontested_fga": stat.get("uncontested_fga"),
        "uncontested_fgm": stat.get("uncontested_fgm"),
        "uncontested_fg_pct": stat.get("uncontested_fg_pct"),
        # Usage percentages
        "pct_fga": stat.get("pct_fga"),
        "pct_fgm": stat.get("pct_fgm"),
        "pct_points": stat.get("pct_points"),
        "pct_rebounds_total": stat.get("pct_rebounds_total"),
        "pct_steals": stat.get("pct_steals"),
        "pct_blocks": stat.get("pct_blocks"),
        "pct_turnovers": stat.get("pct_turnovers"),
    }
    return flat


def flatten_lineup(lineup: Dict) -> Dict:
    """Flatten lineup record."""
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
    """Flatten play-by-play record."""
    team = play.get("team", {}) or {}
    
    return {
        "game_id": play.get("game_id"),
        "order": play.get("order"),
        "type": play.get("type"),
        "text": play.get("text"),
        "home_score": play.get("home_score"),
        "away_score": play.get("away_score"),
        "period": play.get("period"),
        "period_display": play.get("period_display"),
        "clock": play.get("clock"),
        "scoring_play": play.get("scoring_play"),
        "shooting_play": play.get("shooting_play"),
        "score_value": play.get("score_value"),
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "coordinate_x": play.get("coordinate_x"),
        "coordinate_y": play.get("coordinate_y"),
        "wallclock": play.get("wallclock"),
    }


def flatten_player_prop(prop: Dict) -> Dict:
    """Flatten player prop betting line."""
    market = prop.get("market", {}) or {}
    
    flat = {
        "prop_id": prop.get("id"),
        "game_id": prop.get("game_id"),
        "player_id": prop.get("player_id"),
        "vendor": prop.get("vendor"),
        "prop_type": prop.get("prop_type"),
        "line_value": prop.get("line_value"),
        "market_type": market.get("type"),
        "updated_at": prop.get("updated_at"),
    }
    
    # Over/under markets
    if market.get("type") == "over_under":
        flat["over_odds"] = market.get("over_odds")
        flat["under_odds"] = market.get("under_odds")
    # Milestone markets
    elif market.get("type") == "milestone":
        flat["milestone_odds"] = market.get("odds")
    
    return flat


def flatten_season_average(avg: Dict, category: str, stat_type: str) -> Dict:
    """Flatten season average record."""
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
    
    # Add all stats from the stats dict
    for key, value in stats.items():
        flat[key] = value
    
    return flat


def flatten_team_season_average(avg: Dict, category: str, stat_type: str) -> Dict:
    """Flatten team season average record."""
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
    """Flatten standing record."""
    team = standing.get("team", {}) or {}
    
    return {
        "team_id": team.get("id"),
        "team_abbr": team.get("abbreviation"),
        "team_name": team.get("full_name"),
        "team_conference": team.get("conference"),
        "team_division": team.get("division"),
        "wins": standing.get("wins"),
        "losses": standing.get("losses"),
        "conference_rank": standing.get("conference_rank"),
        "conference_record": standing.get("conference_record"),
        "division_rank": standing.get("division_rank"),
        "division_record": standing.get("division_record"),
        "home_record": standing.get("home_record"),
        "road_record": standing.get("road_record"),
        "season": standing.get("season"),
    }


def flatten_injury(injury: Dict) -> Dict:
    """Flatten injury record."""
    player = injury.get("player", {}) or {}
    
    return {
        "player_id": player.get("id"),
        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
        "team_id": player.get("team_id"),
        "status": injury.get("status"),
        "return_date": injury.get("return_date"),
        "description": injury.get("description"),
    }


# ===========================
# SAVE FUNCTIONS
# ===========================

def save_df(df: pd.DataFrame, filename: str, output_dir: str):
    """Save DataFrame to CSV and Parquet."""
    if df.empty:
        print(f"  ⚠️  No data for {filename}")
        return
    
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(f"{output_dir}/{filename}.csv", index=False)
    df.to_parquet(f"{output_dir}/{filename}.parquet", index=False)
    print(f"  ✅ Saved {len(df):,} records → {filename}")


# ===========================
# BACKFILL FUNCTIONS
# ===========================

def backfill_games(client: BallDontLieClient, start_date: str, end_date: str,
                   season: int, output_dir: str) -> pd.DataFrame:
    """Backfill games."""
    print(f"\n📅 GAMES: {start_date} to {end_date}")
    games = client.get_games(start_date, end_date, season)
    
    if not games:
        print("  No games found")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_game(g) for g in games])
    save_df(df, f"games_{start_date}_{end_date}", output_dir)
    return df


def backfill_stats(client: BallDontLieClient, output_dir: str, game_ids: List[int] = None,
                   start_date: str = None, end_date: str = None, season: int = None) -> pd.DataFrame:
    """Backfill player stats."""
    print(f"\n📊 PLAYER STATS")
    stats = client.get_stats(game_ids, start_date, end_date, season)
    
    if not stats:
        print("  No stats found")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_stat(s) for s in stats])
    filename = f"player_stats_{start_date}_{end_date}" if start_date else "player_stats"
    save_df(df, filename, output_dir)
    return df


def backfill_advanced_stats_v2(client: BallDontLieClient, output_dir: str, game_ids: List[int] = None,
                                start_date: str = None, end_date: str = None, season: int = None) -> pd.DataFrame:
    """Backfill V2 advanced stats (100+ metrics)."""
    print(f"\n📈 ADVANCED STATS V2 (hustle, tracking, defense)")
    stats = client.get_advanced_stats_v2(game_ids, start_date, end_date, season, period=0)
    
    if not stats:
        print("  No advanced stats found")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_advanced_stat_v2(s) for s in stats])
    filename = f"advanced_stats_v2_{start_date}_{end_date}" if start_date else "advanced_stats_v2"
    save_df(df, filename, output_dir)
    return df


def backfill_lineups(client: BallDontLieClient, game_ids: List[int], output_dir: str,
                     start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Backfill lineups (starters + bench)."""
    print(f"\n👥 LINEUPS for {len(game_ids)} games")
    lineups = client.get_lineups(game_ids)
    
    if not lineups:
        print("  No lineups found (may not be available yet)")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_lineup(l) for l in lineups])
    filename = f"lineups_{start_date}_{end_date}" if start_date else "lineups"
    save_df(df, filename, output_dir)
    return df


def backfill_play_by_play(client: BallDontLieClient, game_ids: List[int], output_dir: str,
                          start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Backfill play-by-play data."""
    print(f"\n🎬 PLAY-BY-PLAY for {len(game_ids)} games")
    all_plays = []
    
    for i, game_id in enumerate(game_ids):
        plays = client.get_play_by_play(game_id)
        all_plays.extend(plays)
        
        if (i + 1) % 10 == 0:
            print(f"    Processed {i+1}/{len(game_ids)} games ({len(all_plays):,} plays)")
    
    if not all_plays:
        print("  No play-by-play found")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_play(p) for p in all_plays])
    filename = f"play_by_play_{start_date}_{end_date}" if start_date else "play_by_play"
    save_df(df, filename, output_dir)
    return df


def backfill_player_props(client: BallDontLieClient, game_ids: List[int], output_dir: str,
                          start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Backfill player props (live betting lines)."""
    print(f"\n💰 PLAYER PROPS for {len(game_ids)} games")
    all_props = []
    
    for i, game_id in enumerate(game_ids):
        props = client.get_player_props(game_id)
        all_props.extend(props)
        
        if (i + 1) % 10 == 0:
            print(f"    Processed {i+1}/{len(game_ids)} games ({len(all_props):,} props)")
    
    if not all_props:
        print("  No player props found (props removed after games end)")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_player_prop(p) for p in all_props])
    filename = f"player_props_{start_date}_{end_date}" if start_date else "player_props"
    save_df(df, filename, output_dir)
    return df


def backfill_season_averages(client: BallDontLieClient, season: int, output_dir: str) -> pd.DataFrame:
    """Backfill expanded season averages (15+ categories)."""
    print(f"\n📊 SEASON AVERAGES for {season}")
    
    # Category/type combinations
    categories = [
        ("general", "base"),
        ("general", "advanced"),
        ("general", "scoring"),
        ("general", "defense"),
        ("general", "usage"),
        ("clutch", "base"),
        ("clutch", "advanced"),
        ("shooting", "5ft_range"),
        ("shooting", "by_zone"),
        ("playtype", "isolation"),
        ("playtype", "prballhandler"),
        ("playtype", "spotup"),
        ("playtype", "transition"),
        ("tracking", "drives"),
        ("tracking", "passing"),
        ("tracking", "rebounding"),
        ("tracking", "defense"),
        ("hustle", None),
    ]
    
    all_averages = []
    for category, stat_type in categories:
        try:
            print(f"    Fetching {category}/{stat_type or 'default'}...")
            avgs = client.get_season_averages(season, category, stat_type)
            for avg in avgs:
                flat = flatten_season_average(avg, category, stat_type or "default")
                all_averages.append(flat)
        except Exception as e:
            print(f"    ⚠️  Failed {category}/{stat_type}: {e}")
    
    if not all_averages:
        print("  No season averages found")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_averages)
    save_df(df, f"season_averages_{season}", output_dir)
    return df


def backfill_team_season_averages(client: BallDontLieClient, season: int, output_dir: str) -> pd.DataFrame:
    """Backfill team season averages."""
    print(f"\n🏀 TEAM SEASON AVERAGES for {season}")
    
    categories = [
        ("general", "base"),
        ("general", "advanced"),
        ("general", "scoring"),
        ("general", "opponent"),
        ("general", "defense"),
        ("shooting", "5ft_range_base"),
        ("shooting", "by_zone_base"),
        ("playtype", "isolation"),
        ("playtype", "transition"),
        ("tracking", "defense"),
        ("tracking", "rebounding"),
        ("hustle", None),
    ]
    
    all_averages = []
    for category, stat_type in categories:
        try:
            print(f"    Fetching {category}/{stat_type or 'default'}...")
            avgs = client.get_team_season_averages(season, category, stat_type)
            for avg in avgs:
                flat = flatten_team_season_average(avg, category, stat_type or "default")
                all_averages.append(flat)
        except Exception as e:
            print(f"    ⚠️  Failed {category}/{stat_type}: {e}")
    
    if not all_averages:
        print("  No team averages found")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_averages)
    save_df(df, f"team_season_averages_{season}", output_dir)
    return df


def backfill_standings(client: BallDontLieClient, season: int, output_dir: str) -> pd.DataFrame:
    """Backfill standings."""
    print(f"\n🏆 STANDINGS for {season}")
    standings = client.get_standings(season)
    
    if not standings:
        print("  No standings found")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_standing(s) for s in standings])
    save_df(df, f"standings_{season}", output_dir)
    return df


def backfill_injuries(client: BallDontLieClient, output_dir: str) -> pd.DataFrame:
    """Backfill current injuries."""
    print(f"\n🏥 INJURIES")
    injuries = client.get_injuries()
    
    if not injuries:
        print("  No injuries found")
        return pd.DataFrame()
    
    df = pd.DataFrame([flatten_injury(i) for i in injuries])
    today = datetime.now().strftime("%Y-%m-%d")
    save_df(df, f"injuries_{today}", output_dir)
    return df


# ===========================
# MAIN BACKFILL MODES
# ===========================

def run_full_backfill(client: BallDontLieClient, start_date: str, end_date: str,
                      season: int, output_dir: str):
    """Full backfill - ALL endpoints."""
    print("=" * 70)
    print(f"🚀 FULL BACKFILL V2: {start_date} to {end_date} (Season {season})")
    print("=" * 70)
    
    start_time = time.time()
    
    # Core data
    games_df = backfill_games(client, start_date, end_date, season, output_dir)
    game_ids = games_df["game_id"].tolist() if not games_df.empty else []
    
    if game_ids:
        backfill_stats(client, output_dir, game_ids, start_date, end_date)
        backfill_advanced_stats_v2(client, output_dir, game_ids, start_date, end_date)
        backfill_lineups(client, game_ids, output_dir, start_date, end_date)
        backfill_play_by_play(client, game_ids, output_dir, start_date, end_date)
        backfill_player_props(client, game_ids, output_dir, start_date, end_date)
    
    # Season-level data
    backfill_standings(client, season, output_dir)
    backfill_injuries(client, output_dir)
    backfill_season_averages(client, season, output_dir)
    backfill_team_season_averages(client, season, output_dir)
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"✅ FULL BACKFILL COMPLETE")
    print(f"   Time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"   Requests: {client.request_count:,}")
    print(f"   Output: {output_dir}/")
    print("=" * 70)


def run_daily_backfill(client: BallDontLieClient, start_date: str, end_date: str,
                       season: int, output_dir: str):
    """Daily backfill - games, stats, standings, injuries."""
    print("=" * 70)
    print(f"📅 DAILY BACKFILL: {start_date} to {end_date}")
    print("=" * 70)
    
    start_time = time.time()
    
    games_df = backfill_games(client, start_date, end_date, season, output_dir)
    game_ids = games_df["game_id"].tolist() if not games_df.empty else []
    
    if game_ids:
        backfill_stats(client, output_dir, game_ids, start_date, end_date)
        backfill_advanced_stats_v2(client, output_dir, game_ids, start_date, end_date)
    
    backfill_standings(client, season, output_dir)
    backfill_injuries(client, output_dir)
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"✅ DAILY BACKFILL COMPLETE")
    print(f"   Time: {elapsed:.1f}s")
    print(f"   Requests: {client.request_count:,}")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="NBA BallDontLie V2 Backfill - All endpoints including props, lineups, pbp",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full backfill with all V2 endpoints
  python nba_balldontlie_backfill_v2.py --start 2025-10-22 --end 2025-01-26 --season 2025 --full

  # Daily update
  python nba_balldontlie_backfill_v2.py --start 2025-01-26 --end 2025-01-26 --season 2025 --daily

  # Just V2 advanced stats
  python nba_balldontlie_backfill_v2.py --start 2025-01-26 --end 2025-01-26 --season 2025 --advanced-v2

  # Season averages only
  python nba_balldontlie_backfill_v2.py --season 2025 --season-averages --team-season-averages
        """
    )
    
    # Date/season
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--season", type=int, default=2025, help="Season (default: 2025)")
    
    # Modes
    parser.add_argument("--full", action="store_true", help="Full backfill (all endpoints)")
    parser.add_argument("--daily", action="store_true", help="Daily backfill")
    
    # Individual endpoints
    parser.add_argument("--games", action="store_true", help="Backfill games")
    parser.add_argument("--stats", action="store_true", help="Backfill player stats")
    parser.add_argument("--advanced-v2", action="store_true", help="Backfill V2 advanced stats")
    parser.add_argument("--lineups", action="store_true", help="Backfill lineups")
    parser.add_argument("--pbp", action="store_true", help="Backfill play-by-play")
    parser.add_argument("--player-props", action="store_true", help="Backfill player props")
    parser.add_argument("--standings", action="store_true", help="Backfill standings")
    parser.add_argument("--injuries", action="store_true", help="Backfill injuries")
    parser.add_argument("--season-averages", action="store_true", help="Backfill season averages")
    parser.add_argument("--team-season-averages", action="store_true", help="Backfill team season averages")
    
    # Output
    parser.add_argument("--output", type=str, default="data", help="Output directory")
    
    args = parser.parse_args()
    
    if not API_KEY:
        print("❌ BALLDONTLIE_API_KEY not found in .env")
        return
    
    client = BallDontLieClient(API_KEY)
    output_dir = args.output
    os.makedirs(output_dir, exist_ok=True)
    
    today = datetime.now().strftime("%Y-%m-%d")
    start_date = args.start or today
    end_date = args.end or today
    
    # Run modes
    if args.full:
        run_full_backfill(client, start_date, end_date, args.season, output_dir)
    elif args.daily:
        run_daily_backfill(client, start_date, end_date, args.season, output_dir)
    else:
        # Individual endpoints
        game_ids = []
        
        if args.games:
            games_df = backfill_games(client, start_date, end_date, args.season, output_dir)
            if not games_df.empty:
                game_ids = games_df["game_id"].tolist()
        
        # Need game_ids for these
        if args.stats:
            backfill_stats(client, output_dir, game_ids or None, start_date, end_date, args.season)
        
        if args.advanced_v2:
            backfill_advanced_stats_v2(client, output_dir, game_ids or None, start_date, end_date, args.season)
        
        if args.lineups and game_ids:
            backfill_lineups(client, game_ids, output_dir, start_date, end_date)
        
        if args.pbp and game_ids:
            backfill_play_by_play(client, game_ids, output_dir, start_date, end_date)
        
        if args.player_props and game_ids:
            backfill_player_props(client, game_ids, output_dir, start_date, end_date)
        
        # Season-level
        if args.standings:
            backfill_standings(client, args.season, output_dir)
        
        if args.injuries:
            backfill_injuries(client, output_dir)
        
        if args.season_averages:
            backfill_season_averages(client, args.season, output_dir)
        
        if args.team_season_averages:
            backfill_team_season_averages(client, args.season, output_dir)
        
        # No flags = help
        if not any([args.games, args.stats, args.advanced_v2, args.lineups, args.pbp,
                    args.player_props, args.standings, args.injuries,
                    args.season_averages, args.team_season_averages]):
            parser.print_help()


if __name__ == "__main__":
    main()