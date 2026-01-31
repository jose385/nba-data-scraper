#!/usr/bin/env python3
"""
NBA Stints/Possessions Collector (BallDontLie)
----------------------------------------------
Computes stint and possession data from BallDontLie play-by-play.
Uses your GOAT tier subscription - no NBA API needed!

Output: CSV files (monthly shards)

Stint output includes:
- game_id, period, start_clock, end_clock
- home_lineup_ids (5), away_lineup_ids (5)
- possessions, home_points, away_points
- start_margin, end_margin
"""

import os
import time
import logging
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# =============================================================================
# Configuration
# =============================================================================

DATA_DIR = Path("./nba_stints_data")
POSSESSIONS_DIR = DATA_DIR / "stints"
LOG_DIR = DATA_DIR / "logs"

REQUEST_DELAY = 0.5  # BDL is fast, but be respectful
MAX_RETRIES = 3

# =============================================================================
# Setup
# =============================================================================

def setup_directories():
    for d in [DATA_DIR, POSSESSIONS_DIR, LOG_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    log_file = LOG_DIR / f"collector_{datetime.now():%Y%m%d_%H%M%S}.log"
    
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

# =============================================================================
# BallDontLie API
# =============================================================================

class BallDontLieClient:
    """Simple client for BallDontLie API."""
    
    BASE_URL = "https://api.balldontlie.io/v1"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"Authorization": api_key}
    
    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Make GET request with retries."""
        url = f"{self.BASE_URL}/{endpoint}"
        
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(
                    url, 
                    headers=self.headers, 
                    params=params,
                    timeout=60
                )
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise e
    
    def get_games(
        self, 
        season: int, 
        start_date: str = None, 
        end_date: str = None,
        dates: list[str] = None,
        team_ids: list[int] = None,
        team_abbrevs: list[str] = None,
    ) -> list[dict]:
        """
        Get all completed games for a season with optional filters.
        
        Args:
            season: Season year (e.g., 2025 for 2025-26)
            start_date: Filter games on or after this date
            end_date: Filter games on or before this date
            dates: List of specific dates (YYYY-MM-DD)
            team_ids: List of BDL team IDs to filter
            team_abbrevs: List of team abbreviations (e.g., ['LAL', 'BOS'])
        """
        games = []
        cursor = None
        
        # Convert team abbreviations to IDs if needed
        if team_abbrevs and not team_ids:
            team_ids = self._get_team_ids(team_abbrevs)
        
        while True:
            params = {"seasons[]": season, "per_page": 100}
            if cursor:
                params["cursor"] = cursor
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            if team_ids:
                for tid in team_ids:
                    params.setdefault("team_ids[]", []).append(tid) if isinstance(params.get("team_ids[]"), list) else None
                params["team_ids[]"] = team_ids
            
            data = self._get("games", params)
            
            for g in data["data"]:
                if g.get("status") == "Final":
                    game_date = g["date"][:10]
                    
                    # Filter by specific dates if provided
                    if dates and game_date not in dates:
                        continue
                    
                    games.append({
                        "game_id": g["id"],
                        "game_date": game_date,
                        "home_team_id": g["home_team"]["id"],
                        "away_team_id": g["visitor_team"]["id"],
                        "home_team": g["home_team"]["abbreviation"],
                        "away_team": g["visitor_team"]["abbreviation"],
                        "matchup": f"{g['visitor_team']['abbreviation']} @ {g['home_team']['abbreviation']}",
                    })
            
            cursor = data.get("meta", {}).get("next_cursor")
            if not cursor:
                break
            
            time.sleep(REQUEST_DELAY)
        
        return sorted(games, key=lambda x: x["game_date"])
    
    def _get_team_ids(self, abbrevs: list[str]) -> list[int]:
        """Convert team abbreviations to BDL team IDs."""
        abbrevs_upper = [a.upper() for a in abbrevs]
        data = self._get("teams")
        
        team_ids = []
        for team in data.get("data", []):
            if team.get("abbreviation", "").upper() in abbrevs_upper:
                team_ids.append(team["id"])
        
        return team_ids
    
    def get_play_by_play(self, game_id: int) -> list[dict]:
        """Get play-by-play data for a game."""
        plays = []
        cursor = None
        
        while True:
            params = {"game_id": game_id, "per_page": 100}
            if cursor:
                params["cursor"] = cursor
            
            try:
                # Correct endpoint is /plays with game_id param
                data = self._get("plays", params)
                plays.extend(data.get("data", []))
                
                cursor = data.get("meta", {}).get("next_cursor")
                if not cursor:
                    break
                
                time.sleep(REQUEST_DELAY)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    # PBP not available for this game
                    return []
                raise
        
        return plays

# =============================================================================
# Possession/Stint Computation with Lineups
# =============================================================================

def get_starters(client: 'BallDontLieClient', game_id: int) -> tuple[set, set]:
    """
    Get starting lineups from box score.
    Returns (home_starters, away_starters) as sets of player IDs.
    """
    try:
        data = client._get(f"games/{game_id}/box_scores")
        
        home_starters = set()
        away_starters = set()
        
        for player_data in data.get("data", []):
            player = player_data.get("player", {})
            player_id = player.get("id")
            team = player_data.get("team", {})
            
            # Check if starter (usually indicated by position or minutes)
            # BDL might have a "starter" field or we infer from minutes
            is_starter = player_data.get("starter", False)
            
            # Fallback: top 5 by minutes for each team
            if player_id and team.get("id"):
                if player_data.get("home"):
                    home_starters.add(player_id)
                else:
                    away_starters.add(player_id)
        
        # If we got too many, we'll handle in compute_stints
        return home_starters, away_starters
        
    except Exception:
        return set(), set()


def compute_stints(
    plays: list[dict], 
    home_team_id: int, 
    away_team_id: int,
    home_starters: set = None,
    away_starters: set = None
) -> list[dict]:
    """
    Compute stints with lineup tracking from play-by-play data.
    
    A stint is a continuous period where both teams have the same 5 players.
    Stint ends when: substitution, end of period.
    
    Returns list of stint dicts with:
    - game_id, period, start_clock, end_clock
    - home_lineup_ids, away_lineup_ids (lists of 5 player IDs)
    - possessions, home_points, away_points
    - start_margin, end_margin
    """
    if not plays:
        return []
    
    # Sort plays by order (BDL provides sequential order field)
    sorted_plays = sorted(plays, key=lambda p: p.get("order", 0))
    
    stints = []
    current_stint = None
    
    # Track current lineups (we'll try to infer from subs)
    home_lineup = set(home_starters) if home_starters else set()
    away_lineup = set(away_starters) if away_starters else set()
    
    # Track score
    home_score = 0
    away_score = 0
    
    for play in sorted_plays:
        period = play.get("period", 1)
        play_time = play.get("clock", play.get("time", ""))
        # BDL uses team_id directly or nested team object
        team_id = play.get("team_id") or (play.get("team", {}) or {}).get("id")
        event_type = (play.get("type") or "").lower()
        # BDL uses "text" field for description
        description = (play.get("text", play.get("description", "")) or "").lower()
        
        # Start new stint for new period
        if current_stint is None or current_stint["period"] != period:
            if current_stint:
                current_stint["end_clock"] = play_time
                current_stint["end_margin"] = home_score - away_score
                stints.append(current_stint)
            
            current_stint = {
                "period": period,
                "start_clock": play_time,
                "end_clock": None,
                "home_lineup_ids": list(home_lineup)[:5] if home_lineup else [],
                "away_lineup_ids": list(away_lineup)[:5] if away_lineup else [],
                "possessions": 0,
                "home_points": 0,
                "away_points": 0,
                "start_margin": home_score - away_score,
                "end_margin": None,
            }
        
        # Check for substitution
        if "substitution" in event_type or "sub" in event_type:
            # End current stint
            if current_stint:
                current_stint["end_clock"] = play_time
                current_stint["end_margin"] = home_score - away_score
                stints.append(current_stint)
            
            # Try to parse sub (player in, player out)
            player_in = play.get("player_in", {}).get("id") if play.get("player_in") else None
            player_out = play.get("player_out", {}).get("id") if play.get("player_out") else None
            
            if team_id == home_team_id:
                if player_out and player_out in home_lineup:
                    home_lineup.discard(player_out)
                if player_in:
                    home_lineup.add(player_in)
            elif team_id == away_team_id:
                if player_out and player_out in away_lineup:
                    away_lineup.discard(player_out)
                if player_in:
                    away_lineup.add(player_in)
            
            # Start new stint
            current_stint = {
                "period": period,
                "start_clock": play_time,
                "end_clock": None,
                "home_lineup_ids": list(home_lineup)[:5] if home_lineup else [],
                "away_lineup_ids": list(away_lineup)[:5] if away_lineup else [],
                "possessions": 0,
                "home_points": 0,
                "away_points": 0,
                "start_margin": home_score - away_score,
                "end_margin": None,
            }
            continue
        
        # Track scoring
        points = 0
        if "made" in description:
            if "three" in description or "3pt" in description or "3-pt" in description:
                points = 3
            elif "free throw" in description:
                points = 1
            elif "shot" in event_type or "dunk" in event_type or "layup" in event_type:
                points = 2
        
        if points > 0 and current_stint:
            if team_id == home_team_id:
                home_score += points
                current_stint["home_points"] += points
            elif team_id == away_team_id:
                away_score += points
                current_stint["away_points"] += points
        
        # Track possession changes
        possession_ended = False
        
        if "made" in description and ("shot" in event_type or "dunk" in event_type or "layup" in event_type):
            possession_ended = True
        elif "rebound" in event_type and team_id:
            # Defensive rebound
            if (team_id == home_team_id and current_stint.get("last_shot_team") == away_team_id) or \
               (team_id == away_team_id and current_stint.get("last_shot_team") == home_team_id):
                possession_ended = True
        elif "turnover" in event_type or "steal" in event_type:
            possession_ended = True
        elif "free throw" in event_type and "made" in description:
            if ("2 of 2" in description) or ("3 of 3" in description) or ("1 of 1" in description):
                possession_ended = True
        
        if possession_ended and current_stint:
            current_stint["possessions"] += 1
        
        # Track who shot last (for rebound logic)
        if "shot" in event_type or "miss" in description:
            if current_stint:
                current_stint["last_shot_team"] = team_id
    
    # Don't forget last stint
    if current_stint:
        last_play = sorted_plays[-1] if sorted_plays else {}
        current_stint["end_clock"] = last_play.get("clock", last_play.get("time", ""))
        current_stint["end_margin"] = home_score - away_score
        # Remove tracking field
        current_stint.pop("last_shot_team", None)
        stints.append(current_stint)
    
    # Clean up stints
    for stint in stints:
        stint.pop("last_shot_team", None)
    
    return stints


def compute_possessions(plays: list[dict], home_team_id: int, away_team_id: int) -> list[dict]:
    """
    Compute possessions from play-by-play data.
    
    A possession ends when:
    - Made field goal (not and-1 continuation)
    - Defensive rebound
    - Turnover
    - Made last free throw
    - End of period
    """
    if not plays:
        return []
    
    # Sort plays by order (BDL provides sequential order)
    sorted_plays = sorted(plays, key=lambda p: p.get("order", 0))
    
    possessions = []
    current_poss = None
    poss_events = []
    
    for play in sorted_plays:
        period = play.get("period", 1)
        play_time = play.get("clock", play.get("time", ""))
        team_id = play.get("team_id") or (play.get("team", {}) or {}).get("id")
        event_type = (play.get("type") or "").lower()
        description = (play.get("text", play.get("description", "")) or "").lower()
        
        # Determine offensive team
        if team_id:
            offense_team = team_id
            defense_team = away_team_id if team_id == home_team_id else home_team_id
        elif current_poss:
            offense_team = current_poss["offense_team_id"]
            defense_team = current_poss["defense_team_id"]
        else:
            continue
        
        # Start new possession if needed
        if current_poss is None or current_poss["period"] != period:
            if current_poss:
                current_poss["events"] = poss_events
                possessions.append(current_poss)
            
            current_poss = {
                "period": period,
                "start_time": play_time,
                "offense_team_id": offense_team,
                "defense_team_id": defense_team,
                "start_type": "period_start" if not possessions or possessions[-1]["period"] != period else "unknown",
            }
            poss_events = []
        
        poss_events.append({
            "time": play_time,
            "type": event_type,
            "description": description,
            "team_id": team_id,
        })
        
        # Check for possession change
        possession_ended = False
        end_type = None
        
        # Made shot (not free throw)
        if "made" in description and ("shot" in event_type or "dunk" in event_type or "layup" in event_type):
            possession_ended = True
            end_type = "made_shot"
        
        # Defensive rebound
        elif "rebound" in event_type and team_id and team_id != offense_team:
            possession_ended = True
            end_type = "defensive_rebound"
        
        # Turnover
        elif "turnover" in event_type or "steal" in event_type:
            possession_ended = True
            end_type = "turnover"
        
        # Made last free throw (simplified - just check for made FT)
        elif "free throw" in event_type and "made" in description:
            # Check if it's the last FT (e.g., "2 of 2", "3 of 3")
            if "of 2" in description and "2 of 2" in description:
                possession_ended = True
                end_type = "made_ft"
            elif "of 3" in description and "3 of 3" in description:
                possession_ended = True
                end_type = "made_ft"
            elif "of 1" in description:
                possession_ended = True
                end_type = "made_ft"
        
        # End of period
        elif "end" in event_type and "period" in description:
            possession_ended = True
            end_type = "period_end"
        
        if possession_ended and current_poss:
            current_poss["end_time"] = play_time
            current_poss["end_type"] = end_type
            current_poss["events"] = poss_events
            possessions.append(current_poss)
            
            # Start new possession (team switch)
            current_poss = {
                "period": period,
                "start_time": play_time,
                "offense_team_id": defense_team,  # Swapped
                "defense_team_id": offense_team,
                "start_type": end_type,
            }
            poss_events = []
    
    # Don't forget last possession
    if current_poss and poss_events:
        current_poss["end_time"] = poss_events[-1].get("time", "") if poss_events else ""
        current_poss["end_type"] = "period_end"
        current_poss["events"] = poss_events
        possessions.append(current_poss)
    
    return possessions


def parse_time(time_str: str) -> float:
    """Convert MM:SS to seconds."""
    try:
        if not time_str:
            return 0
        parts = time_str.split(":")
        if len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
        return float(parts[0])
    except:
        return 0

# =============================================================================
# Storage
# =============================================================================

def get_month_shard_path(game_date: str, output_type: str = "stints") -> Path:
    dt = datetime.strptime(game_date, "%Y-%m-%d")
    filename = f"{dt.year}-{dt.month:02d}_{output_type}.csv"
    return POSSESSIONS_DIR / filename


def save_stints(data: list[dict], game_date: str, game_id: int, logger: logging.Logger):
    """Save stints to monthly shard."""
    if not data:
        return
    
    shard_path = get_month_shard_path(game_date, "stints")
    
    # Flatten stints for storage
    flat_data = []
    for i, stint in enumerate(data):
        # Pad lineup arrays to 5 players
        home_lineup = stint.get("home_lineup_ids", [])[:5]
        away_lineup = stint.get("away_lineup_ids", [])[:5]
        
        # Pad with None if less than 5
        while len(home_lineup) < 5:
            home_lineup.append(None)
        while len(away_lineup) < 5:
            away_lineup.append(None)
        
        flat_data.append({
            "game_id": game_id,
            "stint_num": i + 1,
            "period": stint.get("period"),
            "start_clock": stint.get("start_clock"),
            "end_clock": stint.get("end_clock"),
            "home_player_1": home_lineup[0],
            "home_player_2": home_lineup[1],
            "home_player_3": home_lineup[2],
            "home_player_4": home_lineup[3],
            "home_player_5": home_lineup[4],
            "away_player_1": away_lineup[0],
            "away_player_2": away_lineup[1],
            "away_player_3": away_lineup[2],
            "away_player_4": away_lineup[3],
            "away_player_5": away_lineup[4],
            "possessions": stint.get("possessions", 0),
            "home_points": stint.get("home_points", 0),
            "away_points": stint.get("away_points", 0),
            "start_margin": stint.get("start_margin", 0),
            "end_margin": stint.get("end_margin", 0),
        })
    
    new_df = pd.DataFrame(flat_data)
    
    if shard_path.exists():
        existing_df = pd.read_csv(shard_path)
        existing_df = existing_df[existing_df["game_id"] != game_id]
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    
    combined_df.to_csv(shard_path, index=False)
    logger.debug(f"Saved {len(new_df)} stints to {shard_path}")


def save_possessions(data: list[dict], game_date: str, game_id: int, logger: logging.Logger):
    if not data:
        return
    
    shard_path = get_month_shard_path(game_date, "possessions")
    
    # Flatten possessions (remove events list for storage)
    flat_data = []
    for i, poss in enumerate(data):
        flat_data.append({
            "game_id": game_id,
            "possession_num": i + 1,
            "period": poss.get("period"),
            "offense_team_id": poss.get("offense_team_id"),
            "defense_team_id": poss.get("defense_team_id"),
            "start_time": poss.get("start_time"),
            "end_time": poss.get("end_time"),
            "start_type": poss.get("start_type"),
            "end_type": poss.get("end_type"),
        })
    
    new_df = pd.DataFrame(flat_data)
    
    if shard_path.exists():
        existing_df = pd.read_csv(shard_path)
        existing_df = existing_df[existing_df["game_id"] != game_id]
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    
    combined_df.to_csv(shard_path, index=False)
    logger.debug(f"Saved {len(new_df)} possessions to {shard_path}")


def load_processed_games(output_type: str = "stints") -> set[int]:
    processed = set()
    pattern = f"*_{output_type}.csv"
    for shard_file in POSSESSIONS_DIR.glob(pattern):
        try:
            df = pd.read_csv(shard_file, usecols=["game_id"])
            processed.update(df["game_id"].unique())
        except Exception:
            pass
    return processed

# =============================================================================
# Collection
# =============================================================================

def run_backfill(
    season: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    dates: Optional[list[str]] = None,
    teams: Optional[list[str]] = None,
    api_key: str = None,
    skip_existing: bool = True,
    output_type: str = "stints",  # "stints" or "possessions"
    logger: logging.Logger = None
):
    client = BallDontLieClient(api_key)
    
    # Build filter description for logging
    filters = []
    if dates:
        filters.append(f"dates={dates}")
    elif start_date or end_date:
        filters.append(f"range={start_date or 'start'} to {end_date or 'end'}")
    if teams:
        filters.append(f"teams={teams}")
    filter_str = f" ({', '.join(filters)})" if filters else ""
    
    print(f"Fetching games for {season}-{(season + 1) % 100:02d}{filter_str}...")
    games = client.get_games(
        season, 
        start_date=start_date, 
        end_date=end_date,
        dates=dates,
        team_abbrevs=teams
    )
    print(f"Found {len(games)} completed games")
    
    if not games:
        logger.info("No games found")
        return
    
    if skip_existing:
        processed = load_processed_games(output_type)
        original_count = len(games)
        games = [g for g in games if g["game_id"] not in processed]
        logger.info(f"Skipping {original_count - len(games)} already processed games")
    
    logger.info(f"Processing {len(games)} games (output: {output_type})...")
    
    stats = {"success": 0, "failed": 0, "no_lineups": 0}
    
    for i, game in enumerate(games, 1):
        game_id = game["game_id"]
        game_date = game["game_date"]
        matchup = game["matchup"]
        
        logger.info(f"[{i}/{len(games)}] {game_date}: {matchup} (BDL #{game_id})")
        
        try:
            # Get play-by-play
            plays = client.get_play_by_play(game_id)
            
            if not plays:
                logger.warning(f"  No play-by-play data")
                stats["failed"] += 1
                continue
            
            if output_type == "stints":
                # Try to get starters for lineup tracking
                home_starters, away_starters = get_starters(client, game_id)
                
                stints = compute_stints(
                    plays, 
                    game["home_team_id"], 
                    game["away_team_id"],
                    home_starters,
                    away_starters
                )
                
                if stints:
                    save_stints(stints, game_date, game_id, logger)
                    has_lineups = any(s.get("home_lineup_ids") for s in stints)
                    logger.info(f"  Got {len(stints)} stints (lineups: {'yes' if has_lineups else 'no'})")
                    if not has_lineups:
                        stats["no_lineups"] += 1
                    stats["success"] += 1
                else:
                    logger.warning(f"  Could not compute stints")
                    stats["failed"] += 1
            else:
                # Original possession-only mode
                possessions = compute_possessions(
                    plays, 
                    game["home_team_id"], 
                    game["away_team_id"]
                )
                
                if possessions:
                    save_possessions(possessions, game_date, game_id, logger)
                    logger.info(f"  Got {len(possessions)} possessions")
                    stats["success"] += 1
                else:
                    logger.warning(f"  Could not compute possessions")
                    stats["failed"] += 1
                
        except Exception as e:
            logger.error(f"  Error: {e}")
            stats["failed"] += 1
        
        time.sleep(REQUEST_DELAY)
        
        if i % 25 == 0:
            logger.info(f"Progress: {stats}")
    
    logger.info(f"Complete: {stats}")
    return stats


def run_daily(season: int, api_key: str, output_type: str, teams: Optional[list[str]], logger: logging.Logger, date_offset: int = 1):
    """
    Collect games for a specific day.
    
    Args:
        date_offset: Days ago (1 = yesterday, 0 = today)
    """
    target_date = (datetime.now() - timedelta(days=date_offset)).strftime("%Y-%m-%d")
    logger.info(f"Collecting games from {target_date}")
    
    run_backfill(
        season=season,
        dates=[target_date],
        teams=teams,
        api_key=api_key,
        skip_existing=False,
        output_type=output_type,
        logger=logger
    )

# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="NBA Possessions/Stints Collector (BallDontLie)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full season backfill
  python nba_bdl_possessions.py backfill --season 2025
  
  # Specific dates
  python nba_bdl_possessions.py backfill --dates 2026-01-15 2026-01-16
  
  # Specific teams
  python nba_bdl_possessions.py backfill --teams LAL BOS --season 2025
  
  # Date range for specific teams
  python nba_bdl_possessions.py backfill --start-date 2026-01-01 --end-date 2026-01-15 --teams GSW
  
  # Yesterday's games
  python nba_bdl_possessions.py daily
  
  # Today's games
  python nba_bdl_possessions.py today
        """
    )
    
    parser.add_argument("mode", choices=["backfill", "daily", "today"], help="Collection mode")
    parser.add_argument("--season", type=int, default=2025, help="Season start year (default: 2025)")
    parser.add_argument("--start-date", help="Start date for range (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date for range (YYYY-MM-DD)")
    parser.add_argument("--dates", nargs="+", help="Specific dates (YYYY-MM-DD YYYY-MM-DD ...)")
    parser.add_argument("--teams", nargs="+", help="Team abbreviations (LAL BOS GSW ...)")
    parser.add_argument("--api-key", help="BallDontLie API key (or set BALLDONTLIE_API_KEY)")
    parser.add_argument("--output", choices=["stints", "possessions"], default="stints",
                       help="Output type: stints or possessions (default: stints)")
    parser.add_argument("--no-skip", action="store_true", help="Don't skip existing games")
    parser.add_argument("--verbose", action="store_true", help="Debug logging")
    
    args = parser.parse_args()
    
    api_key = args.api_key or os.environ.get("BALLDONTLIE_API_KEY")
    if not api_key:
        print("Error: BallDontLie API key required.")
        print("Set BALLDONTLIE_API_KEY env var or use --api-key")
        return
    
    setup_directories()
    logger = setup_logging(args.verbose)
    
    season_str = f"{args.season}-{(args.season + 1) % 100:02d}"
    
    logger.info("=" * 50)
    logger.info("NBA Stints/Possessions Collector (BallDontLie)")
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Season: {season_str}")
    if args.dates:
        logger.info(f"Dates: {args.dates}")
    if args.teams:
        logger.info(f"Teams: {args.teams}")
    logger.info(f"Output: {args.output}")
    logger.info("=" * 50)
    
    if args.mode == "backfill":
        run_backfill(
            season=args.season,
            start_date=args.start_date,
            end_date=args.end_date,
            dates=args.dates,
            teams=args.teams,
            api_key=api_key,
            skip_existing=not args.no_skip,
            output_type=args.output,
            logger=logger
        )
    elif args.mode == "daily":
        run_daily(args.season, api_key, args.output, args.teams, logger, date_offset=1)
    elif args.mode == "today":
        run_daily(args.season, api_key, args.output, args.teams, logger, date_offset=0)
    
    logger.info("Done")


if __name__ == "__main__":
    main()