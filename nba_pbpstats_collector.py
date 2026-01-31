#!/usr/bin/env python3
"""
NBA Possessions Collector
-------------------------
Engine 10: Stints/Possessions via pbpstats

Uses NBA CDN for game schedule, pbpstats for possession data.
No API keys required!
"""

import time
import logging
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from pbpstats.client import Client as PBPClient

# =============================================================================
# Configuration
# =============================================================================

DATA_DIR = Path("./nba_possessions_data")
POSSESSIONS_DIR = DATA_DIR / "possessions"
CACHE_DIR = DATA_DIR / "pbpstats_cache"
LOG_DIR = DATA_DIR / "logs"

REQUEST_DELAY = 1.0  # seconds between games

# =============================================================================
# Setup
# =============================================================================

def setup_directories():
    """Create necessary directories."""
    for d in [DATA_DIR, POSSESSIONS_DIR, CACHE_DIR, LOG_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def setup_logging(verbose: bool = False):
    """Configure logging."""
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
# Game List - Use NBA CDN Schedule (most reliable)
# =============================================================================

def get_nba_schedule(season_year: int) -> list[dict]:
    """
    Fetch full NBA schedule from CDN. Has all game IDs.
    
    Args:
        season_year: Start year of season (e.g., 2025 for 2025-26)
    """
    url = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.nba.com/",
    }
    
    print("Fetching NBA schedule from CDN...")
    
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    
    games = []
    target_season = f"{season_year}-{(season_year + 1) % 100:02d}"
    
    for game_date_obj in data.get("leagueSchedule", {}).get("gameDates", []):
        for game in game_date_obj.get("games", []):
            # Only include regular season games for now
            if game.get("seriesText", "") != "":
                continue  # Skip playoff games
                
            game_id = game.get("gameId", "")
            game_date = game.get("gameDateEst", "")[:10]  # YYYY-MM-DD
            
            # Check if it's the right season based on game ID
            # Format: 00XXYYYYYY where XX indicates season
            if not game_id.startswith(f"002{season_year % 100}"):
                continue
            
            home = game.get("homeTeam", {})
            away = game.get("awayTeam", {})
            
            status = game.get("gameStatus", 0)
            # gameStatus: 1 = scheduled, 2 = in progress, 3 = final
            if status != 3:
                continue  # Only completed games
            
            games.append({
                "game_id": game_id,
                "game_date": game_date,
                "home_team": home.get("teamTricode", ""),
                "away_team": away.get("teamTricode", ""),
                "matchup": f"{away.get('teamTricode', '')} @ {home.get('teamTricode', '')}",
                "season": target_season,
            })
    
    print(f"Found {len(games)} completed games")
    return sorted(games, key=lambda x: x["game_date"])

# =============================================================================
# Possessions via pbpstats
# =============================================================================

def get_game_possessions(game_id: str, logger: logging.Logger) -> Optional[list[dict]]:
    """
    Fetch possession-level data using pbpstats.
    Uses data.nba.com (CDN) instead of stats.nba.com for reliability.
    """
    try:
        settings = {
            "dir": str(CACHE_DIR),
            "Possessions": {"source": "web", "data_provider": "data_nba"}  # CDN, not stats API
        }
        client = PBPClient(settings)
        game = client.Game(game_id)
        
        possessions = []
        for period in game.Periods.items:
            for poss in period.Possessions.items:
                possessions.append({
                    "game_id": game_id,
                    "period": period.Number,
                    "offense_team_id": poss.OffenseTeamId,
                    "defense_team_id": poss.DefenseTeamId,
                    "start_time": poss.StartTime,
                    "end_time": poss.EndTime,
                    "start_type": poss.StartType,
                    "end_type": poss.EndType,
                    "offense_lineup_id": poss.OffenseLineupId,
                    "defense_lineup_id": poss.DefenseLineupId,
                })
        
        logger.info(f"  Got {len(possessions)} possessions")
        return possessions
        
    except Exception as e:
        logger.error(f"  pbpstats error: {e}")
        return None

# =============================================================================
# Storage
# =============================================================================

def get_month_shard_path(game_date: str) -> Path:
    """Get the path for a monthly shard file."""
    dt = datetime.strptime(game_date, "%Y-%m-%d")
    filename = f"{dt.year}-{dt.month:02d}_possessions.parquet"
    return POSSESSIONS_DIR / filename


def save_possessions(data: list[dict], game_date: str, game_id: str, logger: logging.Logger):
    """Save possessions to monthly shard."""
    if not data:
        return
    
    shard_path = get_month_shard_path(game_date)
    new_df = pd.DataFrame(data)
    
    if shard_path.exists():
        existing_df = pd.read_parquet(shard_path)
        # Remove existing data for this game
        existing_df = existing_df[existing_df["game_id"] != game_id]
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    
    combined_df.to_parquet(shard_path, index=False)
    logger.debug(f"Saved {len(new_df)} rows to {shard_path}")


def load_processed_games() -> set[str]:
    """Get set of game IDs already processed."""
    processed = set()
    for shard_file in POSSESSIONS_DIR.glob("*.parquet"):
        try:
            df = pd.read_parquet(shard_file, columns=["game_id"])
            processed.update(df["game_id"].astype(str).unique())
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
    skip_existing: bool = True,
    logger: logging.Logger = None
):
    """Backfill possession data for a season."""
    
    # Get games from NBA CDN schedule
    games = get_nba_schedule(season)
    
    # Apply date filters
    if start_date:
        games = [g for g in games if g["game_date"] >= start_date]
    if end_date:
        games = [g for g in games if g["game_date"] <= end_date]
    
    if not games:
        logger.info("No games found")
        return
    
    # Skip already processed
    if skip_existing:
        processed = load_processed_games()
        original_count = len(games)
        games = [g for g in games if g["game_id"] not in processed]
        logger.info(f"Skipping {original_count - len(games)} already processed games")
    
    logger.info(f"Processing {len(games)} games...")
    
    stats = {"success": 0, "failed": 0}
    
    for i, game in enumerate(games, 1):
        game_id = game["game_id"]
        game_date = game["game_date"]
        matchup = game["matchup"]
        
        logger.info(f"[{i}/{len(games)}] {game_date}: {matchup} ({game_id})")
        
        possessions = get_game_possessions(game_id, logger)
        
        if possessions:
            save_possessions(possessions, game_date, game_id, logger)
            stats["success"] += 1
        else:
            stats["failed"] += 1
        
        time.sleep(REQUEST_DELAY)
        
        # Progress update
        if i % 25 == 0:
            logger.info(f"Progress: {stats}")
    
    logger.info(f"Complete: {stats}")
    return stats


def run_daily(season: int, logger: logging.Logger):
    """Collect yesterday's games."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info(f"Collecting games from {yesterday}")
    
    run_backfill(
        season=season,
        start_date=yesterday,
        end_date=yesterday,
        skip_existing=False,
        logger=logger
    )


def run_single_game(game_id: str, game_date: str, logger: logging.Logger):
    """Collect a single game by NBA game ID."""
    
    possessions = get_game_possessions(game_id, logger)
    
    if possessions:
        save_possessions(possessions, game_date, game_id, logger)
        logger.info(f"Saved {len(possessions)} possessions")
    else:
        logger.error("Failed to get possessions")

# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="NBA Possessions Collector (pbpstats)")
    
    parser.add_argument(
        "mode",
        choices=["backfill", "daily", "game"],
        help="Collection mode"
    )
    parser.add_argument(
        "--season",
        type=int,
        default=2025,
        help="Season start year (default: 2025 for 2025-26)"
    )
    parser.add_argument(
        "--start-date",
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--game-id",
        help="NBA game ID for single game mode (e.g., 0022500123)"
    )
    parser.add_argument(
        "--game-date",
        help="Game date for single game mode (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Don't skip existing games"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Debug logging"
    )
    
    args = parser.parse_args()
    
    setup_directories()
    logger = setup_logging(args.verbose)
    
    season_str = f"{args.season}-{(args.season + 1) % 100:02d}"
    
    logger.info("=" * 50)
    logger.info("NBA Possessions Collector (pbpstats)")
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Season: {season_str}")
    logger.info("=" * 50)
    
    if args.mode == "backfill":
        run_backfill(
            season=args.season,
            start_date=args.start_date,
            end_date=args.end_date,
            skip_existing=not args.no_skip,
            logger=logger
        )
    
    elif args.mode == "daily":
        run_daily(args.season, logger)
    
    elif args.mode == "game":
        if not args.game_id:
            logger.error("--game-id required for game mode")
            return
        game_date = args.game_date or datetime.now().strftime("%Y-%m-%d")
        run_single_game(args.game_id, game_date, logger)
    
    logger.info("Done")


if __name__ == "__main__":
    main()