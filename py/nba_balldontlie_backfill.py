#!/usr/bin/env python3
"""
NBA BallDontLie Data Backfill - GOAT Tier
Comprehensive data collection for betting analytics
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from nba_balldontlie_client import (
    BallDontLieClient, 
    flatten_game, 
    flatten_player_stats,
    flatten_advanced_stats
)


class NBADataBackfill:
    def __init__(self, api_key: Optional[str] = None, output_dir: str = "stage"):
        self.client = BallDontLieClient(api_key)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def save_parquet(self, data: List[dict], name: str, subdir: str = ""):
        if not data:
            print(f"   ⚠️ No data to save for {name}")
            return None
        df = pd.DataFrame(data)
        save_dir = self.output_dir / subdir if subdir else self.output_dir
        save_dir.mkdir(parents=True, exist_ok=True)
        filepath = save_dir / f"{name}.parquet"
        df.to_parquet(filepath, index=False)
        print(f"   💾 Saved {len(df)} rows to {filepath}")
        return filepath
    
    def generate_date_range(self, start: str, end: str) -> List[str]:
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = datetime.strptime(end, "%Y-%m-%d")
        dates = []
        current = start_dt
        while current <= end_dt:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return dates
    
    # === CORE DATA COLLECTION ===
    
    def backfill_teams(self):
        print("\n🏀 Collecting teams...")
        teams = self.client.get_teams()
        self.save_parquet(teams, "teams", "reference")
        return teams
    
    def backfill_players(self):
        print("\n👤 Collecting players...")
        players = self.client.get_players()
        self.save_parquet(players, "players", "reference")
        return players
    
    def backfill_games(self, start_date: str, end_date: str):
        print(f"\n📅 Collecting games: {start_date} to {end_date}...")
        games = self.client.get_games(start_date, end_date)
        if games:
            flat_games = [flatten_game(g) for g in games]
            self.save_parquet(flat_games, f"games_{start_date}_{end_date}", "games")
        return games
    
    def backfill_player_stats(self, start_date: str, end_date: str):
        print(f"\n📊 Collecting player stats: {start_date} to {end_date}...")
        stats = self.client.get_stats(start_date=start_date, end_date=end_date)
        if stats:
            flat_stats = [flatten_player_stats(s) for s in stats]
            self.save_parquet(flat_stats, f"player_stats_{start_date}_{end_date}", "stats")
        return stats
    
    def backfill_box_scores_daily(self, start_date: str, end_date: str):
        print(f"\n📦 Collecting daily box scores: {start_date} to {end_date}...")
        dates = self.generate_date_range(start_date, end_date)
        all_box_scores = []
        for i, date in enumerate(dates, 1):
            print(f"   [{i}/{len(dates)}] {date}...")
            box_scores = self.client.get_box_scores(date)
            if box_scores:
                all_box_scores.extend(box_scores)
        if all_box_scores:
            self.save_parquet(all_box_scores, f"box_scores_{start_date}_{end_date}", "box_scores")
        return all_box_scores
    
    # === GOAT TIER DATA COLLECTION ===
    
    def backfill_advanced_stats(self, start_date: str, end_date: str):
        print(f"\n📈 Collecting advanced stats: {start_date} to {end_date}...")
        stats = self.client.get_advanced_stats(start_date=start_date, end_date=end_date)
        if stats:
            flat_stats = [flatten_advanced_stats(s) for s in stats]
            self.save_parquet(flat_stats, f"advanced_stats_{start_date}_{end_date}", "advanced")
        return stats
    
    def backfill_season_averages(self, season: int, player_ids: Optional[List[int]] = None):
        print(f"\n📊 Collecting season averages for {season}...")
        averages = self.client.get_season_averages(season, player_ids)
        if averages:
            self.save_parquet(averages, f"season_averages_{season}", "season_averages")
        return averages
    
    def backfill_standings(self, season: int):
        print(f"\n🏆 Collecting standings for {season}...")
        standings = self.client.get_standings(season)
        if standings:
            self.save_parquet(standings, f"standings_{season}", "standings")
        return standings
    
    def backfill_injuries(self):
        print("\n🏥 Collecting current injuries...")
        injuries = self.client.get_injuries()
        today = datetime.now().strftime("%Y-%m-%d")
        if injuries:
            self.save_parquet(injuries, f"injuries_{today}", "injuries")
        return injuries
    
    def backfill_leaders(self, season: int, stat_types: Optional[List[str]] = None):
        print(f"\n🌟 Collecting league leaders for {season}...")
        if stat_types is None:
            stat_types = ["pts", "reb", "ast", "stl", "blk", "fg_pct", "fg3_pct", "ft_pct"]
        all_leaders = {}
        for stat in stat_types:
            print(f"   Fetching {stat} leaders...")
            leaders = self.client.get_leaders(season, stat)
            if leaders:
                all_leaders[stat] = leaders
        combined = []
        for stat_type, leaders in all_leaders.items():
            for leader in leaders:
                leader["stat_type"] = stat_type
                combined.append(leader)
        if combined:
            self.save_parquet(combined, f"leaders_{season}", "leaders")
        return all_leaders
    
    def backfill_odds(self, game_ids: List[int]):
        print(f"\n💰 Collecting odds for {len(game_ids)} games...")
        all_odds = []
        for i, game_id in enumerate(game_ids, 1):
            print(f"   [{i}/{len(game_ids)}] Game {game_id}...")
            odds = self.client.get_odds(game_id)
            if odds:
                for odd in odds:
                    odd["game_id"] = game_id
                all_odds.extend(odds)
        if all_odds:
            self.save_parquet(all_odds, f"odds_batch", "odds")
        return all_odds
    
    def backfill_play_by_play(self, game_ids: List[int]):
        print(f"\n🎬 Collecting play-by-play for {len(game_ids)} games...")
        all_pbp = []
        for i, game_id in enumerate(game_ids, 1):
            print(f"   [{i}/{len(game_ids)}] Game {game_id}...")
            pbp = self.client.get_play_by_play(game_id)
            if pbp:
                for play in pbp:
                    play["game_id"] = game_id
                all_pbp.extend(pbp)
        if all_pbp:
            self.save_parquet(all_pbp, f"play_by_play_batch", "play_by_play")
        return all_pbp
    
    # === COMPREHENSIVE BACKFILL ===
    
    def run_full_backfill(self, start_date: str, end_date: str, season: Optional[int] = None):
        print("=" * 60)
        print(f"🏀 NBA FULL DATA BACKFILL")
        print(f"   Date Range: {start_date} to {end_date}")
        print(f"   Output: {self.output_dir}")
        print("=" * 60)
        
        if season is None:
            start_year = int(start_date[:4])
            start_month = int(start_date[5:7])
            season = start_year if start_month >= 10 else start_year - 1
        
        self.backfill_teams()
        self.backfill_players()
        games = self.backfill_games(start_date, end_date)
        self.backfill_player_stats(start_date, end_date)
        self.backfill_box_scores_daily(start_date, end_date)
        self.backfill_advanced_stats(start_date, end_date)
        self.backfill_season_averages(season)
        self.backfill_standings(season)
        self.backfill_injuries()
        self.backfill_leaders(season)
        
        if games and start_date >= "2025-01-01":
            game_ids = [g.get("id") for g in games if g.get("id")]
            if game_ids:
                self.backfill_odds(game_ids)
                self.backfill_play_by_play(game_ids)
        
        print("\n" + "=" * 60)
        print(f"✅ BACKFILL COMPLETE")
        print(f"   Total API requests: {self.client.request_count}")
        print(f"   Data saved to: {self.output_dir}")
        print("=" * 60)
    
    def run_daily_update(self, date: Optional[str] = None):
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        print("=" * 60)
        print(f"🏀 NBA DAILY UPDATE: {date}")
        print("=" * 60)
        self.backfill_games(date, date)
        self.backfill_player_stats(date, date)
        self.backfill_box_scores_daily(date, date)
        self.backfill_advanced_stats(date, date)
        self.backfill_injuries()
        if date >= "2025-01-01":
            games = self.client.get_games(date, date)
            if games:
                game_ids = [g.get("id") for g in games if g.get("id")]
                if game_ids:
                    self.backfill_odds(game_ids)
        print(f"\n✅ Daily update complete ({self.client.request_count} requests)")


def main():
    parser = argparse.ArgumentParser(description="NBA BallDontLie Data Backfill")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--api-key", help="BallDontLie API key (or set BALLDONTLIE_API_KEY env)")
    parser.add_argument("--output", default="stage", help="Output directory (default: stage)")
    parser.add_argument("--full", action="store_true", help="Run full backfill with all data types")
    parser.add_argument("--daily", action="store_true", help="Quick daily update mode")
    parser.add_argument("--season", type=int, help="Season year (e.g., 2024 for 2024-25 season)")
    
    args = parser.parse_args()
    
    try:
        backfill = NBADataBackfill(api_key=args.api_key, output_dir=args.output)
        if args.daily:
            backfill.run_daily_update(args.start)
        elif args.full:
            backfill.run_full_backfill(args.start, args.end, args.season)
        else:
            print(f"📅 Basic backfill: {args.start} to {args.end}")
            backfill.backfill_games(args.start, args.end)
            backfill.backfill_player_stats(args.start, args.end)
    except ValueError as e:
        print(f"❌ Error: {e}")
        print("\nMake sure you have set your API key:")
        print("  export BALLDONTLIE_API_KEY=your_key_here")
        print("  OR")
        print("  echo 'BALLDONTLIE_API_KEY=your_key_here' > .env")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    main()