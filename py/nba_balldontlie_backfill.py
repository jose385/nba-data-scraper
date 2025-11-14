#!/usr/bin/env python3
"""
NBA Data Backfill - BallDontLie API Edition
Clean, simple, and reliable NBA data collection

Collects:
- Games (schedule, scores, results)
- Box scores (player stats per game)
- Team information
- Season averages

Claude handles: injuries, lineups, trends, referee assignments, etc.
"""

import argparse
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from py.nba_balldontlie_client import create_client


def collect_teams(client, out_dir: str) -> str:
    """
    Collect all NBA teams
    Run this once at the start of each season
    """
    print("\n📋 Collecting NBA Teams...")
    
    teams_data = client.get_teams()
    
    if not teams_data:
        print("❌ No teams data retrieved")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(teams_data)
    
    # Save to parquet
    out_file = os.path.join(out_dir, 'nba_teams.parquet')
    df.to_parquet(out_file, index=False)
    
    print(f"✅ Teams: {len(df)} teams → {out_file}")
    
    # Show sample
    print(f"\n📊 Sample teams:")
    print(df[['id', 'abbreviation', 'full_name', 'conference', 'division']].head())
    
    return out_file


def collect_games(client, date_str: str, out_dir: str, season: Optional[int] = None) -> str:
    """
    Collect games for a specific date
    
    Args:
        client: BallDontLie client
        date_str: Date in YYYY-MM-DD format
        out_dir: Output directory
        season: Optional season year (e.g., 2024)
    """
    print(f"\n📅 Collecting Games for {date_str}...")
    
    # Get games for the date
    params = {
        "start_date": date_str,
        "end_date": date_str
    }
    
    if season:
        params["seasons"] = [season]
    
    games_data = client.get_games(**params)
    
    if not games_data:
        print(f"   ℹ️ No games found for {date_str}")
        # Create empty file
        df = pd.DataFrame(columns=['id', 'date', 'season'])
        out_file = os.path.join(out_dir, f'nba_games_{date_str}.parquet')
        df.to_parquet(out_file, index=False)
        return out_file
    
    # Convert to DataFrame
    df = pd.DataFrame(games_data)
    
    # Flatten nested team data
    if 'home_team' in df.columns:
        df['home_team_id'] = df['home_team'].apply(lambda x: x['id'] if isinstance(x, dict) else None)
        df['home_team_name'] = df['home_team'].apply(lambda x: x['full_name'] if isinstance(x, dict) else None)
        df['home_team_abbrev'] = df['home_team'].apply(lambda x: x['abbreviation'] if isinstance(x, dict) else None)
    
    if 'visitor_team' in df.columns:
        df['away_team_id'] = df['visitor_team'].apply(lambda x: x['id'] if isinstance(x, dict) else None)
        df['away_team_name'] = df['visitor_team'].apply(lambda x: x['full_name'] if isinstance(x, dict) else None)
        df['away_team_abbrev'] = df['visitor_team'].apply(lambda x: x['abbreviation'] if isinstance(x, dict) else None)
    
    # Add game date (extract from date field)
    df['game_date'] = date_str
    
    # Select key columns
    key_columns = [
        'id', 'date', 'game_date', 'season', 'status',
        'home_team_id', 'home_team_name', 'home_team_abbrev', 'home_team_score',
        'away_team_id', 'away_team_name', 'away_team_abbrev', 'visitor_team_score',
        'period', 'time', 'postseason'
    ]
    
    # Keep only columns that exist
    available_columns = [col for col in key_columns if col in df.columns]
    df_clean = df[available_columns].copy()
    
    # Rename for consistency
    if 'visitor_team_score' in df_clean.columns:
        df_clean = df_clean.rename(columns={'visitor_team_score': 'away_team_score'})
    
    # Save to parquet
    out_file = os.path.join(out_dir, f'nba_games_{date_str}.parquet')
    df_clean.to_parquet(out_file, index=False)
    
    print(f"✅ Games: {len(df_clean)} games → {out_file}")
    
    # Show summary
    if len(df_clean) > 0:
        print(f"\n📊 Games summary:")
        for _, game in df_clean.head(5).iterrows():
            home = game.get('home_team_abbrev', 'HOME')
            away = game.get('away_team_abbrev', 'AWAY')
            home_score = game.get('home_team_score', 0)
            away_score = game.get('away_team_score', 0)
            status = game.get('status', 'Unknown')
            print(f"   {away} @ {home}: {away_score}-{home_score} ({status})")
    
    return out_file


def collect_box_scores(client, date_str: str, out_dir: str) -> str:
    """
    Collect box scores (player stats) for all games on a date
    
    Args:
        client: BallDontLie client
        date_str: Date in YYYY-MM-DD format
        out_dir: Output directory
    """
    print(f"\n📊 Collecting Box Scores for {date_str}...")
    
    # Get all stats for the date (more efficient than per-game)
    stats_data = client.get_stats_for_date(date_str)
    
    if not stats_data:
        print(f"   ℹ️ No box scores found for {date_str}")
        # Create empty file
        df = pd.DataFrame(columns=['id', 'game_id', 'player_id'])
        out_file = os.path.join(out_dir, f'nba_box_scores_{date_str}.parquet')
        df.to_parquet(out_file, index=False)
        return out_file
    
    # Convert to DataFrame
    df = pd.DataFrame(stats_data)
    
    # Flatten nested data
    if 'player' in df.columns:
        df['player_id'] = df['player'].apply(lambda x: x['id'] if isinstance(x, dict) else None)
        df['player_first_name'] = df['player'].apply(lambda x: x['first_name'] if isinstance(x, dict) else None)
        df['player_last_name'] = df['player'].apply(lambda x: x['last_name'] if isinstance(x, dict) else None)
        df['player_position'] = df['player'].apply(lambda x: x['position'] if isinstance(x, dict) else None)
    
    if 'team' in df.columns:
        df['team_id'] = df['team'].apply(lambda x: x['id'] if isinstance(x, dict) else None)
        df['team_name'] = df['team'].apply(lambda x: x['full_name'] if isinstance(x, dict) else None)
        df['team_abbrev'] = df['team'].apply(lambda x: x['abbreviation'] if isinstance(x, dict) else None)
    
    if 'game' in df.columns:
        df['game_id'] = df['game'].apply(lambda x: x['id'] if isinstance(x, dict) else None)
        df['game_date'] = df['game'].apply(lambda x: x['date'] if isinstance(x, dict) else None)
    
    # Add date
    df['stat_date'] = date_str
    
    # Select key columns (all the stats BallDontLie provides)
    key_columns = [
        'id', 'game_id', 'player_id', 'team_id', 'team_abbrev',
        'player_first_name', 'player_last_name', 'player_position',
        'min', 'fgm', 'fga', 'fg_pct', 'fg3m', 'fg3a', 'fg3_pct',
        'ftm', 'fta', 'ft_pct', 'oreb', 'dreb', 'reb',
        'ast', 'stl', 'blk', 'turnover', 'pf', 'pts',
        'stat_date'
    ]
    
    # Keep only columns that exist
    available_columns = [col for col in key_columns if col in df.columns]
    df_clean = df[available_columns].copy()
    
    # Convert minutes (might be in "MM:SS" format)
    if 'min' in df_clean.columns:
        def convert_minutes(min_val):
            if pd.isna(min_val) or min_val == '':
                return 0.0
            try:
                if isinstance(min_val, str) and ':' in min_val:
                    parts = min_val.split(':')
                    return int(parts[0]) + int(parts[1]) / 60.0
                else:
                    return float(min_val)
            except:
                return 0.0
        
        df_clean['minutes_played'] = df_clean['min'].apply(convert_minutes)
    
    # Save to parquet
    out_file = os.path.join(out_dir, f'nba_box_scores_{date_str}.parquet')
    df_clean.to_parquet(out_file, index=False)
    
    print(f"✅ Box Scores: {len(df_clean)} player stats → {out_file}")
    
    # Show top scorers
    if len(df_clean) > 0 and 'pts' in df_clean.columns:
        print(f"\n🏆 Top scorers:")
        top_scorers = df_clean.nlargest(5, 'pts')
        for _, player in top_scorers.iterrows():
            name = f"{player.get('player_first_name', '')} {player.get('player_last_name', '')}".strip()
            team = player.get('team_abbrev', 'TEAM')
            points = player.get('pts', 0)
            rebounds = player.get('reb', 0)
            assists = player.get('ast', 0)
            print(f"   {name} ({team}): {points} pts, {rebounds} reb, {assists} ast")
    
    return out_file


def create_combined_daily_file(date_str: str, out_dir: str) -> Optional[str]:
    """
    Combine games and box scores into single daily file for easy analysis
    
    Args:
        date_str: Date in YYYY-MM-DD format
        out_dir: Output directory
    """
    print(f"\n📦 Creating combined daily file for {date_str}...")
    
    games_file = os.path.join(out_dir, f'nba_games_{date_str}.parquet')
    box_scores_file = os.path.join(out_dir, f'nba_box_scores_{date_str}.parquet')
    
    # Check if files exist
    if not os.path.exists(games_file):
        print(f"   ⚠️ Games file not found: {games_file}")
        return None
    
    if not os.path.exists(box_scores_file):
        print(f"   ⚠️ Box scores file not found: {box_scores_file}")
        return None
    
    try:
        # Read files
        games_df = pd.read_parquet(games_file)
        box_scores_df = pd.read_parquet(box_scores_file)
        
        if games_df.empty:
            print(f"   ℹ️ No games data to combine")
            return None
        
        # Merge on game_id
        combined_df = box_scores_df.merge(
            games_df,
            left_on='game_id',
            right_on='id',
            how='left',
            suffixes=('_player', '_game')
        )
        
        # Save combined file
        out_file = os.path.join(out_dir, f'nba_complete_{date_str}.parquet')
        combined_df.to_parquet(out_file, index=False)
        
        print(f"✅ Combined: {len(combined_df)} records → {out_file}")
        
        return out_file
        
    except Exception as e:
        print(f"   ❌ Failed to create combined file: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description='NBA Data Collection via BallDontLie API - Simple and Reliable'
    )
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--out-dir', default='stage', help='Output directory (default: stage)')
    parser.add_argument('--api-key', help='BallDontLie API key (optional, increases rate limit)')
    parser.add_argument('--season', type=int, help='Filter by season year (e.g., 2024)')
    parser.add_argument('--collect-teams', action='store_true', help='Also collect team data')
    parser.add_argument('--no-combined', action='store_true', help='Skip creating combined daily files')
    
    args = parser.parse_args()
    
    print("🏀 NBA Data Collection - BallDontLie API")
    print("=" * 60)
    print(f"📅 Date range: {args.start} to {args.end}")
    print(f"📁 Output: {args.out_dir}")
    
    # Create output directory
    os.makedirs(args.out_dir, exist_ok=True)
    
    # Create API client
    client = create_client(api_key=args.api_key)
    
    # Optionally collect teams
    if args.collect_teams:
        collect_teams(client, args.out_dir)
    
    # Parse date range
    try:
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
        
        if start_date > end_date:
            print("❌ Start date must be before or equal to end date")
            return 1
        
    except ValueError as e:
        print(f"❌ Invalid date format: {e}")
        print("   Use YYYY-MM-DD format (e.g., 2024-11-13)")
        return 1
    
    # Process each date
    total_days = (end_date - start_date).days + 1
    print(f"\n🔄 Processing {total_days} day(s)...")
    
    current_date = start_date
    success_count = 0
    error_count = 0
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        print(f"\n{'='*60}")
        print(f"📅 Processing {date_str} ({current_date.strftime('%A')})")
        print(f"{'='*60}")
        
        try:
            # Collect games
            games_file = collect_games(client, date_str, args.out_dir, args.season)
            
            # Collect box scores
            box_scores_file = collect_box_scores(client, date_str, args.out_dir)
            
            # Create combined file
            if not args.no_combined:
                combined_file = create_combined_daily_file(date_str, args.out_dir)
            
            success_count += 1
            print(f"\n✅ {date_str} completed successfully")
            
        except Exception as e:
            error_count += 1
            print(f"\n❌ {date_str} failed: {e}")
            continue
        
        current_date += timedelta(days=1)
    
    # Final summary
    print(f"\n{'='*60}")
    print(f"📊 Collection Summary")
    print(f"{'='*60}")
    print(f"   ✅ Successful: {success_count} days")
    print(f"   ❌ Failed: {error_count} days")
    print(f"   📈 Success rate: {(success_count/total_days)*100:.1f}%")
    print(f"   📁 Files saved to: {args.out_dir}")
    
    # API stats
    print(f"\n{'='*60}")
    client.print_stats()
    
    print(f"\n🎯 Next Steps:")
    print(f"   1. Check your data: ls {args.out_dir}/")
    print(f"   2. Convert to CSV: python convert_parquet_to_csv.py --input-dir {args.out_dir}")
    print(f"   3. Load to database: python loader/nba_load_balldontlie_data.py")
    print(f"   4. Send to Claude for betting analysis!")
    
    print(f"\n💡 What You Have:")
    print(f"   ✅ Games: Schedule, scores, team matchups")
    print(f"   ✅ Box Scores: All player stats per game")
    print(f"   ✅ Combined Files: Ready for analysis")
    
    print(f"\n🤖 What Claude Provides:")
    print(f"   🔍 Injury reports and player status")
    print(f"   📋 Starting lineups and rotations")
    print(f"   👔 Referee assignments and tendencies")
    print(f"   📊 Recent form and trends")
    print(f"   🎯 Betting insights and recommendations")
    
    return 0 if error_count == 0 else 1


if __name__ == '__main__':
    sys.exit(main())