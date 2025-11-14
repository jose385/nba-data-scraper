#!/usr/bin/env python3
"""
NBA BallDontLie Data Loader
Load parquet files into PostgreSQL database
"""

import argparse
import os
import sys
from pathlib import Path
import pandas as pd
import psycopg2
from datetime import datetime

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from py.nba_config_balldontlie import get_config


def load_teams_data(conn, teams_file: str) -> int:
    """Load teams reference data"""
    print(f"\n📋 Loading teams data...")
    
    try:
        df = pd.read_parquet(teams_file)
        
        if df.empty:
            print("   ⚠️ No teams data to load")
            return 0
        
        cursor = conn.cursor()
        
        # Clear existing teams
        cursor.execute("DELETE FROM nba_teams")
        
        # Insert teams
        insert_query = """
        INSERT INTO nba_teams (
            team_id, abbreviation, full_name, 
            conference, division, city
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (team_id) DO UPDATE SET
            abbreviation = EXCLUDED.abbreviation,
            full_name = EXCLUDED.full_name,
            conference = EXCLUDED.conference,
            division = EXCLUDED.division,
            city = EXCLUDED.city,
            last_updated = CURRENT_TIMESTAMP
        """
        
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row.get('id'),
                row.get('abbreviation'),
                row.get('full_name'),
                row.get('conference'),
                row.get('division'),
                row.get('city')
            ))
        
        conn.commit()
        print(f"   ✅ Loaded {len(df)} teams")
        return len(df)
        
    except Exception as e:
        conn.rollback()
        print(f"   ❌ Failed to load teams: {e}")
        return 0


def load_games_data(conn, games_file: str) -> int:
    """Load games data for a specific date"""
    print(f"\n📅 Loading games from {games_file}...")
    
    try:
        df = pd.read_parquet(games_file)
        
        if df.empty:
            print("   ℹ️ No games data to load")
            return 0
        
        cursor = conn.cursor()
        
        # Insert games
        insert_query = """
        INSERT INTO nba_games (
            game_id, game_date, season,
            home_team_id, away_team_id,
            home_team_abbrev, away_team_abbrev,
            home_team_score, away_team_score,
            status, period, time_remaining, postseason
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id) DO UPDATE SET
            home_team_score = EXCLUDED.home_team_score,
            away_team_score = EXCLUDED.away_team_score,
            status = EXCLUDED.status,
            period = EXCLUDED.period,
            time_remaining = EXCLUDED.time_remaining,
            last_updated = CURRENT_TIMESTAMP
        """
        
        loaded = 0
        for _, row in df.iterrows():
            try:
                cursor.execute(insert_query, (
                    row.get('id'),
                    row.get('game_date'),
                    row.get('season'),
                    row.get('home_team_id'),
                    row.get('away_team_id'),
                    row.get('home_team_abbrev'),
                    row.get('away_team_abbrev'),
                    row.get('home_team_score'),
                    row.get('away_team_score') or row.get('away_score'),  # Handle both column names
                    row.get('status'),
                    row.get('period'),
                    row.get('time'),
                    row.get('postseason', False)
                ))
                loaded += 1
            except Exception as e:
                print(f"   ⚠️ Skipped game {row.get('id')}: {e}")
        
        conn.commit()
        print(f"   ✅ Loaded {loaded} games")
        return loaded
        
    except Exception as e:
        conn.rollback()
        print(f"   ❌ Failed to load games: {e}")
        return 0


def load_box_scores_data(conn, box_scores_file: str) -> int:
    """Load box scores (player stats) for a specific date"""
    print(f"\n📊 Loading box scores from {box_scores_file}...")
    
    try:
        df = pd.read_parquet(box_scores_file)
        
        if df.empty:
            print("   ℹ️ No box scores data to load")
            return 0
        
        cursor = conn.cursor()
        
        # Insert box scores
        insert_query = """
        INSERT INTO nba_box_scores (
            stat_id, game_id, player_id, team_id,
            player_first_name, player_last_name, player_position, team_abbrev,
            minutes_played,
            field_goals_made, field_goals_attempted, field_goal_pct,
            three_pointers_made, three_pointers_attempted, three_point_pct,
            free_throws_made, free_throws_attempted, free_throw_pct,
            offensive_rebounds, defensive_rebounds, total_rebounds,
            assists, steals, blocks, turnovers, personal_fouls, points,
            stat_date
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (stat_id) DO UPDATE SET
            minutes_played = EXCLUDED.minutes_played,
            field_goals_made = EXCLUDED.field_goals_made,
            field_goals_attempted = EXCLUDED.field_goals_attempted,
            field_goal_pct = EXCLUDED.field_goal_pct,
            three_pointers_made = EXCLUDED.three_pointers_made,
            three_pointers_attempted = EXCLUDED.three_pointers_attempted,
            three_point_pct = EXCLUDED.three_point_pct,
            free_throws_made = EXCLUDED.free_throws_made,
            free_throws_attempted = EXCLUDED.free_throws_attempted,
            free_throw_pct = EXCLUDED.free_throw_pct,
            offensive_rebounds = EXCLUDED.offensive_rebounds,
            defensive_rebounds = EXCLUDED.defensive_rebounds,
            total_rebounds = EXCLUDED.total_rebounds,
            assists = EXCLUDED.assists,
            steals = EXCLUDED.steals,
            blocks = EXCLUDED.blocks,
            turnovers = EXCLUDED.turnovers,
            personal_fouls = EXCLUDED.personal_fouls,
            points = EXCLUDED.points,
            last_updated = CURRENT_TIMESTAMP
        """
        
        loaded = 0
        for _, row in df.iterrows():
            try:
                cursor.execute(insert_query, (
                    row.get('id'),
                    row.get('game_id'),
                    row.get('player_id'),
                    row.get('team_id'),
                    row.get('player_first_name'),
                    row.get('player_last_name'),
                    row.get('player_position'),
                    row.get('team_abbrev'),
                    row.get('minutes_played'),
                    row.get('fgm'),
                    row.get('fga'),
                    row.get('fg_pct'),
                    row.get('fg3m'),
                    row.get('fg3a'),
                    row.get('fg3_pct'),
                    row.get('ftm'),
                    row.get('fta'),
                    row.get('ft_pct'),
                    row.get('oreb'),
                    row.get('dreb'),
                    row.get('reb'),
                    row.get('ast'),
                    row.get('stl'),
                    row.get('blk'),
                    row.get('turnover'),
                    row.get('pf'),
                    row.get('pts'),
                    row.get('stat_date')
                ))
                loaded += 1
            except Exception as e:
                print(f"   ⚠️ Skipped stat {row.get('id')}: {e}")
        
        conn.commit()
        print(f"   ✅ Loaded {loaded} box scores")
        return loaded
        
    except Exception as e:
        conn.rollback()
        print(f"   ❌ Failed to load box scores: {e}")
        return 0


def main():
    parser = argparse.ArgumentParser(
        description='Load NBA BallDontLie data into PostgreSQL'
    )
    parser.add_argument('--input-dir', default='stage', help='Input directory with parquet files')
    parser.add_argument('--date', help='Specific date to load (YYYY-MM-DD)')
    parser.add_argument('--load-teams', action='store_true', help='Load teams reference data')
    
    args = parser.parse_args()
    
    print("🏀 NBA BallDontLie Data Loader")
    print("=" * 50)
    
    # Get configuration
    config = get_config()
    
    if not config.PG_DSN:
        print("❌ Database not configured!")
        print("   Set NBA_PG_DSN in .env file")
        return 1
    
    # Test database connection
    db_ok, db_msg = config.test_database_connection()
    if not db_ok:
        print(f"❌ Database connection failed: {db_msg}")
        return 1
    
    print(f"✅ Database connected")
    print(f"📁 Input directory: {args.input_dir}")
    
    # Connect to database
    try:
        conn = psycopg2.connect(config.PG_DSN)
        print("✅ Database connection established")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return 1
    
    total_loaded = 0
    
    try:
        # Load teams if requested
        if args.load_teams:
            teams_file = os.path.join(args.input_dir, 'nba_teams.parquet')
            if os.path.exists(teams_file):
                total_loaded += load_teams_data(conn, teams_file)
            else:
                print(f"⚠️ Teams file not found: {teams_file}")
        
        # Load specific date or all dates
        input_path = Path(args.input_dir)
        
        if args.date:
            # Load specific date
            games_file = input_path / f'nba_games_{args.date}.parquet'
            box_scores_file = input_path / f'nba_box_scores_{args.date}.parquet'
            
            if games_file.exists():
                total_loaded += load_games_data(conn, str(games_file))
            else:
                print(f"⚠️ Games file not found: {games_file}")
            
            if box_scores_file.exists():
                total_loaded += load_box_scores_data(conn, str(box_scores_file))
            else:
                print(f"⚠️ Box scores file not found: {box_scores_file}")
        else:
            # Load all dates found in directory
            games_files = sorted(input_path.glob('nba_games_*.parquet'))
            
            if not games_files:
                print(f"⚠️ No game files found in {args.input_dir}")
                return 1
            
            print(f"\n🔄 Found {len(games_files)} game file(s)")
            
            for games_file in games_files:
                # Extract date from filename
                date_str = games_file.stem.replace('nba_games_', '')
                
                # Load games
                total_loaded += load_games_data(conn, str(games_file))
                
                # Load corresponding box scores
                box_scores_file = input_path / f'nba_box_scores_{date_str}.parquet'
                if box_scores_file.exists():
                    total_loaded += load_box_scores_data(conn, str(box_scores_file))
        
        # Summary
        print(f"\n{'='*50}")
        print(f"📊 Loading Complete")
        print(f"{'='*50}")
        print(f"   ✅ Total records loaded: {total_loaded}")
        
        # Database stats
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM nba_teams")
        teams_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM nba_games")
        games_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM nba_box_scores")
        box_scores_count = cursor.fetchone()[0]
        
        print(f"\n📊 Database Summary:")
        print(f"   Teams: {teams_count}")
        print(f"   Games: {games_count}")
        print(f"   Box Scores: {box_scores_count}")
        
        if games_count > 0:
            cursor.execute("""
                SELECT MIN(game_date), MAX(game_date) 
                FROM nba_games
            """)
            min_date, max_date = cursor.fetchone()
            print(f"   Date Range: {min_date} to {max_date}")
        
        print(f"\n🎯 Next Steps:")
        print(f"   1. Query your data: psql -d your_db -c 'SELECT * FROM nba_games LIMIT 5'")
        print(f"   2. Use views: SELECT * FROM recent_player_performance")
        print(f"   3. Send to Claude for betting analysis!")
        
        return 0
        
    except Exception as e:
        print(f"❌ Loading failed: {e}")
        return 1
    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())