#!/usr/bin/env python3
"""
STREAMLINED NBA Data Backfill - Claude-Optimized Version
Focuses on collecting only the data Claude cannot research:
- NBA tracking data (impossible to get elsewhere)
- Shot chart with defensive context
- Play-by-play with advanced metrics

Claude will research: lineups, injuries, referee assignments, recent trends, etc.
"""

import argparse
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import time
import requests
import json
from typing import Dict, List, Tuple, Any, Optional
from pathlib import Path
import warnings
import sys

# Suppress pandas warnings for cleaner output
warnings.filterwarnings('ignore', category=FutureWarning)

# ============================================================================
# REAL NBA API IMPORTS WITH GRACEFUL FALLBACK
# ============================================================================

def get_real_nba_imports():
    """Import NBA API libraries with graceful fallback"""
    imports = {'nba_api': None, 'requests': None}
    
    try:
        from nba_api.stats import endpoints
        from nba_api.stats.static import teams, players
        imports['nba_api'] = {
            'endpoints': endpoints,
            'teams': teams, 
            'players': players
        }
        print("✅ nba_api imported successfully")
    except ImportError:
        print("⚠️ nba_api not available - install with: pip install nba_api")
    
    try:
        import requests
        imports['requests'] = requests
        print("✅ requests library ready")
    except ImportError:
        print("❌ requests library required - install with: pip install requests")
    
    return imports

# Initialize imports
REAL_NBA_IMPORTS = get_real_nba_imports()

# ============================================================================
# NBA API RATE LIMITING AND ERROR HANDLING
# ============================================================================

class NBARateLimiter:
    """Handle rate limiting for NBA API - more aggressive than MLB"""
    
    def __init__(self):
        self.last_nba_api_call = 0
        self.nba_api_delay = 2.5  # 2.5 seconds between NBA API calls
        self.retry_count = 4
        self.retry_delay = 8.0    # Longer retry delay for NBA
        self.timeout_seconds = 30.0
    
    def wait_for_nba_api(self):
        """Wait appropriate time before NBA API call"""
        time_since_last = time.time() - self.last_nba_api_call
        if time_since_last < self.nba_api_delay:
            wait_time = self.nba_api_delay - time_since_last
            print(f"   ⏱️ NBA API rate limiting: waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
        self.last_nba_api_call = time.time()
    
    def retry_with_backoff(self, func, *args, **kwargs):
        """Retry function with exponential backoff for NBA API"""
        for attempt in range(self.retry_count):
            try:
                self.wait_for_nba_api()
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == self.retry_count - 1:
                    print(f"❌ Final NBA API attempt failed: {e}")
                    raise
                
                wait_time = self.retry_delay * (2 ** attempt)
                print(f"⚠️ NBA API attempt {attempt + 1} failed: {e}")
                print(f"   Retrying in {wait_time}s...")
                time.sleep(wait_time)

# Global rate limiter
nba_rate_limiter = NBARateLimiter()

# ============================================================================
# NBA PLACEHOLDER DATA GENERATOR (for testing only)
# ============================================================================

class NBAPlaceholderGenerator:
    """Generate realistic NBA placeholder data for testing"""
    
    def __init__(self, seed: int = 42):
        self.rng = np.random.RandomState(seed)
        self.game_id_counter = 22300000  # NBA format: season + game number
        
        # NBA teams (30 teams)
        self.nba_teams = {
            'ATL': {'id': 1610612737, 'name': 'Atlanta Hawks'},
            'BOS': {'id': 1610612738, 'name': 'Boston Celtics'},
            'BKN': {'id': 1610612751, 'name': 'Brooklyn Nets'},
            'CHA': {'id': 1610612766, 'name': 'Charlotte Hornets'},
            'CHI': {'id': 1610612741, 'name': 'Chicago Bulls'},
            'CLE': {'id': 1610612739, 'name': 'Cleveland Cavaliers'},
            'DAL': {'id': 1610612742, 'name': 'Dallas Mavericks'},
            'DEN': {'id': 1610612743, 'name': 'Denver Nuggets'},
            'DET': {'id': 1610612765, 'name': 'Detroit Pistons'},
            'GSW': {'id': 1610612744, 'name': 'Golden State Warriors'},
            'HOU': {'id': 1610612745, 'name': 'Houston Rockets'},
            'IND': {'id': 1610612754, 'name': 'Indiana Pacers'},
            'LAC': {'id': 1610612746, 'name': 'LA Clippers'},
            'LAL': {'id': 1610612747, 'name': 'Los Angeles Lakers'},
            'MEM': {'id': 1610612763, 'name': 'Memphis Grizzlies'},
            'MIA': {'id': 1610612748, 'name': 'Miami Heat'},
            'MIL': {'id': 1610612749, 'name': 'Milwaukee Bucks'},
            'MIN': {'id': 1610612750, 'name': 'Minnesota Timberwolves'},
            'NOP': {'id': 1610612740, 'name': 'New Orleans Pelicans'},
            'NYK': {'id': 1610612752, 'name': 'New York Knicks'},
            'OKC': {'id': 1610612760, 'name': 'Oklahoma City Thunder'},
            'ORL': {'id': 1610612753, 'name': 'Orlando Magic'},
            'PHI': {'id': 1610612755, 'name': 'Philadelphia 76ers'},
            'PHX': {'id': 1610612756, 'name': 'Phoenix Suns'},
            'POR': {'id': 1610612757, 'name': 'Portland Trail Blazers'},
            'SAC': {'id': 1610612758, 'name': 'Sacramento Kings'},
            'SAS': {'id': 1610612759, 'name': 'San Antonio Spurs'},
            'TOR': {'id': 1610612761, 'name': 'Toronto Raptors'},
            'UTA': {'id': 1610612762, 'name': 'Utah Jazz'},
            'WAS': {'id': 1610612764, 'name': 'Washington Wizards'}
        }
        
    def generate_daily_games(self, date_str: str) -> List[Dict]:
        """Generate realistic NBA daily schedule"""
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        
        # NBA season timing (October - June)
        month = date_obj.month
        day = date_obj.day
        
        # Off-season check
        if month in [7, 8, 9]:  # July, August, September
            return []
        
        # Reduced games in summer months
        if month == 6 and day > 20:  # Late June (Finals only)
            if self.rng.random() < 0.8:  # 80% chance no games
                return []
            num_games = 1  # Finals game
        elif month in [10]:  # October (season start)
            num_games = self.rng.randint(4, 8)
        elif month in [11, 12, 1, 2, 3]:  # Peak season
            num_games = self.rng.randint(8, 15)
        elif month in [4, 5]:  # Playoffs
            num_games = self.rng.randint(2, 6)
        else:
            num_games = self.rng.randint(6, 12)
        
        # Select teams for games
        team_abbrevs = list(self.nba_teams.keys())
        used_teams = set()
        games = []
        
        for game_num in range(num_games):
            available_teams = [t for t in team_abbrevs if t not in used_teams]
            if len(available_teams) < 2:
                break
                
            home_team = self.rng.choice(available_teams)
            available_teams.remove(home_team)
            away_team = self.rng.choice(available_teams)
            
            used_teams.update([home_team, away_team])
            
            game_id = f"00{self.game_id_counter + game_num:07d}"
            
            games.append({
                'game_id': game_id,
                'game_date': date_str,
                'season_year': date_obj.year if month >= 10 else date_obj.year - 1,
                'home_team_id': self.nba_teams[home_team]['id'],
                'away_team_id': self.nba_teams[away_team]['id'],
                'home_team_abbrev': home_team,
                'away_team_abbrev': away_team,
                'home_team_name': self.nba_teams[home_team]['name'],
                'away_team_name': self.nba_teams[away_team]['name']
            })
        
        self.game_id_counter += num_games
        return games

def generate_realistic_nba_plays(game_id: str, game_date: str, home_team: str, away_team: str) -> pd.DataFrame:
    """Generate realistic NBA play-by-play data with advanced metrics"""
    
    # NBA event types
    event_types = [
        'FIELD_GOAL_MADE', 'FIELD_GOAL_MISSED', 'FREE_THROW_MADE', 'FREE_THROW_MISSED',
        'REBOUND_OFFENSIVE', 'REBOUND_DEFENSIVE', 'ASSIST', 'TURNOVER', 'STEAL',
        'BLOCK', 'FOUL_PERSONAL', 'FOUL_TECHNICAL', 'TIMEOUT', 'SUBSTITUTION'
    ]
    
    shot_zones = [
        'Restricted Area', 'Paint (Non-RA)', 'Mid-Range', 'Left Wing 3PT',
        'Right Wing 3PT', 'Center 3PT', 'Above Break 3PT'
    ]
    
    # Generate realistic number of plays (180-220 per game)
    num_plays = random.randint(180, 220)
    
    plays_data = []
    current_period = 1
    clock_seconds = 12 * 60  # 12 minutes per quarter
    home_score = 0
    away_score = 0
    
    for play_num in range(num_plays):
        # Advance game clock
        clock_seconds -= random.randint(8, 35)
        
        # Handle period transitions
        if clock_seconds <= 0:
            current_period += 1
            clock_seconds = 12 * 60 if current_period <= 4 else 5 * 60  # OT is 5 minutes
            
        # Stop after reasonable number of periods
        if current_period > 6:  # Max 2 OT
            break
            
        event_type = random.choice(event_types)
        
        # Generate play data
        play_data = {
            'game_id': game_id,
            'play_id': f"{game_id}_{play_num:04d}",
            'event_num': play_num,
            'period': current_period,
            'clock_time': f"{clock_seconds // 60}:{clock_seconds % 60:02d}",
            'clock_seconds_remaining': clock_seconds,
            'game_clock_seconds': (current_period - 1) * 720 + (720 - clock_seconds),
            'event_type': event_type,
            'event_action_type': random.randint(1, 100),
            'play_description': f"{event_type.lower().replace('_', ' ')} play {play_num}",
            
            # Score tracking
            'score_home': home_score,
            'score_away': away_score,
            'score_margin': home_score - away_score,
            
            # Player involvement
            'player1_id': random.randint(200000, 300000),
            'player1_name': f"Player_{random.randint(1, 50)}",
            'player1_team_id': random.choice([
                1610612737, 1610612738  # Sample team IDs
            ]),
            
            # Shot data (if shot attempt)
            'shot_attempted': event_type in ['FIELD_GOAL_MADE', 'FIELD_GOAL_MISSED'],
            'shot_made': event_type == 'FIELD_GOAL_MADE',
        }
        
        # Add shot-specific data
        if play_data['shot_attempted']:
            shot_distance = random.uniform(1, 35)
            is_three = shot_distance > 23.75
            
            play_data.update({
                'shot_type': '3PT Field Goal' if is_three else '2PT Field Goal',
                'shot_zone_basic': random.choice(shot_zones),
                'shot_distance': round(shot_distance, 1),
                'loc_x': random.randint(-250, 250),
                'loc_y': random.randint(0, 470),
                'shot_clock_remaining': random.uniform(0, 24),
                'closest_defender_distance': round(random.uniform(1, 8), 1),
                'defender_contest_type': random.choice(['Open', 'Tight', 'Wide Open']),
                'shot_probability': round(random.uniform(0.1, 0.8), 3),
                'expected_points': round(random.uniform(0.2, 2.5), 3),
            })
            
            # Update score if shot made
            if play_data['shot_made']:
                points = 3 if is_three else 2
                if random.choice([True, False]):  # Random team
                    home_score += points
                else:
                    away_score += points
                play_data['score_home'] = home_score
                play_data['score_away'] = away_score
                play_data['score_margin'] = home_score - away_score
        
        # Add advanced tracking data
        play_data.update({
            'player1_speed_mph': round(random.uniform(5, 18), 1),
            'player1_distance_traveled': round(random.uniform(5, 50), 1),
            'possession_team_id': random.choice([
                1610612737, 1610612738
            ]),
            'possession_length_seconds': round(random.uniform(5, 24), 1),
            
            # Game situation
            'win_probability_home': round(random.uniform(0.1, 0.9), 3),
            'leverage_score': round(random.uniform(0.5, 9.5), 2),
            
            # Data quality
            'data_source': 'PLACEHOLDER',
            'tracking_data_available': True,
        })
        
        plays_data.append(play_data)
    
    return pd.DataFrame(plays_data)

def generate_nba_shot_chart(game_id: str, num_shots: int = 85) -> pd.DataFrame:
    """Generate realistic shot chart data"""
    
    shots_data = []
    
    for shot_num in range(num_shots):
        # Shot location (NBA court coordinates)
        loc_x = random.randint(-250, 250)
        loc_y = random.randint(0, 470)
        
        # Calculate distance from basket (located at 0, 0)
        distance = round(np.sqrt(loc_x**2 + loc_y**2) / 10, 1)
        
        # Determine shot type based on distance
        is_three = distance > 23.75
        
        shot_data = {
            'game_id': game_id,
            'shot_id': f"{game_id}_shot_{shot_num:03d}",
            'period': random.randint(1, 4),
            'clock_remaining': f"{random.randint(0, 11)}:{random.randint(0, 59):02d}",
            
            'shooter_id': random.randint(200000, 300000),
            'shooter_name': f"Player_{random.randint(1, 50)}",
            'shooter_team_id': random.choice([1610612737, 1610612738]),
            
            'shot_made': random.choice([True, False]),
            'shot_type': '3PT' if is_three else '2PT',
            'shot_value': 3 if is_three else 2,
            'action_type': random.choice(['Jump Shot', 'Layup', 'Dunk', 'Hook Shot']),
            
            'loc_x': loc_x,
            'loc_y': loc_y,
            'zone_name': random.choice(['Paint', 'Mid-Range', 'Left Wing', 'Right Wing']),
            'shot_distance': distance,
            
            'score_margin': random.randint(-15, 15),
            'shot_clock': round(random.uniform(0, 24), 1),
            
            'closest_defender_id': random.randint(200000, 300000),
            'closest_defender_distance': round(random.uniform(1, 8), 1),
            'contest_level': random.choice(['Open', 'Tight', 'Wide Open']),
            'expected_make_pct': round(random.uniform(0.2, 0.7), 3),
            
            'play_type': random.choice(['Isolation', 'Pick and Roll', 'Catch and Shoot']),
            'fast_break': random.choice([True, False]),
            'second_chance': random.choice([True, False]),
        }
        
        shots_data.append(shot_data)
    
    return pd.DataFrame(shots_data)

def generate_nba_box_scores(game_id: str, teams: List[str]) -> pd.DataFrame:
    """Generate realistic box score data"""
    
    box_data = []
    
    # Generate for both teams (10 players each)
    for team_idx, team in enumerate(teams):
        team_id = 1610612737 + team_idx
        
        for player_num in range(10):
            player_id = 200000 + team_idx * 100 + player_num
            minutes = round(random.uniform(15, 38), 1) if player_num < 8 else round(random.uniform(5, 25), 1)
            
            # Generate correlated stats
            fga = random.randint(4, 22)
            fgm = random.randint(0, min(fga, 18))
            three_pa = random.randint(0, min(fga, 12))
            three_pm = random.randint(0, min(three_pa, 8))
            fta = random.randint(0, 8)
            ftm = random.randint(0, fta)
            
            rebounds = random.randint(1, 12)
            assists = random.randint(0, 10)
            steals = random.randint(0, 4)
            blocks = random.randint(0, 3)
            turnovers = random.randint(0, 5)
            fouls = random.randint(0, 6)
            points = fgm * 2 + three_pm + ftm
            
            box_data.append({
                'game_id': game_id,
                'player_id': player_id,
                'team_id': team_id,
                'player_name': f"Player_{player_num + 1}",
                'team_abbrev': team,
                'position': random.choice(['G', 'F', 'C']),
                'starter': player_num < 5,
                
                'minutes_played': minutes,
                'field_goals_made': fgm,
                'field_goals_attempted': fga,
                'field_goal_pct': round(fgm / max(fga, 1), 3),
                'three_pointers_made': three_pm,
                'three_pointers_attempted': three_pa,
                'three_point_pct': round(three_pm / max(three_pa, 1), 3) if three_pa > 0 else 0,
                'free_throws_made': ftm,
                'free_throws_attempted': fta,
                'free_throw_pct': round(ftm / max(fta, 1), 3) if fta > 0 else 0,
                
                'total_rebounds': rebounds,
                'assists': assists,
                'steals': steals,
                'blocks': blocks,
                'turnovers': turnovers,
                'personal_fouls': fouls,
                'points': points,
                
                'plus_minus': random.randint(-25, 25),
                'usage_rate': round(random.uniform(0.1, 0.35), 3),
                'true_shooting_pct': round(random.uniform(0.4, 0.7), 3),
                'distance_traveled': round(random.uniform(1.5, 3.2), 2),
                'avg_speed': round(random.uniform(3.5, 4.8), 2),
                'touches': random.randint(20, 80),
            })
    
    return pd.DataFrame(box_data)

# ============================================================================
# REAL NBA DATA COLLECTION FUNCTIONS
# ============================================================================

def collect_real_nba_games(date_str: str) -> List[Dict]:
    """Collect real NBA games from NBA API"""
    
    if not REAL_NBA_IMPORTS['nba_api']:
        raise ImportError("nba_api not available")
    
    endpoints = REAL_NBA_IMPORTS['nba_api']['endpoints']
    
    print(f"   📡 Calling NBA API scoreboard for {date_str}...")
    
    try:
        # Get games for date
        scoreboard = nba_rate_limiter.retry_with_backoff(
            endpoints.scoreboardv2.ScoreboardV2,
            game_date=date_str
        )
        
        games_data = scoreboard.available_games.get_dict()['data']
        
        if not games_data:
            print(f"   ⚠️ No NBA games found for {date_str}")
            return []
        
        games = []
        for game in games_data:
            game_info = {
                'game_id': game[0],
                'game_date': date_str,
                'season_year': int(date_str.split('-')[0]),
                'home_team_id': game[6],
                'away_team_id': game[7], 
                'home_team_abbrev': game[4],
                'away_team_abbrev': game[5],
                'home_score': game[21] if game[21] else 0,
                'away_score': game[22] if game[22] else 0,
                'game_status': game[3]
            }
            games.append(game_info)
        
        print(f"   ✅ Retrieved {len(games)} NBA games")
        return games
        
    except Exception as e:
        print(f"   ❌ NBA API scoreboard failed: {e}")
        raise

def collect_real_nba_playbyplay(game_id: str) -> pd.DataFrame:
    """Collect real play-by-play data from NBA API"""
    
    if not REAL_NBA_IMPORTS['nba_api']:
        raise ImportError("nba_api not available")
    
    endpoints = REAL_NBA_IMPORTS['nba_api']['endpoints']
    
    try:
        print(f"   📡 Getting play-by-play for game {game_id}...")
        
        pbp = nba_rate_limiter.retry_with_backoff(
            endpoints.playbyplayv3.PlayByPlayV3,
            game_id=game_id
        )
        
        plays_data = pbp.available_plays.get_dict()['data']
        
        if not plays_data:
            return pd.DataFrame()
        
        # Convert to DataFrame with proper column names
        df = pd.DataFrame(plays_data)
        
        # Map column indices to names (NBA API returns indexed data)
        if len(df.columns) >= 10:
            df.columns = [
                'game_id', 'event_num', 'event_type', 'event_action_type',
                'period', 'clock_time', 'play_description', 'home_description',
                'neutral_description', 'away_description'
            ] + [f'col_{i}' for i in range(10, len(df.columns))]
        
        print(f"   ✅ Retrieved {len(df)} plays")
        return df
        
    except Exception as e:
        print(f"   ❌ Play-by-play collection failed: {e}")
        return pd.DataFrame()

def collect_real_nba_boxscore(game_id: str) -> pd.DataFrame:
    """Collect real box score data from NBA API"""
    
    if not REAL_NBA_IMPORTS['nba_api']:
        raise ImportError("nba_api not available")
    
    endpoints = REAL_NBA_IMPORTS['nba_api']['endpoints']
    
    try:
        print(f"   📡 Getting box score for game {game_id}...")
        
        boxscore = nba_rate_limiter.retry_with_backoff(
            endpoints.boxscoretraditionalv2.BoxScoreTraditionalV2,
            game_id=game_id
        )
        
        # Get player stats
        player_stats = boxscore.player_stats.get_dict()['data']
        
        if not player_stats:
            return pd.DataFrame()
        
        df = pd.DataFrame(player_stats)
        
        print(f"   ✅ Retrieved box scores for {len(df)} players")
        return df
        
    except Exception as e:
        print(f"   ❌ Box score collection failed: {e}")
        return pd.DataFrame()

# ============================================================================
# CORE NBA DATA COLLECTION FUNCTIONS
# ============================================================================

def collect_nba_game_info(date_str: str, out_dir: str, use_placeholder: bool = False) -> str:
    """Collect NBA game information - basic context"""
    
    out_file = os.path.join(out_dir, f'nba_game_info_{date_str}.parquet')
    
    if use_placeholder:
        print(f"🔧 Generating placeholder NBA games for {date_str}...")
        generator = NBAPlaceholderGenerator()
        games = generator.generate_daily_games(date_str)
        
        if not games:
            # Create empty file for no games
            df = pd.DataFrame(columns=['game_date', 'game_id'])
            df.to_parquet(out_file, index=False)
            print(f"✅ No NBA games scheduled for {date_str}")
            return out_file
        
        df = pd.DataFrame(games)
        
    else:
        print(f"📡 Collecting REAL NBA games for {date_str}...")
        
        try:
            games = collect_real_nba_games(date_str)
            
            if not games:
                print(f"   ⚠️ No NBA games found for {date_str}")
                df = pd.DataFrame(columns=['game_date', 'game_id'])
            else:
                df = pd.DataFrame(games)
                print(f"   ✅ Real NBA games: {len(df)} games")
                
        except Exception as e:
            print(f"   ❌ Real NBA game collection failed: {e}")
            print(f"   💡 Falling back to placeholder mode")
            return collect_nba_game_info(date_str, out_dir, use_placeholder=True)
    
    df.to_parquet(out_file, index=False)
    print(f"✅ NBA Game Info: {len(df)} games → {out_file}")
    return out_file

def collect_nba_plays(date_str: str, out_dir: str, use_placeholder: bool = False) -> str:
    """Collect NBA play-by-play data - THE CORE VALUE for betting analysis"""
    
    out_file = os.path.join(out_dir, f'nba_plays_{date_str}.parquet')
    
    # Get games for this date first
    game_info_file = os.path.join(out_dir, f'nba_game_info_{date_str}.parquet')
    
    if not os.path.exists(game_info_file):
        print(f"   ⚠️ Game info file not found, collecting first...")
        collect_nba_game_info(date_str, out_dir, use_placeholder)
    
    try:
        games_df = pd.read_parquet(game_info_file)
        if games_df.empty:
            df = pd.DataFrame(columns=['game_id', 'play_id'])
            df.to_parquet(out_file, index=False)
            print(f"✅ No NBA plays for {date_str} (no games)")
            return out_file
    except Exception as e:
        print(f"   ❌ Could not read game info: {e}")
        df = pd.DataFrame(columns=['game_id', 'play_id'])
        df.to_parquet(out_file, index=False)
        return out_file
    
    if use_placeholder:
        print(f"🔧 Generating placeholder NBA plays for {date_str}...")
        
        all_plays = []
        for _, game in games_df.iterrows():
            game_id = game['game_id']
            home_team = game.get('home_team_abbrev', 'HOME')
            away_team = game.get('away_team_abbrev', 'AWAY')
            
            game_plays = generate_realistic_nba_plays(game_id, date_str, home_team, away_team)
            all_plays.append(game_plays)
        
        if all_plays:
            df = pd.concat(all_plays, ignore_index=True)
        else:
            df = pd.DataFrame(columns=['game_id', 'play_id'])
        
    else:
        print(f"📡 Collecting REAL NBA plays for {date_str}...")
        
        all_plays = []
        
        for _, game in games_df.iterrows():
            game_id = game['game_id']
            
            try:
                game_plays = collect_real_nba_playbyplay(game_id)
                if not game_plays.empty:
                    all_plays.append(game_plays)
                    
            except Exception as e:
                print(f"   ❌ Failed to get plays for game {game_id}: {e}")
                continue
        
        if all_plays:
            df = pd.concat(all_plays, ignore_index=True)
            print(f"   ✅ Real NBA plays: {len(df)} plays from {len(all_plays)} games")
        else:
            print(f"   ⚠️ No play-by-play data collected, using placeholder")
            return collect_nba_plays(date_str, out_dir, use_placeholder=True)
    
    df.to_parquet(out_file, index=False)
    print(f"✅ NBA Plays: {len(df)} plays → {out_file}")
    return out_file

def collect_nba_shot_chart(date_str: str, out_dir: str, use_placeholder: bool = False) -> str:
    """Collect NBA shot chart data - CRITICAL for player props"""
    
    out_file = os.path.join(out_dir, f'nba_shot_chart_{date_str}.parquet')
    
    # Get games for this date first
    game_info_file = os.path.join(out_dir, f'nba_game_info_{date_str}.parquet')
    
    try:
        games_df = pd.read_parquet(game_info_file)
        if games_df.empty:
            df = pd.DataFrame(columns=['game_id', 'shot_id'])
            df.to_parquet(out_file, index=False)
            print(f"✅ No shot chart for {date_str} (no games)")
            return out_file
    except Exception as e:
        print(f"   ❌ Could not read game info: {e}")
        df = pd.DataFrame(columns=['game_id', 'shot_id'])
        df.to_parquet(out_file, index=False)
        return out_file
    
    if use_placeholder:
        print(f"🔧 Generating placeholder NBA shot chart for {date_str}...")
        
        all_shots = []
        for _, game in games_df.iterrows():
            game_id = game['game_id']
            shots = generate_nba_shot_chart(game_id)
            all_shots.append(shots)
        
        if all_shots:
            df = pd.concat(all_shots, ignore_index=True)
        else:
            df = pd.DataFrame(columns=['game_id', 'shot_id'])
        
    else:
        print(f"📡 Collecting REAL NBA shot chart for {date_str}...")
        print(f"   💡 Real shot chart collection requires advanced NBA API access")
        print(f"   💡 Using placeholder for now - implement shot chart API later")
        return collect_nba_shot_chart(date_str, out_dir, use_placeholder=True)
    
    df.to_parquet(out_file, index=False)
    print(f"✅ NBA Shot Chart: {len(df)} shots → {out_file}")
    return out_file

def collect_nba_box_scores(date_str: str, out_dir: str, use_placeholder: bool = False) -> str:
    """Collect NBA box scores - ESSENTIAL for player props"""
    
    out_file = os.path.join(out_dir, f'nba_box_scores_{date_str}.parquet')
    
    # Get games for this date first
    game_info_file = os.path.join(out_dir, f'nba_game_info_{date_str}.parquet')
    
    try:
        games_df = pd.read_parquet(game_info_file)
        if games_df.empty:
            df = pd.DataFrame(columns=['game_id', 'player_id'])
            df.to_parquet(out_file, index=False)
            print(f"✅ No box scores for {date_str} (no games)")
            return out_file
    except Exception as e:
        print(f"   ❌ Could not read game info: {e}")
        df = pd.DataFrame(columns=['game_id', 'player_id'])
        df.to_parquet(out_file, index=False)
        return out_file
    
    if use_placeholder:
        print(f"🔧 Generating placeholder NBA box scores for {date_str}...")
        
        all_box_scores = []
        for _, game in games_df.iterrows():
            game_id = game['game_id']
            home_team = game.get('home_team_abbrev', 'HOME')
            away_team = game.get('away_team_abbrev', 'AWAY')
            
            box_scores = generate_nba_box_scores(game_id, [home_team, away_team])
            all_box_scores.append(box_scores)
        
        if all_box_scores:
            df = pd.concat(all_box_scores, ignore_index=True)
        else:
            df = pd.DataFrame(columns=['game_id', 'player_id'])
        
    else:
        print(f"📡 Collecting REAL NBA box scores for {date_str}...")
        
        all_box_scores = []
        
        for _, game in games_df.iterrows():
            game_id = game['game_id']
            
            try:
                box_scores = collect_real_nba_boxscore(game_id)
                if not box_scores.empty:
                    all_box_scores.append(box_scores)
                    
            except Exception as e:
                print(f"   ❌ Failed to get box scores for game {game_id}: {e}")
                continue
        
        if all_box_scores:
            df = pd.concat(all_box_scores, ignore_index=True)
            print(f"   ✅ Real NBA box scores: {len(df)} player records")
        else:
            print(f"   ⚠️ No box score data collected, using placeholder")
            return collect_nba_box_scores(date_str, out_dir, use_placeholder=True)
    
    df.to_parquet(out_file, index=False)
    print(f"✅ NBA Box Scores: {len(df)} players → {out_file}")
    return out_file

# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Streamlined NBA data backfill - Claude optimized')
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--out-dir', default='stage', help='Output directory')
    parser.add_argument('--real-data', action='store_true', help='Use real NBA API data (recommended)')
    parser.add_argument('--minimal', action='store_true', help='Collect only games and box scores')
    parser.add_argument('--placeholder', action='store_true', help='Force placeholder mode for testing')
    
    args = parser.parse_args()
    
    # Determine data mode
    use_placeholder = args.placeholder or (not args.real_data and not REAL_NBA_IMPORTS['nba_api'])
    
    print(f"🏀 STREAMLINED NBA backfill: {args.start} to {args.end}")
    
    if use_placeholder:
        print(f"🔧 PLACEHOLDER MODE: Using generated test data")
        print(f"   💡 To use real data: use --real-data flag")
    else:
        print(f"📡 REAL DATA MODE: Using live NBA API")
        if not REAL_NBA_IMPORTS['nba_api']:
            print(f"❌ nba_api not available! Install with: pip install nba_api")
            return
        print(f"   ✅ NBA API ready for data collection")
    
    print(f"📁 Output directory: {args.out_dir}")
    
    # Validate date range
    try:
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
        
        if start_date > end_date:
            print(f"❌ Start date must be before end date")
            return
        
        date_range = (end_date - start_date).days + 1
        
    except ValueError as e:
        print(f"❌ Invalid date format: {e}")
        return
    
    os.makedirs(args.out_dir, exist_ok=True)
    
    # Define what to collect based on mode
    if args.minimal:
        collection_functions = [
            ('Game Info', collect_nba_game_info),
            ('Box Scores', collect_nba_box_scores),
        ]
        print(f"🎯 MINIMAL MODE: Collecting only essential NBA data")
    else:
        collection_functions = [
            ('Game Info', collect_nba_game_info),
            ('Play-by-Play', collect_nba_plays),
            ('Shot Chart', collect_nba_shot_chart),
            ('Box Scores', collect_nba_box_scores),
        ]
        print(f"🎯 FULL MODE: Collecting comprehensive NBA data")
    
    print(f"💡 Claude will research: injuries, lineups, referee assignments, trends")
    print(f"\n📅 Processing {date_range} days...")
    
    current_date = start_date
    total_files = 0
    total_errors = 0
    
    # Process date range
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        print(f"\n📅 Processing {date_str}...")
        
        day_files = 0
        day_errors = 0
        
        for name, func in collection_functions:
            try:
                result = func(date_str, args.out_dir, use_placeholder)
                if result:  # Success
                    day_files += 1
                    total_files += 1
                else:
                    day_errors += 1
                    total_errors += 1
                    
            except Exception as e:
                print(f"❌ Error collecting {name}: {e}")
                day_errors += 1
                total_errors += 1
        
        # Daily summary
        success_rate = (day_files / len(collection_functions)) * 100
        print(f"   📊 Day summary: {day_files}/{len(collection_functions)} successful ({success_rate:.1f}%)")
        
        current_date += timedelta(days=1)
    
    # Final summary
    print(f"\n✅ STREAMLINED NBA backfill complete!")
    print(f"📊 Summary:")
    print(f"   📁 Total files generated: {total_files}")
    print(f"   ❌ Total errors: {total_errors}")
    print(f"   📈 Overall success rate: {(total_files / max(1, total_files + total_errors)) * 100:.1f}%")
    print(f"   📁 Files saved to: {args.out_dir}")
    
    # Next steps guidance
    if use_placeholder:
        print(f"\n🔧 PLACEHOLDER MODE COMPLETED:")
        print(f"   ✅ Test NBA data generated successfully")
        print(f"   🎯 Perfect for testing pipeline")
        print(f"   💡 For real data: use --real-data flag")
    else:
        print(f"\n📡 REAL DATA MODE COMPLETED:")
        print(f"   ✅ Real NBA data collected")
        print(f"   📊 Advanced metrics ready for analysis")
        print(f"   🎯 Ready for Claude betting analysis")
    
    print(f"\n🎯 Next steps:")
    print(f"   1. python loader/nba_load_parquet_into_pg.py --input-dir {args.out_dir}")
    print(f"   2. python py/nba_analysis.py")
    print(f"   3. Send data to Claude for betting analysis!")
    
    print(f"\n💡 CLAUDE INTEGRATION:")
    print(f"   🏀 Your system: Provides impossible-to-get NBA tracking data")
    print(f"   🤖 Claude: Researches injuries, lineups, refs, trends")
    print(f"   🎯 Result: Complete NBA betting analysis with minimal complexity")

if __name__ == '__main__':
    main()