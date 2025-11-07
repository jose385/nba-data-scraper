#!/usr/bin/env python3
"""
CORRECTED NBA Enhanced Collector - Only Using Verified Endpoints

This version only imports and uses NBA_API endpoints that actually exist
in the current version, avoiding import errors.
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import time
import requests
import json
from typing import Dict, List, Optional, Tuple
import logging
import argparse

# Test which endpoints are actually available
def test_nba_api_endpoints():
    """Test which NBA_API endpoints are actually available"""
    available_endpoints = {}
    
    # Test basic endpoints we know work
    try:
        from nba_api.stats.endpoints import scoreboardv2
        available_endpoints['scoreboardv2'] = scoreboardv2
    except ImportError:
        pass
    
    try:
        from nba_api.stats.endpoints import boxscoretraditionalv2
        available_endpoints['boxscoretraditionalv2'] = boxscoretraditionalv2
    except ImportError:
        pass
        
    try:
        from nba_api.stats.endpoints import boxscoreadvancedv2
        available_endpoints['boxscoreadvancedv2'] = boxscoreadvancedv2
    except ImportError:
        pass
    
    # Test additional endpoints one by one
    endpoint_tests = [
        'boxscoremiscv2',
        'boxscoreusagev2', 
        'boxscorescoringv2',
        'boxscoreplayertrackv2',
        'playbyplay',
        'gamerotation',
        'hustlestatsboxscore',
        'leaguedashteamstats',
        'leaguedashplayerstats',
        'leaguestandingsv3',
        'teamdashboardbygeneralsplits',
        'playerdashboardbygeneralsplits'
    ]
    
    for endpoint_name in endpoint_tests:
        try:
            module = __import__('nba_api.stats.endpoints', fromlist=[endpoint_name])
            endpoint_class = getattr(module, endpoint_name)
            available_endpoints[endpoint_name] = endpoint_class
            print(f"✅ {endpoint_name} - Available")
        except (ImportError, AttributeError):
            print(f"❌ {endpoint_name} - Not available")
    
    # Test static imports
    try:
        from nba_api.stats.static import teams, players
        available_endpoints['teams'] = teams
        available_endpoints['players'] = players
    except ImportError:
        pass
    
    # Test live endpoints
    try:
        from nba_api.live.nba.endpoints import scoreboard
        available_endpoints['live_scoreboard'] = scoreboard
    except ImportError:
        pass
    
    return available_endpoints

# Initialize available endpoints
print("🔍 Testing NBA_API endpoints...")
AVAILABLE_ENDPOINTS = test_nba_api_endpoints()
print(f"✅ Found {len(AVAILABLE_ENDPOINTS)} available endpoints")

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CorrectedNBACollector:
    """
    Corrected NBA Data Collector - Only uses verified existing endpoints
    """
    
    def __init__(self, output_dir: str = "nba_enhanced_data", odds_api_key: Optional[str] = None):
        self.output_dir = output_dir
        self.odds_api_key = odds_api_key
        self.season = "2024-25"
        
        # Enhanced settings
        self.max_retries = 3
        self.base_delay = 0.6
        self.timeout = 30
        
        # Create directory structure
        self.create_directories()
        
        # Get reference data if available
        if 'teams' in AVAILABLE_ENDPOINTS:
            self.nba_teams = AVAILABLE_ENDPOINTS['teams'].get_teams()
            self.team_id_map = {team['abbreviation']: team['id'] for team in self.nba_teams}
        else:
            self.nba_teams = []
            self.team_id_map = {}
        
        logger.info(f"🏀 Corrected NBA Collector initialized")
        logger.info(f"📁 Output: {self.output_dir}")
        logger.info(f"🔍 Available endpoints: {len(AVAILABLE_ENDPOINTS)}")
        
    def create_directories(self):
        """Create directory structure for enhanced data"""
        directories = [
            self.output_dir,
            f"{self.output_dir}/daily_games",
            f"{self.output_dir}/combined_daily",
            f"{self.output_dir}/enhanced_stats",
            f"{self.output_dir}/odds_data",
            f"{self.output_dir}/logs"
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)

    def safe_api_call(self, api_function, *args, **kwargs):
        """Enhanced API call with error handling"""
        for attempt in range(self.max_retries):
            try:
                result = api_function(*args, **kwargs)
                if hasattr(result, 'get_data_frames'):
                    data_frames = result.get_data_frames()
                    if data_frames and len(data_frames) > 0:
                        return result
                return result
            except Exception as e:
                logger.warning(f"⚠️ API call failed (attempt {attempt + 1}): {str(e)[:100]}...")
                if attempt < self.max_retries - 1:
                    time.sleep(self.base_delay * (2 ** attempt))
        return None

    def collect_enhanced_game_data(self, game_id: str, game_date: str) -> Dict[str, pd.DataFrame]:
        """Collect enhanced game data using only available endpoints"""
        logger.info(f"📊 Enhanced collection for game: {game_id}")
        
        result = {}
        
        # ========================================
        # BASIC BOXSCORES (Core data)
        # ========================================
        if 'boxscoretraditionalv2' in AVAILABLE_ENDPOINTS:
            try:
                traditional = self.safe_api_call(
                    AVAILABLE_ENDPOINTS['boxscoretraditionalv2'].BoxScoreTraditionalV2, 
                    game_id=game_id
                )
                if traditional:
                    dfs = traditional.get_data_frames()
                    if len(dfs) >= 2:
                        result['player_traditional'] = dfs[0]
                        result['team_traditional'] = dfs[1]
                
                time.sleep(self.base_delay)
                
            except Exception as e:
                logger.warning(f"⚠️ Traditional boxscore error: {e}")
        
        if 'boxscoreadvancedv2' in AVAILABLE_ENDPOINTS:
            try:
                advanced = self.safe_api_call(
                    AVAILABLE_ENDPOINTS['boxscoreadvancedv2'].BoxScoreAdvancedV2, 
                    game_id=game_id
                )
                if advanced:
                    dfs = advanced.get_data_frames()
                    if len(dfs) >= 2:
                        result['player_advanced'] = dfs[0]
                        result['team_advanced'] = dfs[1]
                
                time.sleep(self.base_delay)
                
            except Exception as e:
                logger.warning(f"⚠️ Advanced boxscore error: {e}")
        
        # ========================================
        # ENHANCED BOXSCORES (If available)
        # ========================================
        if 'boxscoremiscv2' in AVAILABLE_ENDPOINTS:
            try:
                misc = self.safe_api_call(
                    AVAILABLE_ENDPOINTS['boxscoremiscv2'].BoxScoreMiscV2, 
                    game_id=game_id
                )
                if misc:
                    dfs = misc.get_data_frames()
                    if len(dfs) >= 2:
                        result['player_misc'] = dfs[0]
                        result['team_misc'] = dfs[1]
                
                time.sleep(self.base_delay)
                
            except Exception as e:
                logger.warning(f"⚠️ Misc boxscore error: {e}")
        
        if 'boxscoreusagev2' in AVAILABLE_ENDPOINTS:
            try:
                usage = self.safe_api_call(
                    AVAILABLE_ENDPOINTS['boxscoreusagev2'].BoxScoreUsageV2, 
                    game_id=game_id
                )
                if usage:
                    dfs = usage.get_data_frames()
                    if len(dfs) >= 2:
                        result['player_usage'] = dfs[0]
                        result['team_usage'] = dfs[1]
                
                time.sleep(self.base_delay)
                
            except Exception as e:
                logger.warning(f"⚠️ Usage stats error: {e}")
        
        if 'boxscorescoringv2' in AVAILABLE_ENDPOINTS:
            try:
                scoring = self.safe_api_call(
                    AVAILABLE_ENDPOINTS['boxscorescoringv2'].BoxScoreScoringV2, 
                    game_id=game_id
                )
                if scoring:
                    dfs = scoring.get_data_frames()
                    if len(dfs) >= 2:
                        result['player_scoring'] = dfs[0]
                        result['team_scoring'] = dfs[1]
                
                time.sleep(self.base_delay)
                
            except Exception as e:
                logger.warning(f"⚠️ Scoring stats error: {e}")
        
        # ========================================
        # TRACKING DATA (If available)
        # ========================================
        if 'boxscoreplayertrackv2' in AVAILABLE_ENDPOINTS:
            try:
                tracking = self.safe_api_call(
                    AVAILABLE_ENDPOINTS['boxscoreplayertrackv2'].BoxScorePlayerTrackV2, 
                    game_id=game_id
                )
                if tracking:
                    dfs = tracking.get_data_frames()
                    if dfs:
                        result['player_tracking'] = dfs[0]
                
                time.sleep(self.base_delay)
                
            except Exception as e:
                logger.warning(f"⚠️ Player tracking error: {e}")
        
        # ========================================
        # HUSTLE STATS (If available)
        # ========================================
        if 'hustlestatsboxscore' in AVAILABLE_ENDPOINTS:
            try:
                hustle = self.safe_api_call(
                    AVAILABLE_ENDPOINTS['hustlestatsboxscore'].HustleStatsBoxscore, 
                    game_id=game_id
                )
                if hustle:
                    dfs = hustle.get_data_frames()
                    if len(dfs) >= 2:
                        result['player_hustle'] = dfs[0]
                        result['team_hustle'] = dfs[1]
                
                time.sleep(self.base_delay)
                
            except Exception as e:
                logger.warning(f"⚠️ Hustle stats error: {e}")
        
        # ========================================
        # PLAY-BY-PLAY (If available)
        # ========================================
        if 'playbyplay' in AVAILABLE_ENDPOINTS:
            try:
                pbp = self.safe_api_call(
                    AVAILABLE_ENDPOINTS['playbyplay'].PlayByPlay, 
                    game_id=game_id
                )
                if pbp:
                    dfs = pbp.get_data_frames()
                    if dfs:
                        result['play_by_play'] = dfs[0]
                
                time.sleep(self.base_delay)
                
            except Exception as e:
                logger.warning(f"⚠️ Play-by-play error: {e}")
        
        # ========================================
        # ROTATIONS (If available)
        # ========================================
        if 'gamerotation' in AVAILABLE_ENDPOINTS:
            try:
                rotations = self.safe_api_call(
                    AVAILABLE_ENDPOINTS['gamerotation'].GameRotation, 
                    game_id=game_id
                )
                if rotations:
                    dfs = rotations.get_data_frames()
                    if dfs:
                        result['rotations'] = dfs[0]
                
                time.sleep(self.base_delay)
                
            except Exception as e:
                logger.warning(f"⚠️ Rotations error: {e}")
        
        # Add metadata to all dataframes
        for data_type, df in result.items():
            if not df.empty:
                df['GAME_ID'] = game_id
                df['GAME_DATE'] = game_date
                df['COLLECTION_TIME'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"✅ Collected {len(result)} enhanced data types for game {game_id}")
        return result

    def get_games_for_date(self, target_date: datetime) -> pd.DataFrame:
        """Get games for date using available endpoints"""
        date_str = target_date.strftime('%m/%d/%Y')
        
        try:
            # Try live scoreboard first if available
            if 'live_scoreboard' in AVAILABLE_ENDPOINTS:
                try:
                    scoreboard_result = self.safe_api_call(
                        AVAILABLE_ENDPOINTS['live_scoreboard'].ScoreBoard
                    )
                    if scoreboard_result:
                        games_dict = scoreboard_result.get_dict()
                        if games_dict and 'scoreboard' in games_dict:
                            games_data = games_dict['scoreboard'].get('games', [])
                            
                            target_games = []
                            for game in games_data:
                                if 'gameTimeUTC' in game:
                                    try:
                                        game_date = datetime.fromisoformat(
                                            game['gameTimeUTC'].replace('Z', '+00:00')
                                        )
                                        if game_date.date() == target_date.date():
                                            target_games.append(game)
                                    except:
                                        continue
                            
                            if target_games:
                                return pd.DataFrame(target_games)
                except Exception:
                    pass
            
            # Fallback to stats scoreboard
            if 'scoreboardv2' in AVAILABLE_ENDPOINTS:
                scoreboard_result = self.safe_api_call(
                    AVAILABLE_ENDPOINTS['scoreboardv2'].ScoreboardV2, 
                    game_date=date_str
                )
                if scoreboard_result:
                    games_df = scoreboard_result.get_data_frames()[0]
                    if not games_df.empty:
                        games_df['GAME_DATE'] = target_date.strftime('%Y-%m-%d')
                        return games_df
                        
        except Exception as e:
            logger.error(f"❌ Error getting games: {e}")
        
        return pd.DataFrame()

    def collect_enhanced_daily_data(self, target_date: datetime) -> Dict[str, any]:
        """Collect enhanced data for a single day"""
        date_str = target_date.strftime('%Y-%m-%d')
        logger.info(f"🎯 Enhanced collection for {date_str}")
        
        summary = {
            'date': date_str,
            'games_found': 0,
            'data_types_collected': {},
            'available_endpoints': list(AVAILABLE_ENDPOINTS.keys()),
            'errors': []
        }
        
        try:
            # Get games for the date
            games_df = self.get_games_for_date(target_date)
            
            if games_df.empty:
                logger.info(f"📅 No games on {date_str}")
                return summary
            
            summary['games_found'] = len(games_df)
            
            # Collect enhanced game data
            all_game_data = {}
            
            for i, (_, game) in enumerate(games_df.iterrows()):
                game_id = str(game.get('GAME_ID', game.get('gameId', '')))
                if not game_id:
                    continue
                
                logger.info(f"📊 Game {i+1}/{len(games_df)}: {game_id}")
                
                game_data = self.collect_enhanced_game_data(game_id, date_str)
                
                for data_type, df in game_data.items():
                    if data_type not in all_game_data:
                        all_game_data[data_type] = []
                    all_game_data[data_type].append(df)
            
            # Save enhanced data
            files_created = self.save_enhanced_daily_data(
                date_str, games_df, all_game_data
            )
            
            summary['data_types_collected'] = {k: len(v) for k, v in all_game_data.items()}
            
            logger.info(f"🎉 Enhanced collection complete: {summary}")
            
        except Exception as e:
            error_msg = f"Enhanced collection error: {str(e)}"
            logger.error(f"❌ {error_msg}")
            summary['errors'].append(error_msg)
        
        return summary

    def save_enhanced_daily_data(self, date_str: str, games_df: pd.DataFrame, 
                                all_game_data: Dict[str, List[pd.DataFrame]]) -> List[str]:
        """Save enhanced data"""
        files_created = []
        
        try:
            # Save games info
            if not games_df.empty:
                games_file = f"{self.output_dir}/daily_games/games_{date_str}.csv"
                games_df.to_csv(games_file, index=False)
                files_created.append(games_file)
            
            # Save each enhanced data type
            for data_type, df_list in all_game_data.items():
                if df_list:
                    combined_df = pd.concat(df_list, ignore_index=True)
                    data_file = f"{self.output_dir}/enhanced_stats/{data_type}_{date_str}.csv"
                    combined_df.to_csv(data_file, index=False)
                    files_created.append(data_file)
            
            # Create enhanced combined file
            combined_file = self.create_enhanced_combined_file(date_str, games_df, all_game_data)
            if combined_file:
                files_created.append(combined_file)
            
            logger.info(f"💾 Saved {len(files_created)} enhanced files")
            
        except Exception as e:
            logger.error(f"❌ Save error: {e}")
        
        return files_created

    def create_enhanced_combined_file(self, date_str: str, games_df: pd.DataFrame,
                                    all_game_data: Dict[str, List[pd.DataFrame]]) -> Optional[str]:
        """Create enhanced combined file with available data"""
        try:
            combined_file = f"{self.output_dir}/combined_daily/nba_enhanced_{date_str}.csv"
            
            with open(combined_file, 'w') as f:
                # Enhanced header
                f.write(f"# NBA Enhanced Data for {date_str}\n")
                f.write(f"# Season: {self.season}\n")
                f.write(f"# Collection Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# Games: {len(games_df)}\n")
                f.write(f"# Enhanced Data Types: {len(all_game_data)}\n")
                f.write(f"# Available Endpoints: {len(AVAILABLE_ENDPOINTS)}\n\n")
                
                # Games info
                if not games_df.empty:
                    f.write("## GAMES\n")
                    games_df.to_csv(f, index=False)
                    f.write("\n")
                
                # Enhanced data
                for data_type, df_list in all_game_data.items():
                    if df_list:
                        f.write(f"## {data_type.upper()}\n")
                        combined_df = pd.concat(df_list, ignore_index=True)
                        combined_df.to_csv(f, index=False)
                        f.write("\n")
            
            logger.info(f"📄 Created enhanced combined file: {combined_file}")
            return combined_file
            
        except Exception as e:
            logger.error(f"❌ Combined file error: {e}")
            return None

def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description="Corrected NBA Enhanced Collector")
    parser.add_argument('--date', type=str, help='Date (YYYY-MM-DD)')
    parser.add_argument('--yesterday', action='store_true', help='Yesterday\'s games')
    parser.add_argument('--output-dir', type=str, default='nba_enhanced_data', help='Output directory')
    
    args = parser.parse_args()
    
    if len(AVAILABLE_ENDPOINTS) == 0:
        logger.error("❌ No NBA_API endpoints available")
        return
    
    # Initialize collector
    collector = CorrectedNBACollector(output_dir=args.output_dir)
    
    # Determine target date
    if args.date:
        target_date = datetime.strptime(args.date, '%Y-%m-%d')
    elif args.yesterday:
        target_date = datetime.now() - timedelta(days=1)
    else:
        target_date = datetime.now() - timedelta(days=1)
    
    logger.info(f"🚀 Starting enhanced collection for {target_date.strftime('%Y-%m-%d')}")
    logger.info(f"📊 Using {len(AVAILABLE_ENDPOINTS)} available endpoints")
    
    # Run collection
    summary = collector.collect_enhanced_daily_data(target_date)
    
    logger.info("🎉 ENHANCED COLLECTION COMPLETE!")
    logger.info(f"📊 Summary: {summary}")

if __name__ == "__main__":
    main()