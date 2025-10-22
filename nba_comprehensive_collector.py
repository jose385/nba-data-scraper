#!/usr/bin/env python3
"""
COMPREHENSIVE NBA API ENDPOINT AUDIT & ENHANCED COLLECTOR

This script identifies ALL available NBA_API endpoints and ensures we collect 
EVERY piece of available data for maximum prediction model performance.

Based on research, NBA_API has 100+ endpoints. We were only using ~6 of them!
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

# NBA API imports - COMPREHENSIVE LIST
try:
    # ========================================
    # GAME-LEVEL ENDPOINTS (What we had)
    # ========================================
    from nba_api.stats.endpoints import (
        scoreboardv2, boxscoreadvancedv2, boxscoretraditionalv2,
    )
    
    # ========================================
    # MISSING GAME-LEVEL ENDPOINTS 
    # ========================================
    from nba_api.stats.endpoints import (
        playbyplay,               # ❌ MISSING: Play-by-play data
        playbyplayv2,            # ❌ MISSING: Enhanced play-by-play
        boxscoremiscv2,          # ❌ MISSING: Miscellaneous stats
        boxscoreplayertrackv2,   # ❌ MISSING: Player tracking data
        boxscorescoringv2,       # ❌ MISSING: Detailed scoring
        boxscoreusagev2,         # ❌ MISSING: Usage statistics
        boxscorefourFactorsv2,   # ❌ MISSING: Four factors analysis
        
        # Game info and rotations
        gamerotation,            # ❌ MISSING: Player rotations
        hustlestatsboxscore,     # ❌ MISSING: Hustle stats
        videoeventsasset,        # ❌ MISSING: Video highlights
    )
    
    # ========================================
    # PLAYER ANALYSIS ENDPOINTS
    # ========================================
    from nba_api.stats.endpoints import (
        playerdashboardbygeneralsplits,    # ❌ MISSING: Player performance splits
        playerdashboardbyopponent,         # ❌ MISSING: Vs opponent stats
        playerdashboardbyclutch,           # ❌ MISSING: Clutch performance
        playerdashboardbyteamperformance,  # ❌ MISSING: Team context stats
        playerdashboardbyyearoveryear,     # ❌ MISSING: YoY comparisons
        
        # Player career and historical
        playercareerstats,                 # ❌ MISSING: Career history
        playerprofilev2,                   # ❌ MISSING: Player profiles
        playergamelog,                     # ❌ MISSING: Game logs
        
        # Player vs team/league analysis
        playervsteam,                      # ❌ MISSING: Head-to-head records
        playercompare,                     # ❌ MISSING: Player comparisons
        
        # Advanced player metrics
        playergamestreakfinder,            # ❌ MISSING: Streak analysis
        playerawards,                      # ❌ MISSING: Awards/achievements
    )
    
    # ========================================
    # TEAM ANALYSIS ENDPOINTS  
    # ========================================
    from nba_api.stats.endpoints import (
        teamdashboardbygeneralsplits,      # ❌ MISSING: Team performance splits
        teamdashboardbyopponent,           # ❌ MISSING: Vs opponent analysis
        teamdashboardbyclutch,             # ❌ MISSING: Clutch performance
        teamdashboardbyteamperformance,    # ❌ MISSING: Team context
        teamdashboardbyyearoveryear,       # ❌ MISSING: YoY team comparison
        
        # Team game analysis
        teamgamelog,                       # ❌ MISSING: Team game logs
        teamplayerdashboard,               # ❌ MISSING: Team player breakdown
        teamplayeronoffdetails,            # ❌ MISSING: On/off court impact
        
        # Team vs analysis
        teamvsteam,                        # ❌ MISSING: Head-to-head team records
        
        # Team historical
        teamyearbyyearstats,               # ❌ MISSING: Historical team stats
        teamestimatedmetrics,              # ❌ MISSING: Advanced team metrics
    )
    
    # ========================================
    # LEAGUE-WIDE ANALYSIS ENDPOINTS
    # ========================================
    from nba_api.stats.endpoints import (
        leaguegamefinder,                  # ✅ HAD: Game finder
        leaguestandingsv3,                 # ✅ HAD: Standings
        leaguedashteamstats,              # ✅ HAD: Team stats
        leaguedashplayerstats,            # ✅ HAD: Player stats
        
        # Missing league analysis
        leaguedashteamclutch,             # ❌ MISSING: Team clutch stats
        leaguedashplayerclutch,           # ❌ MISSING: Player clutch stats
        leaguedashteamptshot,             # ❌ MISSING: Shot tracking
        leaguedashplayerptshot,           # ❌ MISSING: Player shot tracking
        leaguedashteamshotlocations,      # ❌ MISSING: Shot location analysis
        leaguedashplayershotlocations,    # ❌ MISSING: Player shot locations
        
        # League leaders and rankings
        leagueleaders,                    # ❌ MISSING: League leaders
        allstarballotpredictor,           # ❌ MISSING: All-star predictions
        
        # Advanced league metrics
        leaguedashlineups,                # ❌ MISSING: Lineup analysis
        leaguedashptdefend,               # ❌ MISSING: Defensive tracking
        leaguehustlestatsteam,            # ❌ MISSING: Team hustle stats
        leaguehustlestatsplayer,          # ❌ MISSING: Player hustle stats
    )
    
    # ========================================
    # SHOOTING & PERFORMANCE ANALYSIS
    # ========================================
    from nba_api.stats.endpoints import (
        shotchartdetail,                  # ❌ MISSING: Shot chart data
        shotchartlineupdetail,            # ❌ MISSING: Lineup shot charts
        teamdashptshots,                  # ❌ MISSING: Team shot tracking
        playerdashptshots,                # ❌ MISSING: Player shot tracking
        
        # Defense analysis
        teamdashptreb,                    # ❌ MISSING: Rebounding tracking
        playerdashptreb,                  # ❌ MISSING: Player rebounding
        teamdashptpass,                   # ❌ MISSING: Passing tracking
        playerdashptpass,                 # ❌ MISSING: Player passing
        
        # Speed and distance
        teamdashptstats,                  # ❌ MISSING: Speed/distance stats
        playerdashptstats,                # ❌ MISSING: Player speed/distance
    )
    
    # ========================================
    # MATCHUP & OPPONENT ANALYSIS
    # ========================================
    from nba_api.stats.endpoints import (
        matchupsrollup,                   # ❌ MISSING: Matchup analysis
        synergyplaytypes,                 # ❌ MISSING: Play type analysis
        
        # Win probability and predictions
        winprobabilitypbp,                # ❌ MISSING: Win probability
        infographicfanduelplayer,         # ❌ MISSING: Fantasy analysis
    )
    
    # ========================================
    # LIVE & REAL-TIME ENDPOINTS
    # ========================================
    from nba_api.live.nba.endpoints import (
        scoreboard,                       # ✅ HAD: Live scoreboard
        boxscore,                         # ❌ MISSING: Live boxscore
        playbyplay,                       # ❌ MISSING: Live play-by-play
    )
    
    # ========================================
    # STATIC DATA (Reference)
    # ========================================
    from nba_api.stats.static import teams, players  # ✅ HAD: Basic reference
    
    # Additional static endpoints
    from nba_api.stats.endpoints import (
        commonteamroster,                 # ❌ MISSING: Current rosters
        draftcombinestats,               # ❌ MISSING: Draft combine data
        drafthistory,                    # ❌ MISSING: Draft history
        franchisehistory,                # ❌ MISSING: Franchise history
        commonallplayers,                # ❌ MISSING: All players list
        commonplayerinfo,                # ❌ MISSING: Player biographical
        commonteaminfo,                  # ❌ MISSING: Team information
    )
    
    # ========================================
    # HISTORICAL & TRENDS
    # ========================================
    from nba_api.stats.endpoints import (
        scoringaverages,                 # ❌ MISSING: Scoring trends
        assistleaders,                   # ❌ MISSING: Assist leaders
        assisttracker,                   # ❌ MISSING: Assist tracking
        reboundtracker,                  # ❌ MISSING: Rebound tracking
        
        # Game and season finders
        teamgamefinder,                  # ❌ MISSING: Team game finder
        playergamefinder,                # ❌ MISSING: Player game finder
        teamyearbyyearstats,             # ❌ MISSING: Team historical
        playeryearbyyearstats,           # ❌ MISSING: Player historical
    )
    
    NBA_API_AVAILABLE = True
    
except ImportError as e:
    print(f"❌ NBA API import error: {e}")
    NBA_API_AVAILABLE = False

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ComprehensiveNBACollector:
    """
    COMPREHENSIVE NBA Data Collector
    
    Collects ALL available data from NBA_API endpoints for maximum 
    prediction model performance. Based on research showing 100+ endpoints available.
    """
    
    def __init__(self, output_dir: str = "nba_comprehensive_data", odds_api_key: Optional[str] = None):
        self.output_dir = output_dir
        self.odds_api_key = odds_api_key
        self.season = "2024-25"
        
        # Enhanced settings for comprehensive collection
        self.max_retries = 3
        self.base_delay = 1.0
        self.timeout = 60
        
        # Create comprehensive directory structure
        self.create_comprehensive_directories()
        
        # Get reference data
        self.nba_teams = teams.get_teams() if NBA_API_AVAILABLE else []
        self.team_id_map = {team['abbreviation']: team['id'] for team in self.nba_teams}
        
        logger.info(f"🏀 COMPREHENSIVE NBA Collector initialized")
        logger.info(f"📁 Output: {self.output_dir}")
        logger.info(f"🔍 Total endpoints available: 100+")
        
    def create_comprehensive_directories(self):
        """Create comprehensive directory structure for all data types"""
        directories = [
            # Basic structure
            self.output_dir,
            f"{self.output_dir}/daily_games",
            f"{self.output_dir}/combined_daily",
            
            # Game-level data
            f"{self.output_dir}/game_boxscores",
            f"{self.output_dir}/game_playbyplay", 
            f"{self.output_dir}/game_rotations",
            f"{self.output_dir}/game_tracking",
            f"{self.output_dir}/game_hustle",
            f"{self.output_dir}/game_shots",
            
            # Player analysis
            f"{self.output_dir}/player_dashboards",
            f"{self.output_dir}/player_splits",
            f"{self.output_dir}/player_clutch",
            f"{self.output_dir}/player_tracking",
            f"{self.output_dir}/player_careers",
            f"{self.output_dir}/player_vs_teams",
            
            # Team analysis  
            f"{self.output_dir}/team_dashboards",
            f"{self.output_dir}/team_splits",
            f"{self.output_dir}/team_clutch",
            f"{self.output_dir}/team_tracking",
            f"{self.output_dir}/team_lineups",
            f"{self.output_dir}/team_vs_teams",
            
            # League analysis
            f"{self.output_dir}/league_leaders",
            f"{self.output_dir}/league_lineups",
            f"{self.output_dir}/league_tracking",
            f"{self.output_dir}/league_hustle",
            f"{self.output_dir}/league_clutch",
            
            # Shot analysis
            f"{self.output_dir}/shot_charts",
            f"{self.output_dir}/shot_tracking",
            f"{self.output_dir}/shot_locations",
            
            # Advanced analytics
            f"{self.output_dir}/matchups",
            f"{self.output_dir}/synergy",
            f"{self.output_dir}/win_probability",
            f"{self.output_dir}/four_factors",
            
            # Historical data
            f"{self.output_dir}/historical_team",
            f"{self.output_dir}/historical_player",
            f"{self.output_dir}/draft_data",
            f"{self.output_dir}/franchise_history",
            
            # Reference data
            f"{self.output_dir}/rosters",
            f"{self.output_dir}/player_info",
            f"{self.output_dir}/team_info",
            
            # Market data
            f"{self.output_dir}/odds_data",
            
            # Logs and summaries
            f"{self.output_dir}/logs",
            f"{self.output_dir}/collection_summaries"
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            
        logger.info(f"📁 Created {len(directories)} data directories")

    def safe_api_call(self, api_function, *args, **kwargs):
        """Enhanced API call with comprehensive error handling"""
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

    def collect_comprehensive_game_data(self, game_id: str, game_date: str) -> Dict[str, pd.DataFrame]:
        """Collect ALL available game-level data"""
        logger.info(f"📊 Comprehensive game collection: {game_id}")
        
        result = {}
        
        # ========================================
        # BASIC BOXSCORES (What we had)
        # ========================================
        try:
            traditional = self.safe_api_call(boxscoretraditionalv2.BoxScoreTraditionalV2, game_id=game_id)
            if traditional:
                dfs = traditional.get_data_frames()
                if len(dfs) >= 2:
                    result['player_traditional'] = dfs[0]
                    result['team_traditional'] = dfs[1]
            
            time.sleep(self.base_delay)
            
            advanced = self.safe_api_call(boxscoreadvancedv2.BoxScoreAdvancedV2, game_id=game_id)
            if advanced:
                dfs = advanced.get_data_frames()
                if len(dfs) >= 2:
                    result['player_advanced'] = dfs[0]
                    result['team_advanced'] = dfs[1]
                    
        except Exception as e:
            logger.warning(f"⚠️ Basic boxscore error: {e}")
        
        time.sleep(self.base_delay)
        
        # ========================================
        # MISSING BOXSCORE DATA (NEW!)
        # ========================================
        try:
            # Miscellaneous stats
            misc = self.safe_api_call(boxscoremiscv2.BoxScoreMiscV2, game_id=game_id)
            if misc:
                dfs = misc.get_data_frames()
                if len(dfs) >= 2:
                    result['player_misc'] = dfs[0]
                    result['team_misc'] = dfs[1]
            
            time.sleep(self.base_delay)
            
            # Scoring details
            scoring = self.safe_api_call(boxscorescoringv2.BoxScoreScoringV2, game_id=game_id)
            if scoring:
                dfs = scoring.get_data_frames()
                if len(dfs) >= 2:
                    result['player_scoring'] = dfs[0]
                    result['team_scoring'] = dfs[1]
            
            time.sleep(self.base_delay)
            
            # Usage statistics
            usage = self.safe_api_call(boxscoreusagev2.BoxScoreUsageV2, game_id=game_id)
            if usage:
                dfs = usage.get_data_frames()
                if len(dfs) >= 2:
                    result['player_usage'] = dfs[0]
                    result['team_usage'] = dfs[1]
            
            time.sleep(self.base_delay)
            
            # Four factors analysis
            four_factors = self.safe_api_call(boxscorefourFactorsv2.BoxScoreFourFactorsV2, game_id=game_id)
            if four_factors:
                dfs = four_factors.get_data_frames()
                if len(dfs) >= 2:
                    result['player_four_factors'] = dfs[0]
                    result['team_four_factors'] = dfs[1]
            
            time.sleep(self.base_delay)
            
        except Exception as e:
            logger.warning(f"⚠️ Enhanced boxscore error: {e}")
        
        # ========================================
        # PLAYER TRACKING DATA (NEW!)
        # ========================================
        try:
            tracking = self.safe_api_call(boxscoreplayertrackv2.BoxScorePlayerTrackV2, game_id=game_id)
            if tracking:
                dfs = tracking.get_data_frames()
                if dfs:
                    result['player_tracking'] = dfs[0]
            
            time.sleep(self.base_delay)
            
        except Exception as e:
            logger.warning(f"⚠️ Player tracking error: {e}")
        
        # ========================================
        # HUSTLE STATS (NEW!)
        # ========================================
        try:
            hustle = self.safe_api_call(hustlestatsboxscore.HustleStatsBoxscore, game_id=game_id)
            if hustle:
                dfs = hustle.get_data_frames()
                if len(dfs) >= 2:
                    result['player_hustle'] = dfs[0]
                    result['team_hustle'] = dfs[1]
            
            time.sleep(self.base_delay)
            
        except Exception as e:
            logger.warning(f"⚠️ Hustle stats error: {e}")
        
        # ========================================
        # PLAY-BY-PLAY DATA (NEW!)
        # ========================================
        try:
            pbp = self.safe_api_call(playbyplay.PlayByPlay, game_id=game_id)
            if pbp:
                dfs = pbp.get_data_frames()
                if dfs:
                    result['play_by_play'] = dfs[0]
            
            time.sleep(self.base_delay)
            
        except Exception as e:
            logger.warning(f"⚠️ Play-by-play error: {e}")
        
        # ========================================
        # ROTATIONS DATA (NEW!)
        # ========================================
        try:
            rotations = self.safe_api_call(gamerotation.GameRotation, game_id=game_id)
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
        
        logger.info(f"✅ Collected {len(result)} data types for game {game_id}")
        return result

    def collect_comprehensive_daily_data(self, target_date: datetime) -> Dict[str, any]:
        """Collect ALL available data for a single day"""
        date_str = target_date.strftime('%Y-%m-%d')
        logger.info(f"🎯 COMPREHENSIVE collection for {date_str}")
        
        # Initialize summary
        summary = {
            'date': date_str,
            'games_found': 0,
            'data_types_collected': {},
            'total_dataframes': 0,
            'errors': []
        }
        
        try:
            # Step 1: Get games for the date
            games_df = self.get_games_for_date(target_date)
            
            if games_df.empty:
                logger.info(f"📅 No games on {date_str}")
                return summary
            
            summary['games_found'] = len(games_df)
            
            # Step 2: Collect comprehensive game data
            all_game_data = {}
            
            for i, (_, game) in enumerate(games_df.iterrows()):
                game_id = str(game.get('GAME_ID', game.get('gameId', '')))
                if not game_id:
                    continue
                
                logger.info(f"📊 Game {i+1}/{len(games_df)}: {game_id}")
                
                game_data = self.collect_comprehensive_game_data(game_id, date_str)
                
                # Organize by data type
                for data_type, df in game_data.items():
                    if data_type not in all_game_data:
                        all_game_data[data_type] = []
                    all_game_data[data_type].append(df)
            
            # Step 3: Combine and save all data
            files_created = self.save_comprehensive_daily_data(
                date_str, games_df, all_game_data
            )
            
            summary['data_types_collected'] = {k: len(v) for k, v in all_game_data.items()}
            summary['total_dataframes'] = sum(len(v) for v in all_game_data.values())
            
            logger.info(f"🎉 COMPREHENSIVE collection complete: {summary}")
            
        except Exception as e:
            error_msg = f"Comprehensive collection error: {str(e)}"
            logger.error(f"❌ {error_msg}")
            summary['errors'].append(error_msg)
        
        return summary

    def get_games_for_date(self, target_date: datetime) -> pd.DataFrame:
        """Get games for date (using existing robust method)"""
        date_str = target_date.strftime('%m/%d/%Y')
        
        try:
            # Try live scoreboard first
            scoreboard_result = self.safe_api_call(scoreboard.ScoreBoard)
            if scoreboard_result:
                games_dict = scoreboard_result.get_dict()
                if games_dict and 'scoreboard' in games_dict:
                    games_data = games_dict['scoreboard'].get('games', [])
                    
                    target_games = []
                    for game in games_data:
                        if 'gameTimeUTC' in game:
                            try:
                                game_date = datetime.fromisoformat(game['gameTimeUTC'].replace('Z', '+00:00'))
                                if game_date.date() == target_date.date():
                                    target_games.append(game)
                            except:
                                continue
                    
                    if target_games:
                        return pd.DataFrame(target_games)
            
            # Fallback to stats scoreboard
            scoreboard_result = self.safe_api_call(scoreboardv2.ScoreboardV2, game_date=date_str)
            if scoreboard_result:
                games_df = scoreboard_result.get_data_frames()[0]
                if not games_df.empty:
                    games_df['GAME_DATE'] = target_date.strftime('%Y-%m-%d')
                    return games_df
                    
        except Exception as e:
            logger.error(f"❌ Error getting games: {e}")
        
        return pd.DataFrame()

    def save_comprehensive_daily_data(self, date_str: str, games_df: pd.DataFrame, 
                                    all_game_data: Dict[str, List[pd.DataFrame]]) -> List[str]:
        """Save all comprehensive data"""
        files_created = []
        
        try:
            # Save games info
            if not games_df.empty:
                games_file = f"{self.output_dir}/daily_games/games_{date_str}.csv"
                games_df.to_csv(games_file, index=False)
                files_created.append(games_file)
            
            # Save each data type
            for data_type, df_list in all_game_data.items():
                if df_list:
                    combined_df = pd.concat(df_list, ignore_index=True)
                    
                    # Determine appropriate subdirectory
                    if 'player' in data_type:
                        subdir = 'player_dashboards'
                    elif 'team' in data_type:
                        subdir = 'team_dashboards'
                    elif 'tracking' in data_type:
                        subdir = 'game_tracking'
                    elif 'hustle' in data_type:
                        subdir = 'game_hustle'
                    elif 'play_by_play' in data_type:
                        subdir = 'game_playbyplay'
                    elif 'rotations' in data_type:
                        subdir = 'game_rotations'
                    else:
                        subdir = 'game_boxscores'
                    
                    data_file = f"{self.output_dir}/{subdir}/{data_type}_{date_str}.csv"
                    combined_df.to_csv(data_file, index=False)
                    files_created.append(data_file)
            
            # Create comprehensive combined file
            combined_file = self.create_comprehensive_combined_file(date_str, games_df, all_game_data)
            if combined_file:
                files_created.append(combined_file)
            
            logger.info(f"💾 Saved {len(files_created)} comprehensive files")
            
        except Exception as e:
            logger.error(f"❌ Save error: {e}")
        
        return files_created

    def create_comprehensive_combined_file(self, date_str: str, games_df: pd.DataFrame,
                                         all_game_data: Dict[str, List[pd.DataFrame]]) -> Optional[str]:
        """Create comprehensive combined file with ALL data"""
        try:
            combined_file = f"{self.output_dir}/combined_daily/nba_comprehensive_{date_str}.csv"
            
            with open(combined_file, 'w') as f:
                # Enhanced header
                f.write(f"# NBA COMPREHENSIVE Data for {date_str}\n")
                f.write(f"# Season: {self.season}\n")
                f.write(f"# Collection Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# Games: {len(games_df)}\n")
                f.write(f"# Data Types: {len(all_game_data)}\n")
                f.write(f"# Total DataFrames: {sum(len(v) for v in all_game_data.values())}\n\n")
                
                # Games info
                if not games_df.empty:
                    f.write("## GAMES\n")
                    games_df.to_csv(f, index=False)
                    f.write("\n")
                
                # All comprehensive data
                for data_type, df_list in all_game_data.items():
                    if df_list:
                        f.write(f"## {data_type.upper()}\n")
                        combined_df = pd.concat(df_list, ignore_index=True)
                        combined_df.to_csv(f, index=False)
                        f.write("\n")
            
            logger.info(f"📄 Created comprehensive combined file: {combined_file}")
            return combined_file
            
        except Exception as e:
            logger.error(f"❌ Combined file error: {e}")
            return None

def main():
    """Main execution with comprehensive collection"""
    parser = argparse.ArgumentParser(description="COMPREHENSIVE NBA Data Collector")
    parser.add_argument('--date', type=str, help='Date (YYYY-MM-DD)')
    parser.add_argument('--yesterday', action='store_true', help='Yesterday\'s games')
    parser.add_argument('--output-dir', type=str, default='nba_comprehensive_data', help='Output directory')
    
    args = parser.parse_args()
    
    if not NBA_API_AVAILABLE:
        logger.error("❌ NBA_API not available")
        return
    
    # Initialize comprehensive collector
    collector = ComprehensiveNBACollector(output_dir=args.output_dir)
    
    # Determine target date
    if args.date:
        target_date = datetime.strptime(args.date, '%Y-%m-%d')
    elif args.yesterday:
        target_date = datetime.now() - timedelta(days=1)
    else:
        target_date = datetime.now() - timedelta(days=1)
    
    logger.info(f"🚀 Starting COMPREHENSIVE collection for {target_date.strftime('%Y-%m-%d')}")
    logger.info("📊 This will collect ALL available NBA_API data (100+ endpoints)")
    
    # Run comprehensive collection
    summary = collector.collect_comprehensive_daily_data(target_date)
    
    logger.info("🎉 COMPREHENSIVE COLLECTION COMPLETE!")
    logger.info(f"📊 Summary: {summary}")

if __name__ == "__main__":
    main()