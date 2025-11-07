#!/usr/bin/env python3
"""
NBA 2024-25 Season Historical Backfill Collector

Systematically collects ALL NBA games from the 2024-25 season for model training.
Designed for bulk historical data collection with robust error handling and resume capability.

OPTIMAL TIMING: Based on testing, early morning EST (6-10 AM) provides best NBA API performance.
Avoid peak hours (evenings, game times) when API load is highest.

Key Features:
- Complete 2024-25 season coverage (Oct 2024 - Jun 2025)
- Same data structure as daily collector 
- Progress tracking and resume capability
- Enhanced timeout handling for NBA API reliability
- Bulk collection optimized with intelligent rate limiting
- Comprehensive logging for large-scale operations

Usage Examples:
    python nba_2024_25_backfill.py --full-season              # Complete season
    python nba_2024_25_backfill.py --start 2024-10-01 --end 2024-12-31  # Custom range  
    python nba_2024_25_backfill.py --start 2024-05-23 --end 2024-05-23  # Single day
    python nba_2024_25_backfill.py --resume                   # Resume interrupted collection
    python nba_2024_25_backfill.py --playoffs-only            # Just playoff games
    python nba_2024_25_backfill.py --month 2024-11            # Single month
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import time
import requests
import json
from typing import Dict, List, Optional, Tuple, Set
import logging
import argparse
from pathlib import Path
import pickle
from tqdm import tqdm

# NBA API imports
try:
    from nba_api.stats.endpoints import (
        scoreboardv2, boxscoreadvancedv2, boxscoretraditionalv2,
        leaguegamefinder, teamgamelog, playergamelog, 
        leaguestandingsv3, teamdashboardbygeneralsplits,
        playerdashboardbygeneralsplits, commonteamroster,
        leaguedashteamstats, leaguedashplayerstats
    )
    from nba_api.stats.static import teams, players
    from nba_api.live.nba.endpoints import scoreboard
    NBA_API_AVAILABLE = True
except ImportError:
    print("❌ nba_api not installed. Install with: pip install nba_api")
    NBA_API_AVAILABLE = False
    exit(1)

# Setup enhanced logging for bulk operations
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nba_2024_25_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NBA2024_25Backfill:
    """
    NBA 2024-25 Season Historical Data Collector
    
    Systematically collects ALL games from the 2024-25 season for comprehensive
    model training data. Optimized for bulk operations with progress tracking.
    """
    
    def __init__(self, output_dir: str = "nba_2024_25_data", odds_api_key: Optional[str] = None):
        """Initialize backfill collector"""
        self.output_dir = output_dir
        self.odds_api_key = odds_api_key
        self.season = "2024-25"
        
        # 2024-25 NBA Season Timeline
        self.season_start = datetime(2024, 10, 14)  # 2024-25 season start
        self.season_end = datetime(2025, 6, 30)     # End of potential playoffs
        self.all_star_break_start = datetime(2025, 2, 14)  # All-Star break
        self.all_star_break_end = datetime(2025, 2, 18)
        self.playoffs_start = datetime(2025, 4, 19)  # Estimated playoffs start
        
        # Enhanced settings for bulk collection (optimized for NBA API timeouts)
        self.max_retries = 7
        self.base_delay = 2.0   # Longer delays for bulk operations  
        self.bulk_delay = 1.5   # Between individual game collections
        self.day_delay = 3.0    # Between days during bulk collection
        self.error_delay = 10.0 # After errors (longer for timeouts)
        self.timeout = 60       # Increased timeout for NBA API calls
        
        # Progress tracking
        self.progress_file = f"{self.output_dir}/backfill_progress.pkl"
        self.summary_file = f"{self.output_dir}/backfill_summary.json"
        
        # Create directories
        self.create_directories()
        
        # Get NBA reference data
        self.nba_teams = teams.get_teams()
        self.team_id_map = {team['abbreviation']: team['id'] for team in self.nba_teams}
        
        logger.info(f"🏀 NBA 2024-25 Backfill initialized")
        logger.info(f"📅 Season: {self.season_start.strftime('%Y-%m-%d')} to {self.season_end.strftime('%Y-%m-%d')}")
        logger.info(f"📁 Output: {self.output_dir}")

    def create_directories(self):
        """Create comprehensive directory structure"""
        directories = [
            self.output_dir,
            f"{self.output_dir}/daily_games",
            f"{self.output_dir}/team_stats", 
            f"{self.output_dir}/player_stats",
            f"{self.output_dir}/odds_data",
            f"{self.output_dir}/combined_daily",
            f"{self.output_dir}/logs",
            f"{self.output_dir}/backfill_reports",
            f"{self.output_dir}/progress_checkpoints"
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)

    def load_progress(self) -> Dict:
        """Load previous progress to enable resume"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'rb') as f:
                    progress = pickle.load(f)
                logger.info(f"📋 Loaded previous progress: {len(progress.get('completed_dates', []))} dates completed")
                return progress
            except Exception as e:
                logger.warning(f"⚠️ Could not load progress file: {e}")
        
        return {
            'completed_dates': set(),
            'failed_dates': set(),
            'total_games': 0,
            'start_time': None,
            'last_update': None
        }

    def save_progress(self, progress: Dict):
        """Save current progress"""
        try:
            progress['last_update'] = datetime.now()
            with open(self.progress_file, 'wb') as f:
                pickle.dump(progress, f)
        except Exception as e:
            logger.error(f"❌ Could not save progress: {e}")

    def get_games_for_date(self, target_date: datetime) -> pd.DataFrame:
        """Get all NBA games for specific historical date with enhanced error handling"""
        date_str = target_date.strftime('%m/%d/%Y')
        
        for attempt in range(self.max_retries):
            try:
                # Use ScoreboardV2 for historical data with timeout handling
                scoreboard_data = scoreboardv2.ScoreboardV2(
                    game_date=date_str,
                    timeout=self.timeout
                )
                games_df = scoreboard_data.get_data_frames()[0]  # GameHeader
                
                if games_df.empty:
                    return pd.DataFrame()
                
                # Add metadata
                games_df['GAME_DATE'] = target_date.strftime('%Y-%m-%d')
                games_df['SEASON'] = self.season
                games_df['COLLECTION_TIMESTAMP'] = datetime.now()
                
                return games_df
                
            except Exception as e:
                error_msg = str(e)
                if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                    logger.warning(f"🕐 Connection timeout (attempt {attempt + 1}/{self.max_retries}) for {target_date.strftime('%Y-%m-%d')}")
                    if attempt < self.max_retries - 1:
                        timeout_delay = self.error_delay * (2 ** attempt)
                        logger.info(f"⏳ Waiting {timeout_delay}s before retry...")
                        time.sleep(timeout_delay)
                else:
                    logger.warning(f"⚠️ Attempt {attempt + 1} failed for {target_date.strftime('%Y-%m-%d')}: {str(e)[:100]}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.error_delay * (2 ** attempt))
                    else:
                        logger.error(f"❌ All attempts failed for {target_date.strftime('%Y-%m-%d')}")
        
        return pd.DataFrame()

    def get_detailed_game_data(self, game_id: str, game_date: str) -> Dict[str, pd.DataFrame]:
        """Enhanced game data collection with better error handling for bulk operations"""
        result = {}
        
        for attempt in range(self.max_retries):
            try:
                # Traditional box score with timeout
                traditional_box = boxscoretraditionalv2.BoxScoreTraditionalV2(
                    game_id=game_id,
                    timeout=self.timeout
                )
                traditional_data = traditional_box.get_data_frames()
                
                if len(traditional_data) >= 2:
                    player_stats = traditional_data[0].copy()
                    team_stats = traditional_data[1].copy()
                    
                    # Add comprehensive metadata
                    for df in [player_stats, team_stats]:
                        df['GAME_ID'] = game_id
                        df['GAME_DATE'] = game_date
                        df['SEASON'] = self.season
                        df['COLLECTION_TIMESTAMP'] = datetime.now()
                    
                    result['player_traditional'] = player_stats
                    result['team_traditional'] = team_stats
                
                time.sleep(self.bulk_delay)
                
                # Advanced box score with timeout
                advanced_box = boxscoreadvancedv2.BoxScoreAdvancedV2(
                    game_id=game_id,
                    timeout=self.timeout
                )
                advanced_data = advanced_box.get_data_frames()
                
                if len(advanced_data) >= 2:
                    player_advanced = advanced_data[0].copy()
                    team_advanced = advanced_data[1].copy()
                    
                    # Add metadata
                    for df in [player_advanced, team_advanced]:
                        df['GAME_ID'] = game_id
                        df['GAME_DATE'] = game_date
                        df['SEASON'] = self.season
                        df['COLLECTION_TIMESTAMP'] = datetime.now()
                    
                    result['player_advanced'] = player_advanced
                    result['team_advanced'] = team_advanced
                
                time.sleep(self.bulk_delay)
                break  # Success, exit retry loop
                
            except Exception as e:
                error_msg = str(e)
                if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                    logger.warning(f"🕐 Game {game_id} connection timeout (attempt {attempt + 1}/{self.max_retries})")
                    if attempt < self.max_retries - 1:
                        timeout_delay = self.error_delay * (2 ** attempt)
                        logger.info(f"⏳ Waiting {timeout_delay}s before retry...")
                        time.sleep(timeout_delay)
                else:
                    logger.warning(f"⚠️ Game data attempt {attempt + 1} failed for {game_id}: {str(e)[:100]}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.error_delay * (2 ** attempt))
                    else:
                        logger.error(f"❌ All attempts failed for game {game_id}")
        
        return result

    def get_historical_odds_for_date(self, target_date: datetime) -> pd.DataFrame:
        """Get historical betting odds (if available) - may be limited for older dates"""
        if not self.odds_api_key:
            return pd.DataFrame()
        
        # Note: Historical odds may not be available for older dates via The Odds API
        # This is kept for completeness but may return empty for most historical dates
        try:
            url = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"
            
            params = {
                'apiKey': self.odds_api_key,
                'regions': 'us',
                'markets': 'h2h,spreads,totals',
                'oddsFormat': 'american',
                'dateFormat': 'iso'
            }
            
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                odds_data = response.json()
                # Filter for target date games (likely empty for historical)
                # Implementation kept for future odds providers with historical data
                return pd.DataFrame()
            
        except Exception as e:
            logger.debug(f"Odds collection skipped for historical date {target_date.strftime('%Y-%m-%d')}: {e}")
        
        return pd.DataFrame()

    def collect_single_historical_day(self, target_date: datetime) -> Dict[str, any]:
        """Collect all data for a single historical day with enhanced tracking"""
        date_str = target_date.strftime('%Y-%m-%d')
        
        collection_summary = {
            'date': date_str,
            'games_found': 0,
            'detailed_games_collected': 0,
            'odds_collected': False,
            'files_created': [],
            'errors': [],
            'collection_time': datetime.now(),
            'api_calls_made': 0
        }
        
        try:
            # Check if already exists
            combined_file = f"{self.output_dir}/combined_daily/nba_complete_{date_str}.csv"
            if os.path.exists(combined_file):
                logger.debug(f"📄 Data already exists for {date_str}, skipping")
                return collection_summary
            
            # Get games for the date
            games_df = self.get_games_for_date(target_date)
            collection_summary['api_calls_made'] += 1
            
            if games_df.empty:
                return collection_summary
            
            collection_summary['games_found'] = len(games_df)
            logger.info(f"🎯 Found {len(games_df)} games for {date_str}")
            
            # Collect detailed data for each game
            all_detailed_data = {
                'player_traditional': [],
                'team_traditional': [],
                'player_advanced': [],
                'team_advanced': []
            }
            
            for i, (_, game) in enumerate(games_df.iterrows()):
                game_id = str(game['GAME_ID'])
                logger.debug(f"📊 Collecting game {i+1}/{len(games_df)}: {game_id}")
                
                detailed_data = self.get_detailed_game_data(game_id, date_str)
                collection_summary['api_calls_made'] += 2  # Traditional + Advanced
                
                if detailed_data:
                    collection_summary['detailed_games_collected'] += 1
                    
                    for data_type, df in detailed_data.items():
                        if not df.empty:
                            all_detailed_data[data_type].append(df)
                
                # Brief pause between games
                time.sleep(self.bulk_delay)
            
            # Get historical odds (likely empty for past dates)
            odds_df = self.get_historical_odds_for_date(target_date)
            
            # Save all data
            files_created = self.save_daily_data(
                date_str, games_df, all_detailed_data, odds_df
            )
            collection_summary['files_created'] = files_created
            
            logger.info(f"✅ Completed {date_str}: {collection_summary['detailed_games_collected']}/{collection_summary['games_found']} games")
            
        except Exception as e:
            error_msg = f"Error collecting {date_str}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            collection_summary['errors'].append(error_msg)
        
        return collection_summary

    def save_daily_data(self, date_str: str, games_df: pd.DataFrame, 
                       detailed_data: Dict[str, List[pd.DataFrame]], 
                       odds_df: pd.DataFrame) -> List[str]:
        """Save all collected data (same as daily collector)"""
        files_created = []
        
        try:
            # Save basic games info
            if not games_df.empty:
                games_file = f"{self.output_dir}/daily_games/games_{date_str}.csv"
                games_df.to_csv(games_file, index=False)
                files_created.append(games_file)
            
            # Save detailed stats
            for data_type, df_list in detailed_data.items():
                if df_list:
                    combined_df = pd.concat(df_list, ignore_index=True)
                    detail_file = f"{self.output_dir}/daily_games/{data_type}_{date_str}.csv"
                    combined_df.to_csv(detail_file, index=False)
                    files_created.append(detail_file)
            
            # Save odds data
            if not odds_df.empty:
                odds_file = f"{self.output_dir}/odds_data/odds_{date_str}.csv"
                odds_df.to_csv(odds_file, index=False)
                files_created.append(odds_file)
            
            # Create combined daily file
            combined_file = self.create_combined_daily_file(
                date_str, games_df, detailed_data, odds_df
            )
            if combined_file:
                files_created.append(combined_file)
            
        except Exception as e:
            logger.error(f"❌ Error saving data for {date_str}: {str(e)}")
        
        return files_created

    def create_combined_daily_file(self, date_str: str, games_df: pd.DataFrame,
                                 detailed_data: Dict[str, List[pd.DataFrame]], 
                                 odds_df: pd.DataFrame) -> Optional[str]:
        """Create comprehensive combined file (same structure as daily collector)"""
        try:
            combined_file = f"{self.output_dir}/combined_daily/nba_complete_{date_str}.csv"
            
            with open(combined_file, 'w') as f:
                # Enhanced header for historical data
                f.write(f"# NBA Complete Historical Data for {date_str}\n")
                f.write(f"# Season: {self.season}\n")
                f.write(f"# Collection Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# Collection Type: Historical Backfill\n")
                f.write(f"# Games: {len(games_df)}\n\n")
                
                # Games basic info
                if not games_df.empty:
                    f.write("## GAMES\n")
                    games_df.to_csv(f, index=False)
                    f.write("\n")
                
                # Detailed stats sections
                for data_type, df_list in detailed_data.items():
                    if df_list:
                        f.write(f"## {data_type.upper()}\n")
                        combined_df = pd.concat(df_list, ignore_index=True)
                        combined_df.to_csv(f, index=False)
                        f.write("\n")
                
                # Historical odds (likely empty)
                if not odds_df.empty:
                    f.write("## HISTORICAL_BETTING_ODDS\n")
                    odds_df.to_csv(f, index=False)
                    f.write("\n")
            
            return combined_file
            
        except Exception as e:
            logger.error(f"❌ Error creating combined file: {str(e)}")
            return None

    def get_all_season_dates(self, start_date: Optional[datetime] = None, 
                           end_date: Optional[datetime] = None) -> List[datetime]:
        """Get all dates in the 2024-25 season"""
        actual_start = start_date or self.season_start
        actual_end = end_date or min(datetime.now(), self.season_end)
        
        dates = []
        current = actual_start
        
        while current <= actual_end:
            dates.append(current)
            current += timedelta(days=1)
        
        logger.info(f"📅 Generated {len(dates)} dates from {actual_start.strftime('%Y-%m-%d')} to {actual_end.strftime('%Y-%m-%d')}")
        return dates

    def backfill_season(self, start_date: Optional[datetime] = None, 
                       end_date: Optional[datetime] = None,
                       resume: bool = False) -> Dict[str, any]:
        """Execute complete season backfill with progress tracking"""
        
        logger.info(f"🚀 Starting NBA 2024-25 Season Backfill")
        
        # Load progress if resuming
        progress = self.load_progress() if resume else {
            'completed_dates': set(),
            'failed_dates': set(),
            'total_games': 0,
            'start_time': datetime.now(),
            'last_update': None
        }
        
        if not progress['start_time']:
            progress['start_time'] = datetime.now()
        
        # Get all dates to process
        all_dates = self.get_all_season_dates(start_date, end_date)
        
        # Filter out already completed dates if resuming
        if resume:
            remaining_dates = [d for d in all_dates if d.strftime('%Y-%m-%d') not in progress['completed_dates']]
            logger.info(f"📋 Resuming: {len(remaining_dates)} dates remaining of {len(all_dates)} total")
        else:
            remaining_dates = all_dates
        
        # Summary tracking
        backfill_summary = {
            'season': self.season,
            'start_time': progress['start_time'],
            'total_dates': len(all_dates),
            'dates_processed': len(progress['completed_dates']),
            'dates_remaining': len(remaining_dates),
            'total_games_collected': progress['total_games'],
            'daily_summaries': [],
            'errors': list(progress.get('failed_dates', set()))
        }
        
        # Process each date with progress tracking
        with tqdm(remaining_dates, desc="Backfilling NBA 2024-25", unit="day") as pbar:
            for current_date in pbar:
                date_str = current_date.strftime('%Y-%m-%d')
                pbar.set_description(f"Collecting {date_str}")
                
                # Collect data for this day
                daily_summary = self.collect_single_historical_day(current_date)
                backfill_summary['daily_summaries'].append(daily_summary)
                
                # Update progress
                if daily_summary['errors']:
                    progress['failed_dates'].add(date_str)
                else:
                    progress['completed_dates'].add(date_str)
                    progress['total_games'] += daily_summary['games_found']
                    backfill_summary['total_games_collected'] += daily_summary['games_found']
                    backfill_summary['dates_processed'] += 1
                
                # Save progress every 10 days
                if len(progress['completed_dates']) % 10 == 0:
                    self.save_progress(progress)
                    self.save_backfill_summary(backfill_summary)
                
                # Update progress bar
                pbar.set_postfix({
                    'games': backfill_summary['total_games_collected'],
                    'errors': len(progress['failed_dates'])
                })
                
                # Pause between days for rate limiting
                time.sleep(self.day_delay)
        
        # Final progress save
        backfill_summary['end_time'] = datetime.now()
        backfill_summary['total_duration'] = (backfill_summary['end_time'] - backfill_summary['start_time']).total_seconds()
        
        self.save_progress(progress)
        self.save_backfill_summary(backfill_summary)
        
        logger.info(f"🎉 BACKFILL COMPLETE!")
        logger.info(f"📊 Collected {backfill_summary['total_games_collected']} games across {backfill_summary['dates_processed']} days")
        logger.info(f"⏱️ Duration: {backfill_summary['total_duration']/3600:.1f} hours")
        
        return backfill_summary

    def save_backfill_summary(self, summary: Dict):
        """Save detailed backfill summary"""
        try:
            summary_file = f"{self.output_dir}/backfill_reports/backfill_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            # Convert datetime objects to strings for JSON serialization
            summary_copy = summary.copy()
            for key in ['start_time', 'end_time']:
                if key in summary_copy and summary_copy[key]:
                    summary_copy[key] = summary_copy[key].isoformat()
            
            with open(summary_file, 'w') as f:
                json.dump(summary_copy, f, indent=2, default=str)
            
            # Also save latest summary
            latest_file = f"{self.output_dir}/backfill_summary.json"
            with open(latest_file, 'w') as f:
                json.dump(summary_copy, f, indent=2, default=str)
                
        except Exception as e:
            logger.error(f"❌ Error saving summary: {e}")

    def get_playoffs_dates(self) -> List[datetime]:
        """Get estimated playoff dates for 2024-25 season"""
        return self.get_all_season_dates(self.playoffs_start, self.season_end)

    def get_month_dates(self, year_month: str) -> List[datetime]:
        """Get all dates for a specific month (format: 2024-11)"""
        try:
            year, month = map(int, year_month.split('-'))
            start_date = datetime(year, month, 1)
            
            # Get last day of month
            if month == 12:
                end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(year, month + 1, 1) - timedelta(days=1)
            
            return self.get_all_season_dates(start_date, end_date)
            
        except Exception as e:
            logger.error(f"❌ Error parsing month {year_month}: {e}")
            return []

def parse_date(date_str: str) -> datetime:
    """Parse date string in various formats"""
    formats = ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y']
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse date: {date_str}")

def main():
    """Main execution with comprehensive options"""
    parser = argparse.ArgumentParser(
        description="NBA 2024-25 Season Historical Backfill Collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python nba_2024_25_backfill.py --full-season
  python nba_2024_25_backfill.py --start 2024-10-01 --end 2024-12-31
  python nba_2024_25_backfill.py --start 2024-05-23 --end 2024-05-23
  python nba_2024_25_backfill.py --resume
  python nba_2024_25_backfill.py --playoffs-only
  python nba_2024_25_backfill.py --month 2024-11
        """
    )
    
    # Collection scope options
    scope_group = parser.add_mutually_exclusive_group(required=False)
    scope_group.add_argument('--full-season', action='store_true', help='Backfill entire 2024-25 season')
    scope_group.add_argument('--playoffs-only', action='store_true', help='Just playoff games')
    scope_group.add_argument('--month', type=str, help='Single month (YYYY-MM)')
    scope_group.add_argument('--resume', action='store_true', help='Resume interrupted backfill')
    
    # Date range options (can be used independently)
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD) - automatically triggers custom range')
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD) - automatically triggers custom range')
    
    # Configuration options
    parser.add_argument('--odds-api-key', type=str, help='The Odds API key for betting data')
    parser.add_argument('--output-dir', type=str, default='nba_2024_25_data', help='Output directory')
    
    args = parser.parse_args()
    
    # Validate arguments and determine collection type
    has_scope_option = any([args.full_season, args.playoffs_only, args.month, args.resume])
    has_date_range = args.start or args.end
    
    # If no scope option but date range provided, use custom range
    if not has_scope_option and has_date_range:
        if not args.start or not args.end:
            logger.error("❌ Custom date range requires both --start and --end dates")
            return
        custom_range = True
    elif not has_scope_option and not has_date_range:
        logger.error("❌ Please specify a collection option: --full-season, --resume, --month, --playoffs-only, or provide --start and --end dates")
        parser.print_help()
        return
    else:
        custom_range = False
    
    # Initialize backfill collector
    collector = NBA2024_25Backfill(
        output_dir=args.output_dir,
        odds_api_key=args.odds_api_key
    )
    
    # Execute based on options
    if args.full_season:
        logger.info("🎯 Full season backfill: Oct 2024 - Current")
        collector.backfill_season()
        
    elif args.resume:
        logger.info("🔄 Resuming previous backfill")
        collector.backfill_season(resume=True)
        
    elif args.playoffs_only:
        logger.info("🏆 Playoffs only backfill")
        collector.backfill_season(start_date=collector.playoffs_start)
        
    elif args.month:
        logger.info(f"📅 Monthly backfill: {args.month}")
        month_dates = collector.get_month_dates(args.month)
        if month_dates:
            collector.backfill_season(start_date=month_dates[0], end_date=month_dates[-1])
        
    elif custom_range:
        start_date = parse_date(args.start)
        end_date = parse_date(args.end)
        logger.info(f"📊 Custom range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        collector.backfill_season(start_date=start_date, end_date=end_date)

if __name__ == "__main__":
    main()