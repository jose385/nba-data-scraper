#!/usr/bin/env python3
"""
NBA 2024-25 Season Daily Data Collector

Flexible system to collect NBA data for specific days, date ranges, or catch up on missed games.
Perfect for building your prediction model incrementally and staying current with the season.

Usage Examples:
    python nba_daily_collector.py --date 2024-10-20                    # Single game day
    python nba_daily_collector.py --start 2024-10-15 --end 2024-10-25  # Date range
    python nba_daily_collector.py --last-week                          # Last 7 days
    python nba_daily_collector.py --yesterday                          # Yesterday's games
    python nba_daily_collector.py --catch-up                           # Fill missing days
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
from pathlib import Path

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
except ImportError:
    print("❌ nba_api not installed. Install with: pip install nba_api")
    exit(1)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nba_daily_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NBADailyCollector:
    def __init__(self, output_dir: str = "nba_2024_25_data", odds_api_key: Optional[str] = None):
        """
        Initialize NBA Daily Data Collector for 2024-25 season
        
        Args:
            output_dir: Directory to save all data files
            odds_api_key: Optional API key for betting odds data
        """
        self.output_dir = output_dir
        self.odds_api_key = odds_api_key
        self.season = "2024-25"
        self.season_start = datetime(2024, 10, 1)  # 2024-25 season started
        self.season_end = datetime(2025, 6, 30)    # End of potential playoffs
        
        # Create output directories
        self.create_directories()
        
        # Get static data
        self.nba_teams = teams.get_teams()
        self.team_id_map = {team['abbreviation']: team['id'] for team in self.nba_teams}
        self.team_name_map = {team['full_name']: team['abbreviation'] for team in self.nba_teams}
        
        logger.info(f"🏀 NBA Daily Collector initialized for {self.season} season")
        logger.info(f"📁 Output directory: {self.output_dir}")

    def create_directories(self):
        """Create organized directory structure"""
        directories = [
            self.output_dir,
            f"{self.output_dir}/daily_games",
            f"{self.output_dir}/team_stats", 
            f"{self.output_dir}/player_stats",
            f"{self.output_dir}/odds_data",
            f"{self.output_dir}/combined_daily",
            f"{self.output_dir}/logs"
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)

    def get_games_for_date(self, target_date: datetime) -> pd.DataFrame:
        """Get all NBA games for a specific date"""
        date_str = target_date.strftime('%m/%d/%Y')
        logger.info(f"🔍 Fetching games for {target_date.strftime('%Y-%m-%d')}")
        
        try:
            # Get scoreboard for the specific date
            scoreboard_data = scoreboardv2.ScoreboardV2(game_date=date_str)
            games_df = scoreboard_data.get_data_frames()[0]  # GameHeader
            
            if games_df.empty:
                logger.info(f"📅 No games found for {target_date.strftime('%Y-%m-%d')}")
                return pd.DataFrame()
            
            # Add date info
            games_df['GAME_DATE'] = target_date.strftime('%Y-%m-%d')
            games_df['SEASON'] = self.season
            
            logger.info(f"✅ Found {len(games_df)} games for {target_date.strftime('%Y-%m-%d')}")
            return games_df
            
        except Exception as e:
            logger.error(f"❌ Error fetching games for {target_date.strftime('%Y-%m-%d')}: {str(e)}")
            return pd.DataFrame()

    def get_detailed_game_data(self, game_id: str, game_date: str) -> Dict[str, pd.DataFrame]:
        """Get comprehensive data for a specific game"""
        logger.info(f"📊 Collecting detailed data for game {game_id}")
        
        result = {}
        
        try:
            # Traditional box score (team and player stats)
            traditional_box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
            traditional_data = traditional_box.get_data_frames()
            
            if len(traditional_data) >= 2:
                player_stats = traditional_data[0]
                team_stats = traditional_data[1]
                
                # Add metadata
                player_stats['GAME_ID'] = game_id
                player_stats['GAME_DATE'] = game_date
                team_stats['GAME_ID'] = game_id
                team_stats['GAME_DATE'] = game_date
                
                result['player_traditional'] = player_stats
                result['team_traditional'] = team_stats
            
            # Advanced box score
            time.sleep(0.6)  # Rate limiting
            advanced_box = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id)
            advanced_data = advanced_box.get_data_frames()
            
            if len(advanced_data) >= 2:
                player_advanced = advanced_data[0]
                team_advanced = advanced_data[1]
                
                # Add metadata
                player_advanced['GAME_ID'] = game_id
                player_advanced['GAME_DATE'] = game_date
                team_advanced['GAME_ID'] = game_id
                team_advanced['GAME_DATE'] = game_date
                
                result['player_advanced'] = player_advanced
                result['team_advanced'] = team_advanced
            
            time.sleep(0.6)  # Rate limiting
            
        except Exception as e:
            logger.warning(f"⚠️ Error getting detailed data for game {game_id}: {str(e)}")
        
        return result

    def get_current_standings(self) -> pd.DataFrame:
        """Get current NBA standings"""
        try:
            standings = leaguestandingsv3.LeagueStandingsV3(season=self.season)
            standings_df = standings.get_data_frames()[0]
            
            # Add timestamp
            standings_df['UPDATED_DATE'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return standings_df
            
        except Exception as e:
            logger.error(f"❌ Error fetching standings: {str(e)}")
            return pd.DataFrame()

    def get_current_team_stats(self) -> pd.DataFrame:
        """Get current season team statistics"""
        try:
            team_stats = leaguedashteamstats.LeagueDashTeamStats(
                season=self.season,
                season_type_all_star='Regular Season'
            )
            stats_df = team_stats.get_data_frames()[0]
            
            # Add timestamp
            stats_df['UPDATED_DATE'] = datetime.now().strftime('%Y-%m-%d')
            
            return stats_df
            
        except Exception as e:
            logger.error(f"❌ Error fetching team stats: {str(e)}")
            return pd.DataFrame()

    def get_betting_odds_for_date(self, target_date: datetime) -> pd.DataFrame:
        """Get betting odds for specific date"""
        if not self.odds_api_key:
            return pd.DataFrame()
        
        logger.info(f"💰 Fetching betting odds for {target_date.strftime('%Y-%m-%d')}")
        
        try:
            # The Odds API endpoint for NBA
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
                
                if odds_data:
                    # Filter for target date games
                    target_date_str = target_date.strftime('%Y-%m-%d')
                    filtered_odds = []
                    
                    for game in odds_data:
                        game_date = datetime.fromisoformat(game['commence_time'].replace('Z', '+00:00'))
                        if game_date.strftime('%Y-%m-%d') == target_date_str:
                            filtered_odds.append(game)
                    
                    if filtered_odds:
                        odds_df = pd.json_normalize(filtered_odds)
                        odds_df['COLLECTION_DATE'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        logger.info(f"✅ Found odds for {len(filtered_odds)} games")
                        return odds_df
                    else:
                        logger.info(f"📅 No odds found for {target_date_str}")
                        
            else:
                logger.warning(f"⚠️ Odds API error: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Error fetching odds: {str(e)}")
        
        return pd.DataFrame()

    def collect_single_day(self, target_date: datetime) -> Dict[str, any]:
        """Collect all data for a single day"""
        date_str = target_date.strftime('%Y-%m-%d')
        logger.info(f"🎯 Starting data collection for {date_str}")
        
        collection_summary = {
            'date': date_str,
            'games_found': 0,
            'detailed_games_collected': 0,
            'odds_collected': False,
            'files_created': [],
            'errors': []
        }
        
        try:
            # Step 1: Get games for the date
            games_df = self.get_games_for_date(target_date)
            
            if games_df.empty:
                logger.info(f"📅 No games on {date_str}")
                return collection_summary
            
            collection_summary['games_found'] = len(games_df)
            
            # Step 2: Get detailed data for each game
            all_detailed_data = {
                'player_traditional': [],
                'team_traditional': [],
                'player_advanced': [],
                'team_advanced': []
            }
            
            for _, game in games_df.iterrows():
                game_id = game['GAME_ID']
                detailed_data = self.get_detailed_game_data(game_id, date_str)
                
                if detailed_data:
                    collection_summary['detailed_games_collected'] += 1
                    
                    # Collect all detailed data
                    for data_type, df in detailed_data.items():
                        if not df.empty:
                            all_detailed_data[data_type].append(df)
            
            # Step 3: Get betting odds
            odds_df = self.get_betting_odds_for_date(target_date)
            if not odds_df.empty:
                collection_summary['odds_collected'] = True
            
            # Step 4: Save all data
            files_created = self.save_daily_data(
                date_str, games_df, all_detailed_data, odds_df
            )
            collection_summary['files_created'] = files_created
            
            logger.info(f"✅ Completed collection for {date_str}: {collection_summary['games_found']} games")
            
        except Exception as e:
            error_msg = f"Error collecting data for {date_str}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            collection_summary['errors'].append(error_msg)
        
        return collection_summary

    def save_daily_data(self, date_str: str, games_df: pd.DataFrame, 
                       detailed_data: Dict[str, List[pd.DataFrame]], 
                       odds_df: pd.DataFrame) -> List[str]:
        """Save all collected data for a specific day"""
        files_created = []
        
        try:
            # Save basic games info
            games_file = f"{self.output_dir}/daily_games/games_{date_str}.csv"
            games_df.to_csv(games_file, index=False)
            files_created.append(games_file)
            
            # Save detailed stats (combine all games for the day)
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
            
            # Create combined daily file (all data in one file)
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
        """Create single file with all data for the day (LLM-optimized)"""
        try:
            combined_file = f"{self.output_dir}/combined_daily/nba_complete_{date_str}.csv"
            
            with open(combined_file, 'w') as f:
                # Header with metadata
                f.write(f"# NBA Complete Data for {date_str}\n")
                f.write(f"# Season: {self.season}\n")
                f.write(f"# Collection Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# Games: {len(games_df)}\n\n")
                
                # Games basic info
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
                
                # Odds data
                if not odds_df.empty:
                    f.write("## BETTING_ODDS\n")
                    odds_df.to_csv(f, index=False)
                    f.write("\n")
            
            logger.info(f"📄 Created combined file: {combined_file}")
            return combined_file
            
        except Exception as e:
            logger.error(f"❌ Error creating combined file: {str(e)}")
            return None

    def collect_date_range(self, start_date: datetime, end_date: datetime) -> Dict[str, any]:
        """Collect data for a range of dates"""
        logger.info(f"📅 Collecting data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        current_date = start_date
        total_summary = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'total_days': 0,
            'days_with_games': 0,
            'total_games': 0,
            'daily_summaries': []
        }
        
        while current_date <= end_date:
            daily_summary = self.collect_single_day(current_date)
            total_summary['daily_summaries'].append(daily_summary)
            total_summary['total_days'] += 1
            
            if daily_summary['games_found'] > 0:
                total_summary['days_with_games'] += 1
                total_summary['total_games'] += daily_summary['games_found']
            
            current_date += timedelta(days=1)
            time.sleep(1)  # Brief pause between days
        
        # Save summary report
        self.save_collection_summary(total_summary)
        
        logger.info(f"🎉 Range collection complete: {total_summary['total_games']} games across {total_summary['days_with_games']} days")
        return total_summary

    def save_collection_summary(self, summary: Dict[str, any]):
        """Save collection summary to file"""
        try:
            summary_file = f"{self.output_dir}/collection_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"📋 Summary saved: {summary_file}")
            
        except Exception as e:
            logger.error(f"❌ Error saving summary: {str(e)}")

    def find_missing_days(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """Find days in range that don't have data files"""
        missing_days = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            combined_file = f"{self.output_dir}/combined_daily/nba_complete_{date_str}.csv"
            
            if not os.path.exists(combined_file):
                missing_days.append(current_date)
            
            current_date += timedelta(days=1)
        
        if missing_days:
            logger.info(f"📋 Found {len(missing_days)} missing days")
            for day in missing_days[:5]:  # Show first 5
                logger.info(f"   - {day.strftime('%Y-%m-%d')}")
            if len(missing_days) > 5:
                logger.info(f"   ... and {len(missing_days) - 5} more")
        
        return missing_days

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
    """Main execution function with command line interface"""
    parser = argparse.ArgumentParser(
        description="NBA 2024-25 Season Daily Data Collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python nba_daily_collector.py --date 2024-10-20
  python nba_daily_collector.py --start 2024-10-15 --end 2024-10-25
  python nba_daily_collector.py --yesterday
  python nba_daily_collector.py --last-week
  python nba_daily_collector.py --catch-up --start 2024-10-01
        """
    )
    
    # Date selection options
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument('--date', type=str, help='Single date (YYYY-MM-DD)')
    date_group.add_argument('--yesterday', action='store_true', help='Yesterday\'s games')
    date_group.add_argument('--today', action='store_true', help='Today\'s games')
    date_group.add_argument('--last-week', action='store_true', help='Last 7 days')
    date_group.add_argument('--catch-up', action='store_true', help='Fill missing days (requires --start)')
    
    # Date range options
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD)')
    
    # API options
    parser.add_argument('--odds-api-key', type=str, help='The Odds API key for betting data')
    parser.add_argument('--output-dir', type=str, default='nba_2024_25_data', help='Output directory')
    
    args = parser.parse_args()
    
    # Initialize collector
    collector = NBADailyCollector(
        output_dir=args.output_dir,
        odds_api_key=args.odds_api_key
    )
    
    # Determine dates to collect
    if args.date:
        target_date = parse_date(args.date)
        logger.info(f"🎯 Collecting single day: {target_date.strftime('%Y-%m-%d')}")
        collector.collect_single_day(target_date)
        
    elif args.yesterday:
        target_date = datetime.now() - timedelta(days=1)
        logger.info(f"🎯 Collecting yesterday: {target_date.strftime('%Y-%m-%d')}")
        collector.collect_single_day(target_date)
        
    elif args.today:
        target_date = datetime.now()
        logger.info(f"🎯 Collecting today: {target_date.strftime('%Y-%m-%d')}")
        collector.collect_single_day(target_date)
        
    elif args.last_week:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        logger.info(f"🎯 Collecting last week: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        collector.collect_date_range(start_date, end_date)
        
    elif args.catch_up:
        if not args.start:
            logger.error("❌ --catch-up requires --start date")
            return
        
        start_date = parse_date(args.start)
        end_date = parse_date(args.end) if args.end else datetime.now()
        
        # Find missing days
        missing_days = collector.find_missing_days(start_date, end_date)
        
        if missing_days:
            logger.info(f"🔄 Catching up on {len(missing_days)} missing days")
            for missing_date in missing_days:
                collector.collect_single_day(missing_date)
        else:
            logger.info("✅ No missing days found - all data up to date!")
    
    elif args.start and args.end:
        start_date = parse_date(args.start)
        end_date = parse_date(args.end)
        logger.info(f"🎯 Collecting date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        collector.collect_date_range(start_date, end_date)
    
    else:
        logger.error("❌ Please specify a date option")
        parser.print_help()

if __name__ == "__main__":
    main()