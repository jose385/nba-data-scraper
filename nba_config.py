#!/usr/bin/env python3
"""
nba_config.py - NBA-specific configuration management
Adapted from MLB config.py architecture
"""
import os
import sys
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime

class NBAConfig:
    """NBA betting analysis configuration - mirrors MLB config structure"""
    
    def __init__(self):
        # Core database configuration  
        self.PG_DSN = os.getenv("NBA_PG_DSN") or os.getenv("PG_DSN")
        
        # NBA API configuration
        self.NBA_API_DELAY = float(os.getenv("NBA_API_DELAY", "2.5"))
        self.NBA_API_TIMEOUT = float(os.getenv("NBA_API_TIMEOUT", "30.0"))
        self.NBA_API_RETRY_COUNT = int(os.getenv("NBA_API_RETRY_COUNT", "4"))
        
        # Data modes
        self.USE_PLACEHOLDER_DATA = os.getenv("USE_PLACEHOLDER_DATA", "true").lower() == "true"
        self.PLACEHOLDER_GAMES_PER_DAY = int(os.getenv("PLACEHOLDER_GAMES_PER_DAY", "8"))
        
        # Directory paths
        self.OUTPUT_DIR = os.getenv("OUTPUT_DIR", "stage")
        self.LOG_DIR = os.getenv("LOG_DIR", "logs") 
        self.MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR", "migrations")
        
        # NBA-specific features
        self.ENABLE_PLAYER_TRACKING = os.getenv("ENABLE_PLAYER_TRACKING", "true").lower() == "true"
        self.ENABLE_SHOT_CHART = os.getenv("ENABLE_SHOT_CHART", "true").lower() == "true"
        self.ENABLE_ADVANCED_BOXSCORES = os.getenv("ENABLE_ADVANCED_BOXSCORES", "true").lower() == "true"
        self.ENABLE_LINEUP_ANALYSIS = os.getenv("ENABLE_LINEUP_ANALYSIS", "true").lower() == "true"
        
        # NBA betting thresholds
        self.HIGH_SCORING_THRESHOLD = int(os.getenv("HIGH_SCORING_THRESHOLD", "230"))
        self.LOW_SCORING_THRESHOLD = int(os.getenv("LOW_SCORING_THRESHOLD", "195"))
        self.BLOWOUT_THRESHOLD = int(os.getenv("BLOWOUT_THRESHOLD", "20"))
        
        # Player prop thresholds
        self.POINTS_PROP_THRESHOLD = float(os.getenv("POINTS_PROP_THRESHOLD", "15.5"))
        self.REBOUNDS_PROP_THRESHOLD = float(os.getenv("REBOUNDS_PROP_THRESHOLD", "8.5"))
        self.ASSISTS_PROP_THRESHOLD = float(os.getenv("ASSISTS_PROP_THRESHOLD", "5.5"))
        
        # Data quality settings
        self.MIN_TRACKING_COVERAGE = float(os.getenv("MIN_TRACKING_COVERAGE", "0.8"))
        self.MIN_GAMES_FOR_TRENDS = int(os.getenv("MIN_GAMES_FOR_TRENDS", "5"))
        
        # Logging and debug
        self.DEBUG = os.getenv("DEBUG", "false").lower() == "true"
        self.VERBOSE = os.getenv("VERBOSE", "false").lower() == "true"
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        
    def test_nba_api_connection(self) -> Tuple[bool, str]:
        """Test NBA API connectivity"""
        try:
            from nba_api.stats.static import teams
            team_list = teams.get_teams()
            if team_list and len(team_list) == 30:
                return True, f"NBA API working - found {len(team_list)} teams"
            else:
                return False, f"NBA API returned unexpected data: {len(team_list)} teams"
        except ImportError:
            return False, "nba_api library not installed - run: pip install nba_api"
        except Exception as e:
            return False, f"NBA API connection failed: {e}"
    
    def test_database_connection(self) -> Tuple[bool, str]:
        """Test database connection"""
        if not self.PG_DSN:
            return False, "NBA_PG_DSN not configured"
        
        try:
            import psycopg2
            conn = psycopg2.connect(self.PG_DSN)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0] == 1:
                return True, "Database connection successful"
            else:
                return False, "Database query returned unexpected result"
                
        except ImportError:
            return False, "psycopg2 not installed - run: pip install psycopg2-binary"
        except Exception as e:
            return False, f"Database connection failed: {e}"
    
    def get_enabled_features(self) -> List[str]:
        """Get list of enabled features"""
        features = []
        
        if self.ENABLE_PLAYER_TRACKING:
            features.append("Player Tracking")
        if self.ENABLE_SHOT_CHART:
            features.append("Shot Chart Analysis")
        if self.ENABLE_ADVANCED_BOXSCORES:
            features.append("Advanced Box Scores")
        if self.ENABLE_LINEUP_ANALYSIS:
            features.append("Lineup Analysis")
            
        return features
    
    def print_status(self):
        """Print comprehensive configuration status"""
        print("🏀 NBA Configuration Status")
        print("=" * 40)
        
        # Data mode
        mode = "PLACEHOLDER" if self.USE_PLACEHOLDER_DATA else "REAL API"
        print(f"📊 Data Mode: {mode}")
        
        # Database
        db_status = "✅ Configured" if self.PG_DSN else "❌ Not configured"
        print(f"🗄️ Database: {db_status}")
        
        # Features
        enabled_features = self.get_enabled_features()
        print(f"🔧 Features: {len(enabled_features)} enabled")
        for feature in enabled_features:
            print(f"   • {feature}")
        
        # Directories
        print(f"📁 Output: {self.OUTPUT_DIR}")
        print(f"📄 Logs: {self.LOG_DIR}")
        
        # NBA-specific settings
        print(f"🏀 Game Thresholds: {self.LOW_SCORING_THRESHOLD}-{self.HIGH_SCORING_THRESHOLD} points")
        print(f"📊 Prop Thresholds: {self.POINTS_PROP_THRESHOLD}pts, {self.REBOUNDS_PROP_THRESHOLD}reb, {self.ASSISTS_PROP_THRESHOLD}ast")

def get_nba_config() -> NBAConfig:
    """Get NBA configuration instance"""
    return NBAConfig()

def require_nba_config(
    require_database: bool = False,
    require_nba_api: bool = False,
    graceful_degradation: bool = True
) -> NBAConfig:
    """Get NBA config with validation requirements"""
    config = get_nba_config()
    
    if require_database:
        success, message = config.test_database_connection()
        if not success:
            if graceful_degradation:
                print(f"⚠️ Database not available: {message}")
            else:
                raise Exception(f"Database required but not available: {message}")
    
    if require_nba_api:
        success, message = config.test_nba_api_connection()
        if not success:
            if graceful_degradation:
                print(f"⚠️ NBA API not available: {message}")
            else:
                raise Exception(f"NBA API required but not available: {message}")
    
    return config

def main():
    """Test NBA configuration"""
    print("🏀 NBA Configuration Test")
    print("=" * 30)
    
    config = get_nba_config()
    config.print_status()
    
    print("\n🧪 Testing connections...")
    
    # Test NBA API
    api_success, api_message = config.test_nba_api_connection()
    print(f"📡 NBA API: {'✅' if api_success else '❌'} {api_message}")
    
    # Test Database
    db_success, db_message = config.test_database_connection()
    print(f"🗄️ Database: {'✅' if db_success else '❌'} {db_message}")
    
    print(f"\n🎯 Ready for NBA data collection: {'✅' if (api_success or config.USE_PLACEHOLDER_DATA) else '❌'}")

if __name__ == "__main__":
    main()