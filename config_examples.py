# NBA Daily Collector Configuration Examples
# Copy relevant sections to create your own config.py file

from datetime import datetime

class DailyCollectorConfig:
    """Configuration for NBA Daily Data Collector"""
    
    # =================
    # BASIC SETTINGS
    # =================
    
    # Current NBA season
    SEASON = "2024-25"
    
    # Output directory
    OUTPUT_DIR = "nba_2024_25_data"
    
    # =================
    # API KEYS
    # =================
    
    # The Odds API (free: 500 requests/month)
    # Get at: https://the-odds-api.com/
    THE_ODDS_API_KEY = None  # Replace with: "your_api_key_here"
    
    # =================
    # COLLECTION PREFERENCES
    # =================
    
    # Which data to collect
    COLLECT_TRADITIONAL_STATS = True      # Basic box scores
    COLLECT_ADVANCED_STATS = True         # Efficiency metrics
    COLLECT_BETTING_ODDS = True           # Requires API key
    CREATE_COMBINED_FILES = True          # LLM-optimized daily files
    
    # =================
    # RATE LIMITING
    # =================
    
    # NBA API delays (respect rate limits)
    NBA_API_DELAY = 0.6  # Seconds between requests
    ODDS_API_DELAY = 1.0  # Seconds between odds requests
    
    # =================
    # ERROR HANDLING
    # =================
    
    MAX_RETRIES = 3
    CONTINUE_ON_ERROR = True  # Don't stop collection for single game errors
    
    # =================
    # LOGGING
    # =================
    
    LOG_LEVEL = "INFO"
    LOG_TO_FILE = True
    LOG_FILENAME = "nba_daily_collector.log"


# =================
# QUICK CONFIGS
# =================

class QuickConfigs:
    """Pre-defined configurations for common scenarios"""
    
    @staticmethod
    def free_only():
        """Free APIs only (no betting data)"""
        config = DailyCollectorConfig()
        config.COLLECT_BETTING_ODDS = False
        config.THE_ODDS_API_KEY = None
        return config
    
    @staticmethod
    def with_odds(api_key: str):
        """Include betting odds data"""
        config = DailyCollectorConfig()
        config.THE_ODDS_API_KEY = api_key
        config.COLLECT_BETTING_ODDS = True
        return config
    
    @staticmethod
    def minimal():
        """Minimal data collection (fastest)"""
        config = DailyCollectorConfig()
        config.COLLECT_ADVANCED_STATS = False
        config.COLLECT_BETTING_ODDS = False
        return config


# =================
# USAGE EXAMPLES
# =================

# Example 1: Free data only
# config = QuickConfigs.free_only()

# Example 2: With betting odds
# config = QuickConfigs.with_odds("your_api_key_here")

# Example 3: Custom configuration
# config = DailyCollectorConfig()
# config.THE_ODDS_API_KEY = "your_key_here"
# config.OUTPUT_DIR = "my_custom_nba_data"