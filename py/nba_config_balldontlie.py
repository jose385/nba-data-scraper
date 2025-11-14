#!/usr/bin/env python3
"""
nba_config_balldontlie.py - Simplified NBA configuration for BallDontLie API
Much simpler than the old nba_config.py!
"""
import os
from typing import Tuple, Optional
from pathlib import Path


class NBABallDontLieConfig:
    """Simple NBA configuration for BallDontLie API"""
    
    def __init__(self):
        # BallDontLie API settings
        self.BALLDONTLIE_API_KEY = os.getenv("BALLDONTLIE_API_KEY")  # Optional
        self.BALLDONTLIE_BASE_URL = "https://api.balldontlie.io/v1"
        
        # Rate limiting (requests per minute)
        self.RATE_LIMIT_FREE = 30       # Free tier
        self.RATE_LIMIT_WITH_KEY = 60   # With API key
        
        # Database configuration
        self.PG_DSN = os.getenv("NBA_PG_DSN") or os.getenv("PG_DSN")
        
        # Directory paths
        self.OUTPUT_DIR = os.getenv("OUTPUT_DIR", "stage")
        self.LOG_DIR = os.getenv("LOG_DIR", "logs")
        self.MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR", "migrations")
        
        # Data collection settings
        self.COMBINE_DAILY_FILES = True  # Create combined game+stats files
        self.SAVE_RAW_JSON = False       # Save raw API responses
        
        # Current season (adjust each year)
        self.CURRENT_SEASON = 2024  # 2024-25 season
        
        # Logging
        self.DEBUG = os.getenv("DEBUG", "false").lower() == "true"
        self.VERBOSE = os.getenv("VERBOSE", "false").lower() == "true"
        
        # Ensure directories exist
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create required directories if they don't exist"""
        for directory in [self.OUTPUT_DIR, self.LOG_DIR]:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def get_rate_limit(self) -> int:
        """Get appropriate rate limit based on API key availability"""
        return self.RATE_LIMIT_WITH_KEY if self.BALLDONTLIE_API_KEY else self.RATE_LIMIT_FREE
    
    def test_database_connection(self) -> Tuple[bool, str]:
        """Test database connection"""
        if not self.PG_DSN:
            return False, "NBA_PG_DSN not configured in .env"
        
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
    
    def test_balldontlie_api(self) -> Tuple[bool, str]:
        """Test BallDontLie API connectivity"""
        try:
            import requests
            
            headers = {'Accept': 'application/json'}
            if self.BALLDONTLIE_API_KEY:
                headers['Authorization'] = self.BALLDONTLIE_API_KEY
            
            response = requests.get(
                f"{self.BALLDONTLIE_BASE_URL}/teams",
                headers=headers,
                params={'page': 1, 'per_page': 1},
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and len(data['data']) > 0:
                    rate_limit = self.get_rate_limit()
                    key_status = "with API key" if self.BALLDONTLIE_API_KEY else "free tier"
                    return True, f"BallDontLie API working ({key_status}, {rate_limit} req/min)"
                else:
                    return False, "BallDontLie API returned unexpected data format"
            elif response.status_code == 401:
                return False, "Invalid API key"
            elif response.status_code == 429:
                return False, "Rate limit exceeded - wait and try again"
            else:
                return False, f"API returned status {response.status_code}"
                
        except ImportError:
            return False, "requests library not installed - run: pip install requests"
        except Exception as e:
            return False, f"BallDontLie API test failed: {e}"
    
    def print_status(self):
        """Print configuration status"""
        print("🏀 NBA BallDontLie Configuration")
        print("=" * 50)
        
        # API status
        api_key_status = "✅ Configured" if self.BALLDONTLIE_API_KEY else "⚠️ Using free tier"
        rate_limit = self.get_rate_limit()
        print(f"🔑 API Key: {api_key_status}")
        print(f"⏱️ Rate Limit: {rate_limit} requests/minute")
        
        # Database
        db_status = "✅ Configured" if self.PG_DSN else "❌ Not configured"
        print(f"🗄️ Database: {db_status}")
        
        # Directories
        print(f"📁 Output: {self.OUTPUT_DIR}")
        print(f"📄 Logs: {self.LOG_DIR}")
        
        # Season
        print(f"🏀 Current Season: {self.CURRENT_SEASON}-{self.CURRENT_SEASON + 1}")
        
        print()
        print("💡 To improve rate limit:")
        print("   1. Get free API key at https://www.balldontlie.io/")
        print("   2. Add to .env: BALLDONTLIE_API_KEY=your_key")
        print("   3. Rate limit increases from 30 to 60 req/min")


def get_config() -> NBABallDontLieConfig:
    """Get NBA BallDontLie configuration instance"""
    return NBABallDontLieConfig()


def main():
    """Test NBA BallDontLie configuration"""
    print("🏀 NBA BallDontLie Configuration Test")
    print("=" * 50)
    print()
    
    config = get_config()
    config.print_status()
    
    print("\n🧪 Testing connections...")
    print()
    
    # Test BallDontLie API
    api_success, api_message = config.test_balldontlie_api()
    print(f"📡 BallDontLie API: {'✅' if api_success else '❌'} {api_message}")
    
    # Test Database
    if config.PG_DSN:
        db_success, db_message = config.test_database_connection()
        print(f"🗄️ Database: {'✅' if db_success else '❌'} {db_message}")
    else:
        print(f"🗄️ Database: ⚠️ Not configured (optional)")
    
    print()
    
    if api_success:
        print("🎉 Ready to collect NBA data!")
        print()
        print("🚀 Try:")
        print("   python py/nba_balldontlie_backfill.py --start 2024-11-13 --end 2024-11-13")
    else:
        print("⚠️ Fix API connection issues before collecting data")
        print()
        print("💡 Troubleshooting:")
        print("   1. Check internet connection")
        print("   2. Visit https://www.balldontlie.io/ to verify API status")
        print("   3. Try again in a few minutes")


if __name__ == "__main__":
    main()