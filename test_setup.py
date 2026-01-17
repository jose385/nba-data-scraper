#!/usr/bin/env python3
"""Test script to verify BallDontLie API setup"""

import os
import sys
from pathlib import Path

def main():
    print("\n🏀 BallDontLie NBA Data Setup Test\n")
    print("=" * 50)
    
    # Check .env
    env_file = Path(".env")
    if env_file.exists():
        print("✅ .env file found")
    else:
        print("❌ .env file not found - create it with your API key")
        return
    
    # Check dependencies
    try:
        import requests
        print("✅ requests installed")
    except ImportError:
        print("❌ requests not installed")
        return
    
    try:
        import pandas
        print("✅ pandas installed")
    except ImportError:
        print("❌ pandas not installed")
        return
    
    try:
        import pyarrow
        print("✅ pyarrow installed")
    except ImportError:
        print("❌ pyarrow not installed")
        return
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("✅ python-dotenv installed")
    except ImportError:
        print("⚠️ python-dotenv not installed (optional)")
    
    # Test API
    print("\n" + "=" * 50)
    print("🌐 Testing API Connection...")
    print("=" * 50)
    
    sys.path.insert(0, str(Path(__file__).parent / "py"))
    
    try:
        from nba_balldontlie_client import BallDontLieClient
        client = BallDontLieClient()
        
        teams = client.get_teams()
        if teams:
            print(f"✅ Got {len(teams)} teams")
        else:
            print("❌ Failed to get teams")
            return
        
        injuries = client.get_injuries()
        if injuries is not None:
            print(f"✅ GOAT tier confirmed ({len(injuries)} injuries)")
        
        print(f"\n✅ ALL TESTS PASSED!")
        print("\nRun backfill:")
        print("  python py/nba_balldontlie_backfill.py --start 2025-01-01 --end 2025-01-15 --full")
        
    except ValueError as e:
        print(f"❌ {e}")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()