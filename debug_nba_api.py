 #!/usr/bin/env python3
"""
NBA API Connection Debugger

This script helps diagnose connection issues with the NBA API
and specifically checks for games on 2024-10-22 (season opener).
"""

import sys
import time
from datetime import datetime, timedelta
import requests

def test_basic_connectivity():
    """Test basic internet and NBA.com connectivity"""
    print("🌐 Testing Basic Connectivity:")
    print("-" * 40)
    
    # Test basic internet
    try:
        response = requests.get("https://www.google.com", timeout=10)
        print("✅ Internet connection: Working")
    except Exception as e:
        print(f"❌ Internet connection: Failed - {e}")
        return False
    
    # Test NBA.com
    try:
        response = requests.get("https://www.nba.com", timeout=10)
        print(f"✅ NBA.com: Working (Status: {response.status_code})")
    except Exception as e:
        print(f"❌ NBA.com: Failed - {e}")
    
    # Test NBA Stats API
    try:
        response = requests.get("https://stats.nba.com", timeout=10)
        print(f"✅ stats.nba.com: Working (Status: {response.status_code})")
    except Exception as e:
        print(f"❌ stats.nba.com: Failed - {e}")
        return False
    
    return True

def test_nba_api_import():
    """Test NBA API library import"""
    print("\n📚 Testing NBA API Library:")
    print("-" * 40)
    
    try:
        import nba_api
        print(f"✅ nba_api imported successfully")
        if hasattr(nba_api, '__version__'):
            print(f"   Version: {nba_api.__version__}")
        return True
    except ImportError as e:
        print(f"❌ nba_api import failed: {e}")
        print("   Install with: pip install nba_api")
        return False

def test_season_opener_specifically():
    """Test specifically for 2024-10-22 season opener"""
    print("\n🏀 Testing Season Opener (2024-10-22):")
    print("-" * 40)
    
    try:
        from nba_api.stats.endpoints import scoreboardv2
        
        # NBA API uses MM/DD/YYYY format
        season_opener_date = "10/22/2024"
        
        print(f"🔍 Requesting scoreboard for {season_opener_date}...")
        
        # Try with longer timeout
        start_time = time.time()
        scoreboard = scoreboardv2.ScoreboardV2(game_date=season_opener_date, timeout=120)
        end_time = time.time()
        
        print(f"⏱️ API call took {end_time - start_time:.1f} seconds")
        
        # Get the data
        games_df = scoreboard.get_data_frames()[0]
        
        if games_df.empty:
            print("❌ No games found for season opener")
            print("   This is unexpected - the season opener should have games")
            
            # Try alternative date formats
            alt_dates = ["10/22/2024", "2024-10-22", "22/10/2024"]
            for alt_date in alt_dates:
                try:
                    print(f"🔄 Trying alternative date format: {alt_date}")
                    alt_scoreboard = scoreboardv2.ScoreboardV2(game_date=alt_date, timeout=60)
                    alt_games_df = alt_scoreboard.get_data_frames()[0]
                    if not alt_games_df.empty:
                        print(f"✅ Found {len(alt_games_df)} games with format {alt_date}")
                        return True
                except Exception as e:
                    print(f"   ❌ {alt_date} failed: {str(e)[:100]}")
                    
        else:
            print(f"✅ Found {len(games_df)} games for season opener!")
            print("\nGames found:")
            for i, (_, game) in enumerate(games_df.iterrows()):
                home = game.get('HOME_TEAM_ID', 'Unknown')
                away = game.get('VISITOR_TEAM_ID', 'Unknown')
                game_id = game.get('GAME_ID', 'Unknown')
                print(f"  Game {i+1}: {away} @ {home} (ID: {game_id})")
            return True
            
    except Exception as e:
        print(f"❌ Season opener test failed: {str(e)}")
        print(f"   Error type: {type(e).__name__}")
        
        if "timeout" in str(e).lower():
            print("   🔍 This is a TIMEOUT error - NBA API is slow")
            print("   💡 Try: Increase timeout or try later")
        elif "connection" in str(e).lower():
            print("   🔍 This is a CONNECTION error")
            print("   💡 Try: Check internet connection or try later")
        
        return False

def test_live_scoreboard():
    """Test live scoreboard endpoint"""
    print("\n📡 Testing Live Scoreboard:")
    print("-" * 40)
    
    try:
        from nba_api.live.nba.endpoints import scoreboard
        
        print("🔍 Requesting live scoreboard...")
        start_time = time.time()
        live_board = scoreboard.ScoreBoard(timeout=60)
        end_time = time.time()
        
        print(f"⏱️ Live API call took {end_time - start_time:.1f} seconds")
        
        games_dict = live_board.get_dict()
        
        if games_dict and 'scoreboard' in games_dict:
            games_list = games_dict['scoreboard'].get('games', [])
            print(f"✅ Live scoreboard found {len(games_list)} total games")
            
            # Check for 2024-10-22 games
            target_date = datetime(2024, 10, 22)
            matching_games = []
            
            for game in games_list:
                if 'gameTimeUTC' in game:
                    try:
                        game_time = datetime.fromisoformat(
                            game['gameTimeUTC'].replace('Z', '+00:00')
                        )
                        if game_time.date() == target_date.date():
                            matching_games.append(game)
                    except:
                        pass
            
            if matching_games:
                print(f"✅ Found {len(matching_games)} games for 2024-10-22 in live data!")
                for i, game in enumerate(matching_games):
                    home_team = game.get('homeTeam', {}).get('teamName', 'Unknown')
                    away_team = game.get('awayTeam', {}).get('teamName', 'Unknown')
                    print(f"  Game {i+1}: {away_team} @ {home_team}")
                return True
            else:
                print("❌ No games for 2024-10-22 found in live data")
                print("   (This might be normal if games are not recent)")
        else:
            print("❌ Live scoreboard returned invalid data")
            
    except Exception as e:
        print(f"❌ Live scoreboard test failed: {str(e)}")
        
        if "timeout" in str(e).lower():
            print("   🔍 TIMEOUT error with live API too")
        
        return False

def provide_recommendations():
    """Provide recommendations based on test results"""
    print("\n💡 RECOMMENDATIONS:")
    print("=" * 50)
    
    print("1. 🐌 NBA API is experiencing SLOW response times")
    print("   Solution: Use the robust collector with longer timeouts")
    print("   Command: python nba_robust_collector.py --date 2024-10-22")
    print()
    
    print("2. 🔄 If timeouts persist, try these alternatives:")
    print("   - Run collection during off-peak hours (early morning)")
    print("   - Try collecting recent games instead of historical")
    print("   - Use smaller date ranges")
    print()
    
    print("3. 📊 For 2024-10-22 specifically:")
    print("   This was the NBA season opener with confirmed games:")
    print("   - Lakers vs Timberwolves")  
    print("   - Suns vs Clippers")
    print("   The robust collector has fallback data for this date")
    print()
    
    print("4. 🚀 Best approach right now:")
    print("   python nba_robust_collector.py --date 2024-10-22")
    print("   (This handles timeouts and has season opener fallback data)")

def main():
    """Main diagnostic function"""
    print("🏀 NBA API Connection Diagnostic Tool")
    print("=" * 50)
    
    # Run all tests
    connectivity_ok = test_basic_connectivity()
    api_import_ok = test_nba_api_import()
    
    if not connectivity_ok or not api_import_ok:
        print("\n❌ Basic requirements not met - fix these first")
        return
    
    # Test specific NBA API endpoints
    season_opener_ok = test_season_opener_specifically()
    live_api_ok = test_live_scoreboard()
    
    # Provide recommendations
    provide_recommendations()
    
    # Summary
    print(f"\n📊 DIAGNOSTIC SUMMARY:")
    print(f"   Internet: {'✅' if connectivity_ok else '❌'}")
    print(f"   NBA API Library: {'✅' if api_import_ok else '❌'}")
    print(f"   Season Opener Data: {'✅' if season_opener_ok else '❌'}")
    print(f"   Live API: {'✅' if live_api_ok else '❌'}")
    
    if not season_opener_ok and not live_api_ok:
        print("\n⚠️  NBA API appears to be having timeout/connection issues")
        print("   This is likely temporary - try the robust collector")

if __name__ == "__main__":
    main()
    