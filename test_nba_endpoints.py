#!/usr/bin/env python3
"""
NBA API Endpoint Tester

This script tests which NBA_API endpoints are actually available
in your current environment to avoid import errors.
"""

import sys

def test_nba_api_availability():
    """Test if NBA API is available"""
    try:
        import nba_api
        print(f"✅ NBA_API installed: version {nba_api.__version__ if hasattr(nba_api, '__version__') else 'unknown'}")
        return True
    except ImportError:
        print("❌ NBA_API not installed")
        print("   Install with: pip install nba_api")
        return False

def test_basic_endpoints():
    """Test basic endpoints that should always work"""
    print("\n🔧 Testing Basic Endpoints:")
    print("-" * 40)
    
    basic_endpoints = [
        ('scoreboardv2', 'ScoreboardV2'),
        ('boxscoretraditionalv2', 'BoxScoreTraditionalV2'),
        ('boxscoreadvancedv2', 'BoxScoreAdvancedV2'),
    ]
    
    available = {}
    
    for endpoint_name, class_name in basic_endpoints:
        try:
            module = __import__('nba_api.stats.endpoints', fromlist=[endpoint_name])
            endpoint_class = getattr(module, class_name)
            available[endpoint_name] = endpoint_class
            print(f"✅ {endpoint_name} - Available")
        except (ImportError, AttributeError) as e:
            print(f"❌ {endpoint_name} - Not available: {e}")
    
    return available

def test_enhanced_endpoints():
    """Test enhanced endpoints that may or may not be available"""
    print("\n📊 Testing Enhanced Endpoints:")
    print("-" * 40)
    
    enhanced_endpoints = [
        ('boxscoremiscv2', 'BoxScoreMiscV2'),
        ('boxscoreusagev2', 'BoxScoreUsageV2'),
        ('boxscorescoringv2', 'BoxScoreScoringV2'),
        ('boxscoreplayertrackv2', 'BoxScorePlayerTrackV2'),
        ('boxscorefourFactorsv2', 'BoxScoreFourFactorsV2'),  # This was causing the error
        ('playbyplay', 'PlayByPlay'),
        ('gamerotation', 'GameRotation'),
        ('hustlestatsboxscore', 'HustleStatsBoxscore'),
        ('leaguedashteamstats', 'LeagueDashTeamStats'),
        ('leaguedashplayerstats', 'LeagueDashPlayerStats'),
        ('leaguestandingsv3', 'LeagueStandingsV3'),
    ]
    
    available = {}
    
    for endpoint_name, class_name in enhanced_endpoints:
        try:
            module = __import__('nba_api.stats.endpoints', fromlist=[endpoint_name])
            endpoint_class = getattr(module, class_name)
            available[endpoint_name] = endpoint_class
            print(f"✅ {endpoint_name} - Available")
        except (ImportError, AttributeError):
            print(f"❌ {endpoint_name} - Not available")
    
    return available

def test_static_data():
    """Test static data availability"""
    print("\n📋 Testing Static Data:")
    print("-" * 40)
    
    try:
        from nba_api.stats.static import teams, players
        print("✅ teams - Available")
        print("✅ players - Available")
        return True
    except ImportError:
        print("❌ Static data not available")
        return False

def test_live_endpoints():
    """Test live endpoints"""
    print("\n🔴 Testing Live Endpoints:")
    print("-" * 40)
    
    try:
        from nba_api.live.nba.endpoints import scoreboard
        print("✅ live scoreboard - Available")
        return True
    except ImportError:
        print("❌ Live endpoints not available")
        return False

def generate_working_collector_recommendations(available_endpoints):
    """Generate recommendations for which collector to use"""
    print("\n🎯 RECOMMENDATIONS:")
    print("=" * 50)
    
    basic_count = len([e for e in ['scoreboardv2', 'boxscoretraditionalv2', 'boxscoreadvancedv2'] 
                      if e in available_endpoints])
    
    if basic_count >= 3:
        print("✅ Use: nba_daily_collector.py (original)")
        print("   All basic endpoints available")
    
    enhanced_endpoints = ['boxscoremiscv2', 'boxscoreusagev2', 'boxscorescoringv2', 
                         'boxscoreplayertrackv2']
    enhanced_count = len([e for e in enhanced_endpoints if e in available_endpoints])
    
    if enhanced_count >= 2:
        print("✅ Use: nba_enhanced_collector_corrected.py")
        print(f"   {enhanced_count}/{len(enhanced_endpoints)} enhanced endpoints available")
    
    if basic_count < 3:
        print("⚠️  Limited functionality - basic endpoints missing")
        print("   Try upgrading NBA_API: pip install --upgrade nba_api")

def main():
    """Main testing function"""
    print("🏀 NBA API Endpoint Availability Test")
    print("=" * 50)
    
    # Test NBA API availability
    if not test_nba_api_availability():
        return
    
    # Test different endpoint categories
    basic_available = test_basic_endpoints()
    enhanced_available = test_enhanced_endpoints()
    static_available = test_static_data()
    live_available = test_live_endpoints()
    
    # Combine all available endpoints
    all_available = {**basic_available, **enhanced_available}
    
    # Generate recommendations
    generate_working_collector_recommendations(all_available)
    
    print(f"\n📊 SUMMARY:")
    print(f"   Basic endpoints: {len(basic_available)}/3")
    print(f"   Enhanced endpoints: {len(enhanced_available)}/11")
    print(f"   Total available: {len(all_available)}")
    
    if len(basic_available) >= 3:
        print("\n🚀 You're ready to collect NBA data!")
        print("   Start with: python nba_daily_collector.py --yesterday")
    else:
        print("\n⚠️  Some issues found - check NBA_API installation")

if __name__ == "__main__":
    main()