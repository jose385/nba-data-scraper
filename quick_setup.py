#!/usr/bin/env python3
"""
Quick Setup and Examples for NBA Daily Data Collector

This script provides easy setup and common usage examples for collecting NBA 2024-25 season data.
Perfect for building your prediction model with flexible, day-by-day data collection.
"""

import subprocess
import sys
import os
from datetime import datetime, timedelta

def install_requirements():
    """Install required packages"""
    print("🔧 Installing NBA data collection requirements...")
    
    requirements = [
        "nba_api>=1.2.1",
        "pandas>=2.0.0", 
        "requests>=2.31.0",
        "numpy>=1.24.0"
    ]
    
    try:
        for package in requirements:
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", package
            ])
        print("✅ All requirements installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install requirements: {e}")
        return False

def check_nba_api():
    """Test NBA API connection"""
    try:
        from nba_api.stats.endpoints import scoreboardv2
        from datetime import datetime
        
        # Test with a recent date
        test_date = (datetime.now() - timedelta(days=1)).strftime('%m/%d/%Y')
        scoreboard = scoreboardv2.ScoreboardV2(game_date=test_date)
        games = scoreboard.get_data_frames()[0]
        
        print(f"✅ NBA API working! Found {len(games)} games for test date")
        return True
        
    except Exception as e:
        print(f"❌ NBA API test failed: {e}")
        return False

def run_example_collection():
    """Run example data collection"""
    print("\n🎯 Running example data collection...")
    print("Collecting yesterday's NBA games...")
    
    try:
        # Run the daily collector for yesterday
        result = subprocess.run([
            sys.executable, "nba_daily_collector.py", "--yesterday"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Example collection completed!")
            print("Check the 'nba_2024_25_data' folder for your data files")
        else:
            print(f"⚠️ Collection completed with warnings:")
            print(result.stdout)
            
    except Exception as e:
        print(f"❌ Example collection failed: {e}")

def show_usage_examples():
    """Display common usage examples"""
    print("\n📚 COMMON USAGE EXAMPLES:")
    print("=" * 50)
    
    examples = [
        ("📅 Single Day", "python nba_daily_collector.py --date 2024-10-20"),
        ("🕐 Yesterday's Games", "python nba_daily_collector.py --yesterday"),
        ("📊 Today's Games", "python nba_daily_collector.py --today"),
        ("📈 Last Week", "python nba_daily_collector.py --last-week"),
        ("📋 Date Range", "python nba_daily_collector.py --start 2024-10-15 --end 2024-10-25"),
        ("🔄 Catch Up Missing", "python nba_daily_collector.py --catch-up --start 2024-10-01"),
        ("💰 With Betting Odds", "python nba_daily_collector.py --yesterday --odds-api-key YOUR_KEY")
    ]
    
    for title, command in examples:
        print(f"\n{title}:")
        print(f"  {command}")
    
    print("\n" + "=" * 50)

def show_output_structure():
    """Show expected output structure"""
    print("\n📁 OUTPUT STRUCTURE:")
    print("=" * 30)
    print("""
nba_2024_25_data/
├── combined_daily/           # 🎯 MAIN FILES (LLM-ready)
│   ├── nba_complete_2024-10-20.csv
│   ├── nba_complete_2024-10-21.csv
│   └── ...
├── daily_games/             # Individual data types
│   ├── games_2024-10-20.csv
│   ├── player_traditional_2024-10-20.csv
│   └── team_advanced_2024-10-20.csv
├── odds_data/              # Betting odds (if API key provided)
│   └── odds_2024-10-20.csv
└── collection_summary_*.json # Collection reports
    """)
    
    print("🎯 USE THE combined_daily/ FILES FOR YOUR LLM!")
    print("   Each file contains ALL data for that day's games.")

def main():
    """Main setup function"""
    print("🏀 NBA 2024-25 Daily Data Collector - Quick Setup")
    print("=" * 60)
    
    # Step 1: Install requirements
    if not install_requirements():
        print("Please fix installation issues and try again.")
        return
    
    # Step 2: Test NBA API
    if not check_nba_api():
        print("Please check your internet connection and try again.")
        return
    
    # Step 3: Show usage examples
    show_usage_examples()
    show_output_structure()
    
    # Step 4: Offer to run example
    print("\n🚀 QUICK START OPTIONS:")
    print("1. Run example collection (yesterday's games)")
    print("2. Skip to manual usage")
    
    choice = input("\nEnter choice (1 or 2): ").strip()
    
    if choice == "1":
        run_example_collection()
        
        print("\n🎉 SETUP COMPLETE!")
        print("\n📝 NEXT STEPS:")
        print("1. Check the 'nba_2024_25_data' folder for your data")
        print("2. Use combined_daily/*.csv files for your LLM model")
        print("3. Run daily collections to keep your data current")
        print("4. Use --catch-up to fill any missing days")
        
    else:
        print("\n✅ Setup complete! Use the examples above to start collecting data.")
    
    print(f"\n📖 For full documentation, see the README.md file")

if __name__ == "__main__":
    main()