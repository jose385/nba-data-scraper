#!/usr/bin/env python3
"""
Debug ESPN NBA API Response Structure
"""

import requests
import json
from py.nba_enhanced_backfill import ESPNNBADataCollector

def debug_espn_game_details(game_id):
    """Debug what ESPN actually returns for game details"""
    
    collector = ESPNNBADataCollector()
    data = collector.get_game_details(game_id)
    
    print(f"=== ESPN DEBUG for Game {game_id} ===")
    
    if data:
        print(f"SUCCESS: Got data from ESPN")
        print(f"Top-level keys: {list(data.keys())}")
        
        if 'boxscore' in data:
            boxscore = data['boxscore']
            print(f"Boxscore keys: {list(boxscore.keys())}")
            
            if 'teams' in boxscore:
                print(f"Number of teams: {len(boxscore['teams'])}")
                
                for i, team in enumerate(boxscore['teams']):
                    print(f"\nTeam {i}:")
                    print(f"  Team keys: {list(team.keys())}")
                    
                    if 'statistics' in team:
                        print(f"  Statistics categories: {len(team['statistics'])}")
                        for j, stat in enumerate(team['statistics']):
                            stat_name = stat.get('name', 'unknown')
                            num_athletes = len(stat.get('athletes', []))
                            print(f"    {j}: {stat_name} ({num_athletes} athletes)")
                            
                            # Show first athlete structure if available
                            if stat.get('athletes'):
                                first_athlete = stat['athletes'][0]
                                print(f"      Sample athlete keys: {list(first_athlete.keys())}")
                                if 'stats' in first_athlete:
                                    print(f"      Sample stats: {first_athlete['stats'][:5]}...")
                    else:
                        print(f"  No statistics key in team")
            else:
                print(f"No teams key in boxscore")
        else:
            print(f"No boxscore key in response")
            
        # Save raw response for inspection
        with open(f'espn_debug_{game_id}.json', 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Raw response saved to espn_debug_{game_id}.json")
            
    else:
        print(f"ERROR: No data returned from ESPN")

def debug_espn_scoreboard(date_str):
    """Debug ESPN scoreboard to see what games are available"""
    
    collector = ESPNNBADataCollector()
    data = collector.get_scoreboard_data(date_str)
    
    print(f"=== ESPN SCOREBOARD DEBUG for {date_str} ===")
    
    if data and 'events' in data:
        print(f"Found {len(data['events'])} events")
        
        for i, event in enumerate(data['events'][:3]):  # Show first 3
            print(f"\nEvent {i}:")
            print(f"  ID: {event.get('id')}")
            print(f"  Name: {event.get('name')}")
            print(f"  Status: {event.get('status', {}).get('type', {}).get('description')}")
            
            competitions = event.get('competitions', [])
            if competitions:
                comp = competitions[0]
                competitors = comp.get('competitors', [])
                print(f"  Competitors: {len(competitors)}")
                for comp_team in competitors:
                    team_name = comp_team.get('team', {}).get('abbreviation', 'Unknown')
                    score = comp_team.get('score', 'N/A')
                    print(f"    {team_name}: {score}")
    else:
        print(f"No events found or invalid response")

if __name__ == "__main__":
    # Test with a known game ID from your previous run
    print("Testing ESPN API responses...")
    
    # First test scoreboard for a past date with games
    debug_espn_scoreboard("2024-01-15")
    
    # Then test a specific game (you can replace with actual game ID)
    # debug_espn_game_details("401584721")  # Example game ID
    