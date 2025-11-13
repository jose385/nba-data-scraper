#!/usr/bin/env python3
"""
Test NBA API connectivity with new IP
"""

import requests
from datetime import datetime

def test_ip_change():
    """Test if IP change resolved NBA API issues"""
    
    print("🔍 Testing IP change effectiveness...")
    
    # Check current IP
    try:
        ip_response = requests.get('https://ifconfig.me', timeout=10)
        current_ip = ip_response.text.strip()
        print(f"📍 Current IP: {current_ip}")
    except:
        print("❌ Could not get current IP")
        return False
    
    # Test basic NBA API endpoint
    try:
        print("🏀 Testing NBA API connectivity...")
        
        # Use a simple NBA stats endpoint
        url = "https://stats.nba.com/stats/leaguegamefinder"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.nba.com/',
            'Origin': 'https://www.nba.com'
        }
        params = {
            'Season': '2024-25',
            'SeasonType': 'Regular Season',
            'PlayerOrTeam': 'T'
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            print("✅ NBA API working! IP change successful!")
            return True
        elif response.status_code == 429:
            print("⚠️ Still getting rate limited - may need to wait longer")
            return False
        elif response.status_code in [403, 451, 503]:
            print("❌ Still blocked - try different IP method")
            return False
        else:
            print(f"❓ Unexpected response: {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Still timing out - IP may still be blocked")
        return False
    except Exception as e:
        print(f"❌ NBA API test failed: {e}")
        return False

if __name__ == "__main__":
    test_ip_change()