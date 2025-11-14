#!/usr/bin/env python3
"""
Quick test to diagnose BallDontLie API key format
"""

import os
import requests

# Load API key
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

api_key = os.getenv('BALLDONTLIE_API_KEY')

print("🔍 BallDontLie API Key Diagnostic")
print("=" * 50)

if not api_key:
    print("❌ No API key found!")
    print("   Set it with: export BALLDONTLIE_API_KEY=your_key")
    print("   Or add to .env file")
    exit(1)

print(f"✅ API Key found: {api_key[:10]}...{api_key[-4:]}")
print()

# Test different header formats
test_formats = [
    ("No prefix", api_key),
    ("Bearer prefix", f"Bearer {api_key}"),
    ("Token prefix", f"Token {api_key}"),
]

url = "https://api.balldontlie.io/v1/teams"

for format_name, auth_value in test_formats:
    print(f"Testing: {format_name}")
    print(f"   Authorization: {auth_value[:20]}...")
    
    headers = {
        'Accept': 'application/json',
        'Authorization': auth_value
    }
    
    try:
        response = requests.get(url, headers=headers, params={'page': 1, 'per_page': 1}, timeout=10)
        print(f"   Status: {response.status_code}")
        
        if response.status_code == 200:
            print(f"   ✅ SUCCESS! This format works!")
            data = response.json()
            if 'data' in data and len(data['data']) > 0:
                print(f"   Team: {data['data'][0]['full_name']}")
            print()
            print("🎉 Found working format!")
            print(f"   Use: Authorization: {format_name}")
            break
        elif response.status_code == 401:
            print(f"   ❌ Unauthorized (401)")
        else:
            print(f"   ⚠️ Other error: {response.text[:100]}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print()

print("\n💡 If all failed:")
print("   1. Check your API key at https://www.balldontlie.io/")
print("   2. Make sure it's copied correctly")
print("   3. Try regenerating a new key")