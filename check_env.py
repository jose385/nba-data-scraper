#!/usr/bin/env python3
"""
Simple test to check if API key is being loaded
"""
import os

# Try loading with dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ dotenv loaded")
except ImportError:
    print("⚠️ dotenv not installed (this is OK)")

# Check for API key
api_key = os.getenv('BALLDONTLIE_API_KEY')

print("\n🔍 Environment Variable Check:")
print("=" * 50)

if api_key:
    print(f"✅ BALLDONTLIE_API_KEY found!")
    print(f"   Length: {len(api_key)} characters")
    print(f"   Preview: {api_key[:10]}...{api_key[-4:]}")
else:
    print("❌ BALLDONTLIE_API_KEY not found!")
    print("\n💡 To fix:")
    print("   1. Check .env file exists: ls -la .env")
    print("   2. Check contents: cat .env")
    print("   3. Or export directly: export BALLDONTLIE_API_KEY=your_key")

# Show all env vars that start with BALL
print("\n📋 All BALL* environment variables:")
for key, value in os.environ.items():
    if key.startswith('BALL'):
        print(f"   {key}: {value[:10]}...{value[-4:] if len(value) > 10 else value}")

if not any(key.startswith('BALL') for key in os.environ.keys()):
    print("   (none found)")