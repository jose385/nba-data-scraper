#!/usr/bin/env python3
"""
Test NBA Backfill System
Validates NBA data collection pipeline
"""
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

def setup_test_environment():
    """Setup test environment"""
    print("🏀 NBA Backfill Test Setup")
    print("=" * 40)
    
    # Check if .env exists
    env_file = Path('.env')
    template_file = Path('.env.nba.template')
    
    if not env_file.exists():
        if template_file.exists():
            print("📝 Creating .env from template...")
            import shutil
            shutil.copy(template_file, env_file)
            print("✅ .env created - please configure your database settings")
        else:
            print("❌ No .env file found and no template available")
            return False
    
    # Load environment
    try:
        from py.nba_config import get_nba_config
        config = get_nba_config()
        print("✅ NBA configuration loaded")
        return True
    except ImportError:
        print("❌ NBA configuration not available")
        return False

def test_placeholder_backfill():
    """Test placeholder data generation"""
    print("\n🔧 Testing Placeholder Mode...")
    
    # Use a recent date for testing
    test_date = datetime.now() - timedelta(days=7)
    date_str = test_date.strftime('%Y-%m-%d')
    
    # Run backfill with placeholder data
    cmd = [
        sys.executable,
        'py/nba_enhanced_backfill.py',
        '--start', date_str,
        '--end', date_str,
        '--placeholder',
        '--out-dir', 'test_output'
    ]
    
    print(f"🚀 Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print("✅ Placeholder backfill successful")
            
            # Check output files
            output_dir = Path('test_output')
            if output_dir.exists():
                files = list(output_dir.glob('*.parquet'))
                print(f"📁 Generated {len(files)} files:")
                for file in files:
                    size_kb = file.stat().st_size / 1024
                    print(f"   • {file.name} ({size_kb:.1f} KB)")
                
                # Validate one file
                if files:
                    sample_file = files[0]
                    try:
                        df = pd.read_parquet(sample_file)
                        print(f"📊 Sample file validation:")
                        print(f"   Rows: {len(df)}")
                        print(f"   Columns: {len(df.columns)}")
                        print(f"   Sample columns: {list(df.columns[:5])}")
                        return True
                    except Exception as e:
                        print(f"❌ Failed to read sample file: {e}")
                        return False
            else:
                print("❌ No output directory created")
                return False
        else:
            print(f"❌ Backfill failed:")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Backfill timed out after 60 seconds")
        return False
    except Exception as e:
        print(f"❌ Backfill error: {e}")
        return False

def test_real_data_backfill():
    """Test real NBA API data collection"""
    print("\n📡 Testing Real Data Mode...")
    
    # Check if NBA API is available
    try:
        from nba_api.stats.static import teams
        team_list = teams.get_teams()
        if not team_list or len(team_list) != 30:
            print("❌ NBA API not working properly")
            return False
        print("✅ NBA API connectivity confirmed")
    except ImportError:
        print("⚠️ nba_api not installed - skipping real data test")
        print("   Install with: pip install nba_api")
        return True  # Not a failure, just skipped
    except Exception as e:
        print(f"⚠️ NBA API error: {e} - skipping real data test")
        return True  # Not a failure, just skipped
    
    # Use a date that definitely has NBA games (regular season)
    test_date = datetime(2024, 1, 15)  # Middle of season
    date_str = test_date.strftime('%Y-%m-%d')
    
    cmd = [
        sys.executable, 
        'py/nba_enhanced_backfill.py',
        '--start', date_str,
        '--end', date_str,
        '--real-data',
        '--minimal',  # Only essential data to reduce API calls
        '--out-dir', 'test_real_output'
    ]
    
    print(f"🚀 Running: {' '.join(cmd)}")
    print("⚠️ This will make real NBA API calls...")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0:
            print("✅ Real data backfill successful")
            
            # Check output
            output_dir = Path('test_real_output')
            if output_dir.exists():
                files = list(output_dir.glob('*.parquet'))
                print(f"📁 Generated {len(files)} real data files")
                return True
            else:
                print("❌ No real data output generated")
                return False
        else:
            print(f"⚠️ Real data backfill failed (may be expected):")
            print(f"STDOUT: {result.stdout[-500:]}")  # Last 500 chars
            print(f"STDERR: {result.stderr[-500:]}")
            return True  # Don't fail test for API issues
            
    except subprocess.TimeoutExpired:
        print("❌ Real data backfill timed out after 120 seconds")
        return False
    except Exception as e:
        print(f"❌ Real data backfill error: {e}")
        return False

def cleanup_test_files():
    """Clean up test output files"""
    print("\n🧹 Cleaning up test files...")
    
    test_dirs = ['test_output', 'test_real_output']
    
    for test_dir in test_dirs:
        dir_path = Path(test_dir)
        if dir_path.exists():
            import shutil
            shutil.rmtree(dir_path)
            print(f"   🗑️ Removed {test_dir}")

def run_all_tests():
    """Run complete test suite"""
    print("🏀 NBA Backfill Test Suite")
    print("=" * 50)
    
    tests_passed = 0
    tests_failed = 0
    
    # Setup
    if not setup_test_environment():
        print("❌ Test setup failed")
        return False
    
    # Test 1: Placeholder data
    print("\n" + "="*50)
    if test_placeholder_backfill():
        tests_passed += 1
        print("✅ Placeholder test PASSED")
    else:
        tests_failed += 1
        print("❌ Placeholder test FAILED")
    
    # Test 2: Real data (optional)
    print("\n" + "="*50)
    if test_real_data_backfill():
        tests_passed += 1
        print("✅ Real data test PASSED")
    else:
        tests_failed += 1
        print("❌ Real data test FAILED")
    
    # Summary
    print("\n" + "="*50)
    print(f"🏀 NBA Test Results:")
    print(f"   ✅ Passed: {tests_passed}")
    print(f"   ❌ Failed: {tests_failed}")
    print(f"   📊 Success Rate: {tests_passed}/{tests_passed + tests_failed}")
    
    # Cleanup
    cleanup_test_files()
    
    if tests_failed == 0:
        print("\n🎉 All NBA tests passed! System ready for use.")
        print("\n🎯 Next steps:")
        print("   1. Configure your database in .env")
        print("   2. Run: python py/nba_enhanced_backfill.py --start YYYY-MM-DD --end YYYY-MM-DD")
        print("   3. Start collecting NBA betting data!")
        return True
    else:
        print(f"\n⚠️ {tests_failed} test(s) failed. Please check the errors above.")
        return False
    

def test_real_data_backfill():
    """Test real NBA API data collection with enhanced settings"""
    print("\n📡 Testing Real Data Mode...")
    
    # Check NBA API availability
    try:
        from nba_api.stats.static import teams
        team_list = teams.get_teams()
        if not team_list or len(team_list) != 30:
            print("❌ NBA API not working properly")
            return False
        print("✅ NBA API connectivity confirmed")
    except ImportError:
        print("⚠️ nba_api not installed - skipping real data test")
        return True
    except Exception as e:
        print(f"⚠️ NBA API error: {e} - skipping real data test")
        return True
    
    # Use a date during regular season with games
    test_date = datetime(2024, 1, 15)  # Middle of season
    date_str = test_date.strftime('%Y-%m-%d')
    
    cmd = [
        sys.executable, 
        'py/nba_enhanced_backfill.py',
        '--start', date_str,
        '--end', date_str,
        '--real-data',
        '--minimal',
        '--out-dir', 'test_real_output'
    ]
    
    print(f"🚀 Running: {' '.join(cmd)}")
    print("⚠️ Using enhanced NBA API settings (longer timeouts)...")
    
    try:
        # Increased timeout to 180 seconds for real NBA API
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        
        if result.returncode == 0:
            print("✅ Real data backfill successful")
            
            output_dir = Path('test_real_output')
            if output_dir.exists():
                files = list(output_dir.glob('*.parquet'))
                print(f"📁 Generated {len(files)} real data files")
                
                # Show file details
                for file in files:
                    size_kb = file.stat().st_size / 1024
                    print(f"   • {file.name} ({size_kb:.1f} KB)")
                
                return True
            else:
                print("❌ No real data output generated")
                return False
        else:
            print(f"⚠️ Real data backfill failed:")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Real data backfill timed out after 180 seconds")
        print("💡 NBA API may be slow - this is normal, try again later")
        return False
    except Exception as e:
        print(f"❌ Real data backfill error: {e}")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)