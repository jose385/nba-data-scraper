#!/usr/bin/env python3
"""
NBA Environment Validation
Comprehensive check of NBA betting analysis setup
"""
import sys
import os
from pathlib import Path

def check_dependencies():
    """Check required Python dependencies"""
    print("📦 Checking NBA Dependencies...")
    
    required_packages = {
        'pandas': 'Data manipulation',
        'numpy': 'Numerical computing', 
        'pyarrow': 'Parquet file support',
        'psycopg2': 'PostgreSQL connection',
        'requests': 'HTTP requests',
        'nba_api': 'NBA data collection'
    }
    
    missing = []
    available = []
    
    for package, description in required_packages.items():
        try:
            __import__(package)
            available.append(f"{package} - {description}")
        except ImportError:
            missing.append(f"{package} - {description}")
    
    for pkg in available:
        print(f"   ✅ {pkg}")
    
    for pkg in missing:
        print(f"   ❌ {pkg}")
    
    if missing:
        print(f"\n💡 Install missing packages:")
        print(f"   pip install {' '.join([pkg.split(' - ')[0] for pkg in missing])}")
        return False
    
    return True

def check_nba_config():
    """Check NBA configuration"""
    print("\n⚙️ Checking NBA Configuration...")
    
    try:
        from py.nba_config import get_nba_config
        config = get_nba_config()
        
        # Data mode
        mode = "PLACEHOLDER" if config.USE_PLACEHOLDER_DATA else "REAL API"
        print(f"   📊 Data Mode: {mode}")
        
        # Database
        if config.PG_DSN:
            print(f"   ✅ Database configured")
            
            # Test connection
            success, message = config.test_database_connection()
            if success:
                print(f"   ✅ Database connection: {message}")
            else:
                print(f"   ❌ Database connection: {message}")
        else:
            print(f"   ❌ Database not configured")
        
        # NBA API
        api_success, api_message = config.test_nba_api_connection()
        if api_success:
            print(f"   ✅ NBA API: {api_message}")
        else:
            print(f"   ❌ NBA API: {api_message}")
        
        # Features
        features = config.get_enabled_features()
        print(f"   🔧 Enabled features: {len(features)}")
        for feature in features:
            print(f"      • {feature}")
        
        return True
        
    except ImportError as e:
        print(f"   ❌ NBA configuration not available: {e}")
        return False
    except Exception as e:
        print(f"   ❌ Configuration error: {e}")
        return False

def check_directories():
    """Check required directories"""
    print("\n📁 Checking NBA Directories...")
    
    required_dirs = [
        'py', 'stage', 'logs', 'migrations'
    ]
    
    missing_dirs = []
    
    for directory in required_dirs:
        dir_path = Path(directory)
        if dir_path.exists() and dir_path.is_dir():
            print(f"   ✅ {directory}/")
        else:
            print(f"   ❌ {directory}/")
            missing_dirs.append(directory)
    
    if missing_dirs:
        print(f"\n💡 Create missing directories:")
        print(f"   python setup_nba_directories.py")
        return False
    
    return True

def check_environment_file():
    """Check .env file"""
    print("\n🔧 Checking Environment File...")
    
    env_file = Path('.env')
    template_file = Path('.env.nba.template')
    
    if env_file.exists():
        print(f"   ✅ .env file exists")
        
        # Check for NBA-specific variables
        with open(env_file) as f:
            content = f.read()
            
        nba_vars = ['NBA_PG_DSN', 'NBA_API_DELAY', 'USE_PLACEHOLDER_DATA']
        missing_vars = []
        
        for var in nba_vars:
            if var in content or var.replace('NBA_', '') in content:
                print(f"   ✅ {var} configured")
            else:
                print(f"   ⚠️ {var} not found")
                missing_vars.append(var)
        
        return len(missing_vars) == 0
        
    else:
        print(f"   ❌ .env file not found")
        if template_file.exists():
            print(f"   💡 Copy template: cp .env.nba.template .env")
        else:
            print(f"   💡 Create .env file with your settings")
        return False

def main():
    """Run complete NBA environment check"""
    print("🏀 NBA Environment Validation")
    print("=" * 50)
    
    checks = [
        ("Dependencies", check_dependencies),
        ("Directories", check_directories), 
        ("Environment File", check_environment_file),
        ("NBA Configuration", check_nba_config)
    ]
    
    passed = 0
    failed = 0
    
    for check_name, check_func in checks:
        print(f"\n{'='*50}")
        try:
            if check_func():
                passed += 1
                print(f"✅ {check_name}: PASSED")
            else:
                failed += 1
                print(f"❌ {check_name}: FAILED")
        except Exception as e:
            failed += 1
            print(f"❌ {check_name}: ERROR - {e}")
    
    # Summary
    print(f"\n{'='*50}")
    print(f"🏀 NBA Environment Check Results:")
    print(f"   ✅ Passed: {passed}")
    print(f"   ❌ Failed: {failed}")
    
    if failed == 0:
        print(f"\n🎉 NBA environment is ready!")
        print(f"\n🎯 Ready to run:")
        print(f"   python py/nba_enhanced_backfill.py --start 2024-01-15 --end 2024-01-15 --placeholder")
    else:
        print(f"\n⚠️ Fix the issues above before proceeding")
        
    return failed == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)