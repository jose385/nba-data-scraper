#!/usr/bin/env python3
"""
NBA Directory Setup - Following MLB Architecture
Creates all necessary directories for NBA betting analysis
"""
import sys
import os
from pathlib import Path
from datetime import datetime

def create_nba_directories():
    """Create all NBA project directories with error handling"""
    
    # NBA-specific directory structure
    directories = {
        # Core data directories (REQUIRED)
        'stage': 'NBA data staging area for parquet files',
        'logs': 'NBA application and error logs',
        
        # Code directories (REQUIRED) 
        'py': 'Main Python package',
        'loader': 'NBA data loading utilities',
        'migrations': 'NBA database schema migrations',
        
        # NBA-specific directories
        'nba_data': 'Raw NBA API data storage',
        'nba_analysis': 'NBA analysis outputs',
        'nba_models': 'NBA prediction models',
        
        # Optional directories (RECOMMENDED)
        'tests': 'Unit and integration tests',
        'docs': 'NBA project documentation', 
        'backup': 'NBA data backup storage',
        '.github/workflows': 'CI/CD workflows for NBA',
        
        # Additional useful directories
        'scripts': 'NBA utility and maintenance scripts',
        'config': 'NBA configuration files and templates'
    }
    
    print("🏀 NBA Betting Analysis - Directory Setup")
    print("=" * 50)
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    created = []
    existed = []
    failed = []
    
    for directory, description in directories.items():
        dir_path = Path(directory)
        
        try:
            if dir_path.exists():
                if dir_path.is_dir():
                    existed.append(directory)
                    print(f"✅ {directory:<20} - {description}")
                else:
                    failed.append(f"{directory} exists but is not a directory")
                    print(f"❌ {directory:<20} - EXISTS BUT NOT A DIRECTORY!")
            else:
                dir_path.mkdir(parents=True, exist_ok=True)
                created.append(directory)
                print(f"📂 {directory:<20} - Created: {description}")
                
        except PermissionError:
            failed.append(f"{directory}: Permission denied")
            print(f"❌ {directory:<20} - PERMISSION DENIED")
        except Exception as e:
            failed.append(f"{directory}: {str(e)}")
            print(f"❌ {directory:<20} - ERROR: {e}")
    
    # Create NBA-specific essential files
    essential_files = {
        'py/__init__.py': '"""NBA Betting Analysis Package"""\n__version__ = "1.0.0"\n',
        'py/nba_config.py': '# NBA configuration will be added here\n',
        'logs/.gitkeep': '# Keep this directory in git\n',
        'stage/.gitkeep': '# Keep this directory in git\n',
        'nba_data/.gitkeep': '# Keep this directory in git\n',
        'tests/__init__.py': '"""NBA Test package"""\n',
        'docs/README.md': '# NBA Betting Analysis Documentation\n\nThis directory contains NBA project documentation.\n'
    }
    
    print(f"\n📄 Creating NBA essential files...")
    files_created = []
    files_existed = []
    files_failed = []
    
    for file_path, content in essential_files.items():
        file_obj = Path(file_path)
        
        if not file_obj.parent.exists():
            continue
            
        try:
            if file_obj.exists():
                files_existed.append(file_path)
                print(f"   ✅ {file_path}")
            else:
                file_obj.write_text(content)
                files_created.append(file_path)
                print(f"   📝 Created: {file_path}")
        except Exception as e:
            files_failed.append(f"{file_path}: {e}")
            print(f"   ❌ Failed to create {file_path}: {e}")
    
    # Summary
    print("\n" + "=" * 50)
    print(f"📊 NBA Directory Setup Summary:")
    print(f"   📂 Created: {len(created)} directories")
    print(f"   ✅ Existed: {len(existed)} directories")
    print(f"   ❌ Failed: {len(failed)} directories")
    
    if failed:
        return False
    
    print(f"\n🏀 Next Steps for NBA System:")
    print(f"   1. Set up environment: python setup_nba_env.py")
    print(f"   2. Install dependencies: pip install -r py/nba_requirements.txt")
    print(f"   3. Initialize database: python initialize_nba_database.py")
    print(f"   4. Test backfill: python py/nba_enhanced_backfill.py --start 2024-01-15 --end 2024-01-15 --placeholder")
    
    print(f"\n✅ NBA directory setup complete!")
    return True

if __name__ == "__main__":
    success = create_nba_directories()
    sys.exit(0 if success else 1)