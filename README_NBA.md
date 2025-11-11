# NBA Betting Analysis - Quick Start

## Setup

1. **Create directories:**
```bash
   python setup_nba_directories.py
```

2. **Install dependencies:**
```bash
   pip install -r py/nba_requirements.txt
```

3. **Configure environment:**
```bash
   cp .env.nba.template .env
   # Edit .env with your settings
```

4. **Validate setup:**
```bash
   python check_nba_env.py
```

## Test the System

1. **Run test suite:**
```bash
   python test_nba_backfill.py
```

2. **Test placeholder data:**
```bash
   python py/nba_enhanced_backfill.py --start 2024-01-15 --end 2024-01-15 --placeholder
```

3. **Test real data (when ready):**
```bash
   python py/nba_enhanced_backfill.py --start 2024-01-15 --end 2024-01-15 --real-data --minimal
```

## Key Files Created

- `py/nba_enhanced_backfill.py` - Main data collection script
- `py/nba_config.py` - Configuration management  
- `.env` - Your environment settings
- `stage/` - Data output directory

## Claude Integration

Your system collects the "gold mine" NBA tracking data, Claude researches:
- Injury reports and load management
- Starting lineups and rotations
- Referee assignments and tendencies  
- Recent form and matchup analysis