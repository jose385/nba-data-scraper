# NBA Data Scraper

Comprehensive NBA data collection system using BallDontLie API (GOAT tier). Collects games, box scores, player stats, advanced metrics, and possession/stint data for betting analysis and prediction models.

---

## Quick Start

```bash
# 1. Install dependencies
pip install requests pandas
# Optional: pip install pyarrow  (for parquet files from other scripts)

# 2. Set API key
export BALLDONTLIE_API_KEY="your-api-key"

# 3. Run collection
python py/nba_balldontlie_backfill.py --start 2025-10-22 --end 2025-10-31
```

---

## Scripts Overview

| Script | Purpose | Data Collected |
|--------|---------|----------------|
| `py/nba_balldontlie_client.py` | API client | Core BDL API wrapper |
| `py/nba_balldontlie_backfill.py` | Main collector | Games, box scores, teams |
| `py/premium_nba_collection.py` | Premium collector | Advanced stats, standings, injuries |
| `nba_bdl_possessions.py` | Stints/possessions | Play-by-play derived data |

---

## 1. Main Data Collection (`nba_balldontlie_backfill.py`)

Collects core NBA data: games, box scores, and team information.

### Basic Usage

```bash
# Collect games for a date range
python py/nba_balldontlie_backfill.py \
    --start 2025-10-22 \
    --end 2025-11-30

# Collect single day
python py/nba_balldontlie_backfill.py \
    --start 2025-11-15 \
    --end 2025-11-15

# Full collection (teams + games + box scores)
python py/nba_balldontlie_backfill.py \
    --start 2025-10-22 \
    --end 2025-12-31 \
    --full

# Collect only teams
python py/nba_balldontlie_backfill.py --teams-only

# Specify output directory
python py/nba_balldontlie_backfill.py \
    --start 2025-11-01 \
    --end 2025-11-30 \
    --output ./data
```

### Output Files

```
stage/
├── nba_teams.parquet
├── nba_games_2025-11-15.parquet
├── nba_box_scores_2025-11-15.parquet
└── collection_summary_YYYY-MM-DD.json
```

---

## 2. Premium Collection (`premium_nba_collection.py`)

Collects premium/GOAT tier data including advanced stats, standings, and injuries.

### Basic Usage

```bash
# Standard premium collection
python premium_nba_collection.py \
    --start 2025-11-01 \
    --end 2025-11-15

# Betting-focused collection (includes odds if available)
python premium_nba_collection.py \
    --start 2025-11-15 \
    --end 2025-11-15 \
    --betting-focus

# Full premium collection
python premium_nba_collection.py \
    --start 2025-10-22 \
    --end 2025-12-31 \
    --full
```

### Premium Data Endpoints

| Endpoint | Description | Availability |
|----------|-------------|--------------|
| Teams | All NBA teams | ✅ All tiers |
| Players | Player info | ✅ All tiers |
| Games | Schedule & scores | ✅ All tiers |
| Box Scores | Player game stats | ✅ All tiers |
| Advanced Stats | PER, TS%, etc. | ✅ GOAT tier |
| Standings | Conference/division | ✅ GOAT tier |
| Player Injuries | Injury reports | ✅ GOAT tier |
| Season Averages | Player season stats | ✅ GOAT tier |
| League Leaders | Statistical leaders | ✅ GOAT tier |
| Play-by-Play | Game events | ✅ GOAT tier (2025+) |
| Lineups | Starting lineups | ✅ GOAT tier (2025+) |
| Betting Odds | Sportsbook lines | ✅ GOAT tier (2025+) |

---

## 3. Stints/Possessions Collection (`nba_bdl_possessions.py`)

Computes possession and stint data from play-by-play for advanced analytics.

### Basic Usage

```bash
# Full season backfill
python nba_bdl_possessions.py backfill --season 2025

# Yesterday's games
python nba_bdl_possessions.py daily

# Today's games  
python nba_bdl_possessions.py today
```

### Filter by Dates

```bash
# Date range
python nba_bdl_possessions.py backfill \
    --start-date 2026-01-01 \
    --end-date 2026-01-15

# Specific dates
python nba_bdl_possessions.py backfill \
    --dates 2026-01-15 2026-01-16 2026-01-17

# Single date
python nba_bdl_possessions.py backfill --dates 2026-01-20
```

### Filter by Teams

```bash
# Single team (all their games)
python nba_bdl_possessions.py backfill --teams LAL

# Multiple teams
python nba_bdl_possessions.py backfill --teams LAL BOS GSW

# Team games in date range
python nba_bdl_possessions.py backfill \
    --teams LAL \
    --start-date 2026-01-01 \
    --end-date 2026-01-31
```

### Combined Filters

```bash
# Lakers and Celtics games on specific dates
python nba_bdl_possessions.py backfill \
    --teams LAL BOS \
    --dates 2026-01-15 2026-01-20

# Warriors January games
python nba_bdl_possessions.py backfill \
    --teams GSW \
    --start-date 2026-01-01 \
    --end-date 2026-01-31

# Today's Lakers game
python nba_bdl_possessions.py today --teams LAL

# Yesterday's games for multiple teams
python nba_bdl_possessions.py daily --teams LAL BOS GSW
```

### Output Type

```bash
# Stints (default) - segments between substitutions
python nba_bdl_possessions.py backfill --output stints

# Possessions - individual possession tracking
python nba_bdl_possessions.py backfill --output possessions
```

### Other Options

```bash
# Re-process existing games (don't skip)
python nba_bdl_possessions.py backfill --no-skip

# Verbose logging
python nba_bdl_possessions.py backfill --verbose

# Specify season
python nba_bdl_possessions.py backfill --season 2024
```

### Stints Output Schema

| Column | Type | Description |
|--------|------|-------------|
| game_id | int | BallDontLie game ID |
| stint_num | int | Sequential stint number |
| period | int | Period (1-4, 5+ for OT) |
| start_clock | str | Start time (MM:SS) |
| end_clock | str | End time (MM:SS) |
| possessions | int | Number of possessions |
| home_points | int | Home team points |
| away_points | int | Away team points |
| start_margin | int | Score differential at start |
| end_margin | int | Score differential at end |

---

## 4. API Client (`nba_balldontlie_client.py`)

Low-level API client for custom data collection.

### Usage in Python

```python
from py.nba_balldontlie_client import create_client

# Initialize client
client = create_client()  # Uses BALLDONTLIE_API_KEY env var

# Get teams
teams = client.get_teams()

# Get games for date range
games = client.get_games(
    start_date="2025-11-01",
    end_date="2025-11-15",
    seasons=[2025]
)

# Get box scores for a game
stats = client.get_game_stats(game_id=18446850)

# Get season averages
averages = client.get_season_averages(
    season=2025,
    player_ids=[1, 2, 3]
)

# Get standings
standings = client.get_standings(season=2025)

# Get player injuries
injuries = client.get_injuries()

# Get advanced stats
advanced = client.get_advanced_stats(
    season=2025,
    player_ids=[1, 2, 3]
)
```

---

## Team Abbreviations

| Team | Code | Team | Code |
|------|------|------|------|
| Atlanta Hawks | ATL | Milwaukee Bucks | MIL |
| Boston Celtics | BOS | Minnesota Timberwolves | MIN |
| Brooklyn Nets | BKN | New Orleans Pelicans | NOP |
| Charlotte Hornets | CHA | New York Knicks | NYK |
| Chicago Bulls | CHI | Oklahoma City Thunder | OKC |
| Cleveland Cavaliers | CLE | Orlando Magic | ORL |
| Dallas Mavericks | DAL | Philadelphia 76ers | PHI |
| Denver Nuggets | DEN | Phoenix Suns | PHX |
| Detroit Pistons | DET | Portland Trail Blazers | POR |
| Golden State Warriors | GSW | Sacramento Kings | SAC |
| Houston Rockets | HOU | San Antonio Spurs | SAS |
| Indiana Pacers | IND | Toronto Raptors | TOR |
| LA Clippers | LAC | Utah Jazz | UTA |
| Los Angeles Lakers | LAL | Washington Wizards | WAS |
| Memphis Grizzlies | MEM | Miami Heat | MIA |

---

## Directory Structure

```
nba-data-scraper/
├── py/
│   ├── nba_balldontlie_client.py    # API client
│   ├── nba_balldontlie_backfill.py  # Main collector
│   └── nba_config.py                # Configuration
├── nba_bdl_possessions.py           # Stints/possessions collector
├── premium_nba_collection.py        # Premium data collector
├── loader/
│   └── nba_load_data.py             # PostgreSQL loader
├── migrations/
│   └── 001_nba_schema.sql           # Database schema
├── stage/                           # Daily data output
│   ├── nba_games_YYYY-MM-DD.parquet
│   └── nba_box_scores_YYYY-MM-DD.parquet
├── nba_stints_data/                 # Stints output
│   ├── stints/
│   │   └── YYYY-MM_stints.csv
│   └── logs/
├── .env                             # API key (not in git)
├── requirements.txt
└── README.md
```

---

## Common Workflows

### Daily Data Collection (Cron)

```bash
# Add to crontab - runs at 6 AM daily
0 6 * * * cd /path/to/nba-data-scraper && \
    python py/nba_balldontlie_backfill.py --start $(date -d "yesterday" +%Y-%m-%d) --end $(date -d "yesterday" +%Y-%m-%d) && \
    python nba_bdl_possessions.py daily \
    >> /var/log/nba_collector.log 2>&1
```

### Backfill Full Season

```bash
# Collect all 2025-26 season data
python py/nba_balldontlie_backfill.py \
    --start 2025-10-22 \
    --end 2026-04-15 \
    --full

python nba_bdl_possessions.py backfill --season 2025
```

### Pre-Game Data Collection

```bash
# Collect today's matchup data for betting analysis
python nba_bdl_possessions.py backfill --teams LAL BOS --season 2025
python premium_nba_collection.py --start $(date +%Y-%m-%d) --end $(date +%Y-%m-%d) --betting-focus
```

---

## Reading Data in Python

```python
import pandas as pd
from pathlib import Path

# Load games
games = pd.read_parquet("stage/nba_games_2025-11-15.parquet")

# Load box scores
box_scores = pd.read_parquet("stage/nba_box_scores_2025-11-15.parquet")

# Load all stints (CSV files)
stints_dir = Path("nba_stints_data/stints")
all_stints = pd.concat([
    pd.read_csv(f) for f in stints_dir.glob("*_stints.csv")
])

# Filter to specific team
lakers_games = games[
    (games["home_team_abbrev"] == "LAL") | 
    (games["away_team_abbrev"] == "LAL")
]

# Join box scores with games
merged = box_scores.merge(games, on="game_id")
```

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BALLDONTLIE_API_KEY` | Yes | BallDontLie API key |
| `DATABASE_URL` | No | PostgreSQL connection string |

---

## Troubleshooting

### "API key required"
```bash
export BALLDONTLIE_API_KEY="your-key-here"
```

### 404 errors on play-by-play
- Play-by-play requires GOAT tier
- Only available for 2025+ season games
- Game must be completed (Final status)

### Rate limiting
- Free tier: 30 requests/minute
- GOAT tier: 600 requests/minute
- Scripts include automatic delays

### No games found
- Check date format: `YYYY-MM-DD`
- Verify season matches dates
- NBA season runs October to April

---

## API Tiers

| Feature | Free | ALL-STAR | GOAT |
|---------|------|----------|------|
| Rate Limit | 30/min | 300/min | 600/min |
| Teams/Players/Games | ✅ | ✅ | ✅ |
| Box Scores | ✅ | ✅ | ✅ |
| Season Averages | ❌ | ✅ | ✅ |
| Advanced Stats | ❌ | ✅ | ✅ |
| Standings | ❌ | ✅ | ✅ |
| Injuries | ❌ | ✅ | ✅ |
| Play-by-Play | ❌ | ❌ | ✅ |
| Lineups | ❌ | ❌ | ✅ |
| Betting Odds | ❌ | ❌ | ✅ |

---

## License

MIT