# NBA Data Scraper

Comprehensive NBA data collection system using BallDontLie API (GOAT tier). Collects games, box scores, player stats, advanced metrics, and possession/stint data for betting analysis and prediction models.

---

## Quick Start

```bash
# 1. Install dependencies
pip install requests pandas pyarrow python-dotenv

# 2. Set API key
export BALLDONTLIE_API_KEY="your-api-key"

# 3. Run collection (V2 - all endpoints)
python py/nba_balldontlie_backfill_v2.py \
    --start 2025-10-22 --end 2026-01-31 \
    --season 2025 --full
```

---

## Scripts Overview

| Script | Purpose | Data Collected |
|--------|---------|----------------|
| `py/nba_balldontlie_backfill_v2.py` | **Primary collector** | Games, stats, advanced stats, odds, props, lineups, PBP |
| `py/nba_balldontlie_client.py` | API client | Core BDL API wrapper |
| `py/nba_balldontlie_backfill.py` | Legacy collector | Games, box scores, teams |
| `py/premium_nba_collection.py` | Premium collector | Advanced stats, standings, injuries |
| `nba_bdl_possessions.py` | Stints/possessions | Play-by-play derived stint data |

---

## 1. Primary Data Collection (`nba_balldontlie_backfill_v2.py`)

The main comprehensive collector. Supports all V1 and V2 BallDontLie endpoints, team filtering, and dual CSV/Parquet output.

### Quick Start

```bash
# Full season backfill (all endpoints)
python py/nba_balldontlie_backfill_v2.py \
    --start 2025-10-22 --end 2026-01-31 \
    --season 2025 --full

# Daily update (games, stats, advanced, odds, standings, injuries)
python py/nba_balldontlie_backfill_v2.py \
    --start 2026-01-31 --end 2026-01-31 \
    --season 2025 --daily

# Single team (much faster)
python py/nba_balldontlie_backfill_v2.py \
    --start 2025-10-22 --end 2026-01-31 \
    --season 2025 --full --team LAL
```

### Modes

| Flag | What it collects |
|------|------------------|
| `--full` | Everything: games, stats, advanced-v2, lineups, PBP, props, odds, standings, injuries, season averages |
| `--daily` | Essentials: games, stats, advanced-v2, odds, standings, injuries |

### Individual Endpoint Flags

```bash
# Pick exactly what you need
python py/nba_balldontlie_backfill_v2.py \
    --start 2026-01-20 --end 2026-01-26 \
    --season 2025 \
    --games --stats --advanced-v2 --odds
```

| Flag | Description | Needs `--games` first |
|------|-------------|----------------------|
| `--games` | Game scores and schedules | — |
| `--stats` | Player box scores | ✅ |
| `--advanced-v2` | 100+ metrics (hustle, tracking, defense) | ✅ |
| `--lineups` | Starting lineups + bench | ✅ |
| `--pbp` | Play-by-play with shot coordinates | ✅ |
| `--player-props` | Live betting prop lines | ✅ |
| `--odds` | Spreads, moneylines, totals from 5 books | ✅ |
| `--standings` | Conference/division standings | — |
| `--injuries` | Current injury reports | — |
| `--leaders` | League stat leaders | — |
| `--season-averages` | Player season stats (15+ categories) | — |
| `--team-season-averages` | Team-level season stats | — |
| `--teams` | Reference team data | — |

### Team Filtering

```bash
# Filter by abbreviation
python py/nba_balldontlie_backfill_v2.py \
    --start 2025-10-22 --end 2026-01-31 \
    --season 2025 --full --team LAL

# Filter by team ID
python py/nba_balldontlie_backfill_v2.py \
    --start 2025-10-22 --end 2026-01-31 \
    --season 2025 --full --team-id 14
```

### Endpoint-Only Collections (No Date Needed)

```bash
# Season averages only
python py/nba_balldontlie_backfill_v2.py \
    --season 2025 --season-averages --team-season-averages

# Standings only
python py/nba_balldontlie_backfill_v2.py \
    --season 2025 --standings

# Current injuries
python py/nba_balldontlie_backfill_v2.py --injuries

# Leaders only
python py/nba_balldontlie_backfill_v2.py --season 2025 --leaders
```

### Historical Backfill

```bash
# 2024-25 Season
python py/nba_balldontlie_backfill_v2.py \
    --start 2024-10-22 --end 2025-06-30 \
    --season 2024 --full

# 2023-24 Season
python py/nba_balldontlie_backfill_v2.py \
    --start 2023-10-24 --end 2024-06-30 \
    --season 2023 --full
```

### What `--full` vs `--daily` Includes

| Endpoint | `--full` | `--daily` |
|----------|----------|-----------|
| games | ✅ | ✅ |
| stats | ✅ | ✅ |
| advanced-v2 | ✅ | ✅ |
| odds | ✅ | ✅ |
| player-props | ✅ | ❌ |
| lineups | ✅ | ❌ |
| pbp | ✅ | ❌ |
| standings | ✅ | ✅ |
| injuries | ✅ | ✅ |
| season-averages | ✅ | ❌ |
| team-season-averages | ✅ | ❌ |

### V2 Output Files

```
data/
├── games/
│   └── games_2025-10-22_2026-01-31.csv
├── player_stats/
│   └── player_stats_2025-10-22_2026-01-31.csv
├── advanced_stats_v2/
│   └── advanced_stats_v2_2025-10-22_2026-01-31.csv
├── lineups/
│   └── lineups_2025-10-22_2026-01-31.csv
├── play_by_play/
│   └── play_by_play_batch.csv
├── odds/
│   └── odds_batch.csv
├── player_props/
│   └── player_props_batch.csv
├── season_averages/
│   └── season_averages_2025.csv
├── team_season_averages/
│   └── team_season_averages_2025.csv
├── standings/
│   └── standings_2025.csv
├── injuries/
│   └── injuries_current.csv
└── leaders/
    └── leaders_2025.csv
```

---

## 2. Legacy Data Collection (`nba_balldontlie_backfill.py`)

Older collector for core data. Use `backfill_v2.py` instead for comprehensive collection.

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

## 3. Premium Collection (`premium_nba_collection.py`)

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

## 4. Stints/Possessions Collection (`nba_bdl_possessions.py`)

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

### Output File Naming

| Filter | Filename Example |
|--------|------------------|
| No filter | `2025-11_stints.csv` |
| Single team | `2025-11_LAL_stints.csv` |
| Multiple teams | `2025-11_BOS_GSW_LAL_stints.csv` |

Teams are sorted alphabetically in the filename for consistency.

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

## 5. API Client (`nba_balldontlie_client.py`)

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
│   ├── nba_balldontlie_client.py         # API client
│   ├── nba_balldontlie_backfill_v2.py    # Primary collector (V2)
│   ├── nba_balldontlie_backfill.py       # Legacy collector
│   └── nba_config.py                     # Configuration
├── nba_bdl_possessions.py                # Stints/possessions collector
├── premium_nba_collection.py             # Premium data collector
├── loader/
│   └── nba_load_data.py                  # PostgreSQL loader
├── migrations/
│   └── 001_nba_schema.sql                # Database schema
├── data/                                 # V2 backfill output
│   ├── games/
│   ├── player_stats/
│   ├── advanced_stats_v2/
│   ├── lineups/
│   ├── play_by_play/
│   ├── odds/
│   ├── player_props/
│   ├── season_averages/
│   ├── standings/
│   └── injuries/
├── stage/                                # Legacy/premium output
│   ├── nba_games_YYYY-MM-DD.parquet
│   └── nba_box_scores_YYYY-MM-DD.parquet
├── nba_stints_data/                      # Stints output
│   ├── stints/
│   │   ├── 2025-10_stints.csv           # All teams
│   │   ├── 2025-10_LAL_stints.csv       # Single team
│   │   └── 2025-10_BOS_LAL_stints.csv   # Multiple teams
│   └── logs/
├── .env                                  # API key (not in git)
├── requirements.txt
└── README.md
```

---

## Common Workflows

### Daily Data Collection (Cron)

```bash
# Add to crontab - runs at 6 AM daily
0 6 * * * cd /path/to/nba-data-scraper && \
    python py/nba_balldontlie_backfill_v2.py --start $(date -d "yesterday" +%Y-%m-%d) --end $(date -d "yesterday" +%Y-%m-%d) --season 2025 --daily && \
    python nba_bdl_possessions.py daily \
    >> /var/log/nba_collector.log 2>&1
```

### Backfill Full Season

```bash
# Collect all 2025-26 season data (V2 - comprehensive)
python py/nba_balldontlie_backfill_v2.py \
    --start 2025-10-22 --end 2026-04-15 \
    --season 2025 --full

# Collect stint/possession data
python nba_bdl_possessions.py backfill --season 2025
```

### Pre-Game Data Collection

```bash
# Today's games with odds and props
python py/nba_balldontlie_backfill_v2.py \
    --start $(date +%Y-%m-%d) --end $(date +%Y-%m-%d) \
    --season 2025 --games --odds --player-props

# Lakers-specific stint data
python nba_bdl_possessions.py backfill --teams LAL --season 2025
```

### Team-Specific Full Backfill

```bash
# All Lakers data for the season
python py/nba_balldontlie_backfill_v2.py \
    --start 2025-10-22 --end 2026-01-31 \
    --season 2025 --full --team LAL

# Lakers stint data
python nba_bdl_possessions.py backfill --teams LAL --season 2025
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

# Load all stints (all teams)
stints_dir = Path("nba_stints_data/stints")
all_stints = pd.concat([
    pd.read_csv(f) for f in stints_dir.glob("*_stints.csv")
])

# Load team-specific stints
lakers_stints = pd.read_csv("nba_stints_data/stints/2025-11_LAL_stints.csv")

# Load multi-team stints
rivalry_stints = pd.read_csv("nba_stints_data/stints/2025-11_BOS_LAL_stints.csv")

# Filter games to specific team
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