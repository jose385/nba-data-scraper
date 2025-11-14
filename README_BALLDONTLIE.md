# NBA Betting Analysis - BallDontLie API Edition

🏀 **Clean, simple, and reliable NBA data collection using BallDontLie API**

## Why BallDontLie?

### Problems with Previous Approach:
- ❌ `nba_api` (stats.nba.com) - Frequent timeouts, Cloudflare blocking, complex rate limiting
- ❌ ESPN API - Inconsistent data format, limited endpoints
- ❌ Complex multi-source fallback system - Maintenance nightmare

### Benefits of BallDontLie:
- ✅ **Simple REST API** - Easy to use, well-documented
- ✅ **Reliable** - No Cloudflare blocking, consistent uptime
- ✅ **Clear rate limits** - 30 req/min free, 60 with API key
- ✅ **Good enough data** - Perfect for betting analysis
- ✅ **One clean source** - No complex fallback logic needed

## What You Get

### From BallDontLie API (Automated Collection):
- ✅ **Games**: Schedule, scores, final results, periods
- ✅ **Box Scores**: All traditional player stats (pts, reb, ast, fg%, 3pt%, etc.)
- ✅ **Teams**: All 30 NBA teams with conference/division
- ✅ **Season Averages**: Player season statistics

### From Claude (Contextual Research):
- 🔍 **Injury Reports**: Latest player status and return timelines
- 📋 **Starting Lineups**: Confirmed lineups before games
- 👔 **Referee Assignments**: Officials and their tendencies
- 📊 **Recent Trends**: Hot/cold streaks, matchup history
- 💰 **Betting Lines**: Current odds and line movements
- 🎯 **Betting Insights**: Analysis and recommendations

**This division is perfect**: You collect the hard-to-get stats, Claude researches the context!

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements_balldontlie.txt
```

Or minimal install:
```bash
pip install pandas pyarrow numpy requests psycopg2-binary python-dotenv
```

### 2. Get API Key (Optional but Recommended)
```bash
# Go to: https://www.balldontlie.io/
# Sign up (free)
# Copy your API key
# Add to .env:
echo "BALLDONTLIE_API_KEY=your_key_here" >> .env
```

**Free tier**: 30 requests/minute (no key needed)  
**With key**: 60 requests/minute

### 3. Test the API Client
```bash
python py/nba_balldontlie_client.py
```

This will:
- Test API connectivity
- Fetch teams
- Get recent games
- Retrieve box scores

### 4. Collect Historical Data
```bash
# Collect specific date
python py/nba_balldontlie_backfill.py \
  --start 2024-11-13 \
  --end 2024-11-13

# Collect date range
python py/nba_balldontlie_backfill.py \
  --start 2024-11-01 \
  --end 2024-11-13

# With API key (faster)
python py/nba_balldontlie_backfill.py \
  --start 2024-11-13 \
  --end 2024-11-13 \
  --api-key your_key_here
```

### 5. Check Your Data
```bash
ls stage/
# You'll see:
# - nba_games_2024-11-13.parquet
# - nba_box_scores_2024-11-13.parquet
# - nba_complete_2024-11-13.parquet (combined file)
```

### 6. View Data (Convert to CSV)
```bash
python convert_parquet_to_csv.py --input-dir stage
```

### 7. Setup Database (Optional)
```bash
# Configure database
cp .env.nba.template .env
# Edit .env with your PostgreSQL connection

# Run schema migration
psql -U your_user -d your_database -f migrations/002_nba_balldontlie_schema.sql

# Load data
python loader/nba_load_balldontlie_data.py --input-dir stage
```

## Data Structure

### Games (`nba_games_YYYY-MM-DD.parquet`)
```
Columns:
- id, date, season, status
- home_team_id, home_team_name, home_team_abbrev, home_team_score
- away_team_id, away_team_name, away_team_abbrev, away_team_score
- period, time, postseason
```

### Box Scores (`nba_box_scores_YYYY-MM-DD.parquet`)
```
Columns:
- game_id, player_id, team_id
- player_first_name, player_last_name, player_position
- minutes_played (decimal)
- Field Goals: fgm, fga, fg_pct
- Three Pointers: fg3m, fg3a, fg3_pct
- Free Throws: ftm, fta, ft_pct
- Rebounds: oreb, dreb, reb
- Other: ast, stl, blk, turnover, pf, pts
```

### Combined (`nba_complete_YYYY-MM-DD.parquet`)
Complete dataset with games + box scores merged - ready for analysis!

## Usage Examples

### Collect Last Week's Games
```bash
python py/nba_balldontlie_backfill.py \
  --start $(date -d '7 days ago' +%Y-%m-%d) \
  --end $(date +%Y-%m-%d)
```

### Collect Season Opening Week
```bash
python py/nba_balldontlie_backfill.py \
  --start 2024-10-22 \
  --end 2024-10-29 \
  --season 2024
```

### Daily Automated Collection (Cron)
```bash
# Add to crontab for daily 6 AM collection
0 6 * * * cd /path/to/nba && python py/nba_balldontlie_backfill.py --start $(date -d '1 day ago' +\%Y-\%m-\%d) --end $(date -d '1 day ago' +\%Y-\%m-\%d)
```

## Rate Limiting

BallDontLie has clear, simple rate limits:
- **Free**: 30 requests/minute (2 second delay)
- **With API Key**: 60 requests/minute (1 second delay)

Our client automatically handles rate limiting with smart delays.

### Typical Collection Times:
- **Single date**: 30-60 seconds (3-5 API calls)
- **One week**: 3-5 minutes (20-30 API calls)
- **One month**: 10-15 minutes (80-120 API calls)

Much faster and more reliable than `nba_api`!

## Claude Integration Workflow

### 1. Morning: Collect Yesterday's Data
```bash
python py/nba_balldontlie_backfill.py \
  --start $(date -d '1 day ago' +%Y-%m-%d) \
  --end $(date -d '1 day ago' +%Y-%m-%d)
```

### 2. Analyze with Claude
```
"Here's yesterday's NBA data [attach CSV]. 

For tonight's games:
1. Research injury reports
2. Check probable starting lineups
3. Find referee assignments
4. Analyze recent form and trends
5. Recommend betting opportunities"
```

### 3. Claude Provides:
- 🏥 Injury updates (e.g., "LeBron questionable with ankle")
- 📋 Confirmed lineups (e.g., "Curry starting at PG")
- 👔 Refs (e.g., "Scott Foster reffing - expect tight game")
- 📊 Trends (e.g., "Lakers 7-2 ATS in last 9 games")
- 💰 Bets (e.g., "Lakers -5.5, Over 225.5, LeBron O25.5 pts")

## Project Structure

```
nba/
├── py/
│   ├── nba_balldontlie_client.py       # API client
│   ├── nba_balldontlie_backfill.py     # Data collection script
│   └── nba_config.py                    # Configuration
├── migrations/
│   └── 002_nba_balldontlie_schema.sql  # Database schema
├── stage/                               # Parquet data files
├── loader/                              # Database loading scripts
├── requirements_balldontlie.txt         # Python dependencies
└── README_BALLDONTLIE.md               # This file
```

## Advantages Over Previous System

| Feature | Old (nba_api) | New (BallDontLie) |
|---------|---------------|-------------------|
| **Reliability** | ❌ Frequent timeouts | ✅ Very reliable |
| **Setup** | ❌ Complex (3 APIs) | ✅ Simple (1 API) |
| **Rate Limits** | ❌ Unclear, strict | ✅ Clear, reasonable |
| **Code** | ❌ 1000+ lines | ✅ 300 lines |
| **Maintenance** | ❌ High | ✅ Low |
| **Data Quality** | ✅ Excellent | ✅ Good enough |
| **API Key** | ❌ Not available | ✅ Free tier available |

## What Data is Missing?

BallDontLie doesn't provide:
- ❌ Play-by-play data
- ❌ Shot charts with locations
- ❌ Advanced tracking (speed, distance)
- ❌ Real-time live data

**But that's OK!** These are rarely needed for betting analysis, and if you need them, you can:
1. Use Claude to research specific plays
2. Manually check NBA.com for shot charts
3. Focus on what matters: box scores and trends

## Database Schema

Simple, focused schema with 4 main tables:

1. **nba_teams** - Team reference data (30 teams)
2. **nba_games** - Daily game results
3. **nba_box_scores** - Player stats per game
4. **nba_season_averages** - Player season stats

Plus helpful views:
- `recent_player_performance` - Last 10 days
- `game_scoring_trends` - Daily totals
- `team_performance` - Win/loss records

## Troubleshooting

### "Rate limit exceeded"
```bash
# Wait 60 seconds and retry
# Or get API key for higher limits
```

### "No games found"
```bash
# Check if date is during NBA season (Oct-June)
# Off-season = no games
# Use --season flag to verify: --season 2024
```

### "API key not working"
```bash
# Verify key in .env:
cat .env | grep BALLDONTLIE_API_KEY

# Pass directly to script:
python py/nba_balldontlie_backfill.py --api-key YOUR_KEY --start ...
```

### "Data looks empty"
```bash
# BallDontLie updates ~2 hours after games end
# Wait a bit after games finish
# Check status column: should be "Final"
```

## FAQ

**Q: Is BallDontLie free?**  
A: Yes! Free tier gives 30 requests/minute. Optional paid tiers for higher limits.

**Q: Do I need an API key?**  
A: No, but recommended. Increases rate limit from 30 to 60 req/min.

**Q: How recent is the data?**  
A: Updates within 1-2 hours after games end. Good for next-day analysis.

**Q: Can I get live game data?**  
A: Not with BallDontLie. Use official NBA sources for live scores.

**Q: What about advanced stats (PER, True Shooting%, etc.)?**  
A: Calculate these from the traditional stats, or use Claude to analyze.

**Q: Can I get historical data?**  
A: Yes! BallDontLie has data back to ~2010. Just specify date range.

**Q: What's the best way to use this for betting?**  
A: 
1. Collect yesterday's games every morning
2. Feed to Claude with today's matchups
3. Claude researches context (injuries, trends, refs)
4. Get betting recommendations
5. Profit! 💰

## Support

- **BallDontLie Docs**: https://docs.balldontlie.io/
- **API Status**: Check https://www.balldontlie.io/ for any issues
- **Get API Key**: https://www.balldontlie.io/

## Next Steps

1. ✅ Test API connection: `python py/nba_balldontlie_client.py`
2. ✅ Collect sample data: `python py/nba_balldontlie_backfill.py --start 2024-11-13 --end 2024-11-13`
3. ✅ View the data: `ls stage/` and convert to CSV
4. ✅ Send to Claude for analysis
5. ✅ Start making informed bets!

---

**Much simpler. Much more reliable. Perfect for betting analysis.** 🏀📊💰