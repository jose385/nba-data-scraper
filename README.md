# NBA 2024-25 Daily Data Collector

🎯 **Flexible system to collect NBA data for specific days, date ranges, or catch up on missed games.**

Perfect for building your prediction model incrementally and staying current with the 2024-25 season. No more massive backfills - collect exactly the days you need!

## 🚀 Quick Start

### **1. One-Click Setup**
```bash
python quick_setup.py
```

### **2. Collect Yesterday's Games**
```bash
python nba_daily_collector.py --yesterday
```

### **3. Check Your Data**
```bash
ls nba_2024_25_data/combined_daily/
# You'll see: nba_complete_2024-10-XX.csv
```

## 📅 Flexible Collection Options

### **Single Day Collection**
```bash
# Specific date
python nba_daily_collector.py --date 2024-10-20

# Yesterday's games
python nba_daily_collector.py --yesterday

# Today's games (useful for morning predictions)
python nba_daily_collector.py --today
```

### **Date Range Collection**
```bash
# Last week's games
python nba_daily_collector.py --last-week

# Custom date range
python nba_daily_collector.py --start 2024-10-15 --end 2024-10-25

# Season opening week
python nba_daily_collector.py --start 2024-10-01 --end 2024-10-07
```

### **Smart Catch-Up**
```bash
# Fill missing days since season start
python nba_daily_collector.py --catch-up --start 2024-10-01

# Fill missing days in specific range
python nba_daily_collector.py --catch-up --start 2024-10-15 --end 2024-10-25
```

### **With Betting Odds**
```bash
# Add betting data (requires free API key)
python nba_daily_collector.py --yesterday --odds-api-key YOUR_ODDS_API_KEY

# Get API key free at: https://the-odds-api.com (500 requests/month)
```

## 📊 What Data You Get

### **🎯 Combined Daily Files (LLM-Ready)**
**Location**: `nba_2024_25_data/combined_daily/nba_complete_YYYY-MM-DD.csv`

Each file contains **ALL** data for that day's games:
- ✅ **Game information** (teams, scores, times)
- ✅ **Player traditional stats** (points, rebounds, assists, shooting %)
- ✅ **Player advanced stats** (usage rate, PER, efficiency)
- ✅ **Team traditional stats** (team totals, shooting %)
- ✅ **Team advanced stats** (offensive rating, defensive rating, pace)
- ✅ **Betting odds** (moneylines, spreads, totals - if API key provided)

**Perfect for feeding directly into your LLM for same-day predictions!**

### **📁 Organized Data Structure**
```
nba_2024_25_data/
├── combined_daily/              # 🎯 MAIN FILES - Use these!
│   ├── nba_complete_2024-10-20.csv
│   ├── nba_complete_2024-10-21.csv
│   └── nba_complete_2024-10-22.csv
├── daily_games/                 # Individual data components
│   ├── games_2024-10-20.csv           # Basic game info
│   ├── player_traditional_2024-10-20.csv  # Player stats
│   ├── player_advanced_2024-10-20.csv     # Advanced player metrics
│   ├── team_traditional_2024-10-20.csv    # Team stats
│   └── team_advanced_2024-10-20.csv       # Advanced team metrics
├── odds_data/                   # Betting odds (if API key provided)
│   └── odds_2024-10-20.csv
└── collection_summary_*.json    # Collection reports
```

## 🎯 Perfect for Your Use Case

### **Same-Day Predictions**
```bash
# Morning routine: collect yesterday's games for analysis
python nba_daily_collector.py --yesterday

# Afternoon: collect today's games for evening predictions  
python nba_daily_collector.py --today --odds-api-key YOUR_KEY
```

### **Incremental Model Building**
```bash
# Start with recent week
python nba_daily_collector.py --last-week

# Gradually expand backwards
python nba_daily_collector.py --start 2024-10-01 --end 2024-10-14

# Fill any gaps
python nba_daily_collector.py --catch-up --start 2024-10-01
```

### **Focus on Important Games**
```bash
# Opening night
python nba_daily_collector.py --date 2024-10-22

# Christmas games
python nba_daily_collector.py --date 2024-12-25

# All-Star weekend
python nba_daily_collector.py --start 2025-02-14 --end 2025-02-16
```

## 📈 Data Features & Predictive Power

Based on our previous research, you'll get the **most predictive NBA features**:

### **🏆 Tier 1 Features (Highest Predictive Power)**
- **Team Efficiency Differential** (38% importance)
- **Defensive Rating**
- **Offensive Rating** 
- **Net Rating**
- **Pace** (possessions per game)

### **🎯 Tier 2 Features**
- **Shooting Efficiency** (eFG%, TS%)
- **Rebounding Rates**
- **Turnover Rates**
- **Free Throw Rates**

### **📊 Tier 3 Features**
- **Player Usage Rates**
- **Player Efficiency Rating (PER)**
- **Plus/Minus Statistics**
- **Lineup-specific data**

### **💰 Betting Intelligence** (with API key)
- **Opening vs Current Lines**
- **Market Movement**
- **Sharp vs Public Money indicators**
- **Multiple sportsbook comparison**

## ⏱️ Performance & Efficiency

| Collection Type | Runtime | API Calls | Disk Space | Best For |
|----------------|---------|-----------|------------|----------|
| **Single Day** | 2-5 minutes | 10-30 | ~5-15MB | Daily routine |
| **Last Week** | 15-30 minutes | 70-210 | ~50-100MB | Weekly catch-up |
| **Month** | 45-90 minutes | 300-900 | ~200-400MB | Monthly analysis |
| **Season Start** | 2-3 hours | 1000+ | ~1GB | Initial backfill |

**Rate Limiting**: Built-in 600ms delays between NBA API calls (respects NBA.com limits)

## 🔑 API Keys (Optional but Recommended)

### **The Odds API** (Free Tier)
- **Get it**: [the-odds-api.com](https://the-odds-api.com/)
- **Free**: 500 requests/month
- **Covers**: ~15-20 game days per month
- **Paid**: $10-50/month for unlimited

### **Usage**:
```bash
python nba_daily_collector.py --yesterday --odds-api-key "your_key_here"
```

## 🛠️ Advanced Usage

### **Custom Output Directory**
```bash
python nba_daily_collector.py --yesterday --output-dir my_nba_data
```

### **Check Missing Days**
```bash
# Shows what days you're missing without collecting
python nba_daily_collector.py --catch-up --start 2024-10-01 --end 2024-10-31
```

### **Automated Daily Collection** (Cron/Scheduled)
```bash
# Add to crontab for daily 6 AM collection
0 6 * * * cd /path/to/nba && python nba_daily_collector.py --yesterday
```

## 🎯 Integration with Your LLM Model

### **Daily Prediction Workflow**
1. **Morning**: Collect yesterday's results
   ```bash
   python nba_daily_collector.py --yesterday
   ```

2. **Afternoon**: Collect today's games + odds for predictions
   ```bash
   python nba_daily_collector.py --today --odds-api-key YOUR_KEY
   ```

3. **Feed to LLM**: Use the combined daily files
   ```python
   # Your LLM prediction code
   daily_data = pd.read_csv("nba_2024_25_data/combined_daily/nba_complete_2024-10-21.csv")
   # Feed to your model for game predictions
   ```

### **Training Data Preparation**
```bash
# Collect historical data for model training
python nba_daily_collector.py --start 2024-10-01 --end 2024-10-31

# Use combined_daily/*.csv files as training input
# Each file = complete context for that day's games
```

## 🔧 Troubleshooting

### **Common Issues**

**"No games found"**
- NBA season runs October-June
- Check if you're requesting off-season dates
- Some days have no games scheduled

**"API rate limit"**
- Built-in delays should prevent this
- If persistent, check your internet connection
- NBA API is free but has usage limits

**"Missing detailed stats"**
- Game data appears gradually after games finish
- Collect 2-3 hours after games end for complete stats
- Some historical games may have incomplete data

### **Data Validation**
```bash
# Check what you've collected
ls -la nba_2024_25_data/combined_daily/

# View collection summaries
cat nba_2024_25_data/collection_summary_*.json
```

## 📅 NBA 2024-25 Season Schedule

**Key Dates for Data Collection:**
- **Season Start**: October 22, 2024
- **All-Star Break**: February 14-16, 2025
- **Regular Season End**: April 13, 2025
- **Playoffs**: April 19 - June 22, 2025

**Game Frequency:**
- **Peak season**: 10-15 games per day
- **Light days**: 2-6 games per day
- **No games**: All-Star break, some Sundays

## 🎉 Success Stories

**Perfect for:**
- ✅ **Daily prediction models** 
- ✅ **Incremental data building**
- ✅ **Testing specific scenarios**
- ✅ **Catching up on missed days**
- ✅ **Focus on important games**
- ✅ **LLM training data preparation**

**Examples:**
```bash
# Build last 30 days for model testing
python nba_daily_collector.py --start 2024-09-22 --end 2024-10-22

# Focus on playoff-contending teams' games
python nba_daily_collector.py --start 2024-10-01 --end 2024-10-31

# Collect high-profile games only
python nba_daily_collector.py --date 2024-12-25  # Christmas
python nba_daily_collector.py --date 2024-10-22  # Season opener
```

## 📞 Support

**Questions? Issues?**
1. Check the log file: `nba_daily_collector.log`
2. Verify your date formats: `YYYY-MM-DD`
3. Test with a single recent day first
4. Make sure you have internet connectivity

**Pro Tips:**
- Start with `--yesterday` to test everything works
- Use `--catch-up` to find and fill missing days
- Combined daily files are perfect for LLM input
- Collect 2-3 hours after games end for complete stats

---

## 🏀 Ready to Start?

```bash
# 1. Quick setup
python quick_setup.py

# 2. Test with yesterday
python nba_daily_collector.py --yesterday

# 3. Check your data
ls nba_2024_25_data/combined_daily/

# 4. Start building your model!
```

**This flexible system gives you exactly the data you need, when you need it, for building the best NBA prediction model possible!** 🎯