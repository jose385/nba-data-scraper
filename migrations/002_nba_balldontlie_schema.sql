-- migrations/002_nba_balldontlie_schema.sql
-- SIMPLIFIED NBA Schema for BallDontLie API
-- Focuses on actual data available from the API

CREATE SCHEMA IF NOT EXISTS public;

-- =============================================================================
-- TEAMS TABLE (Reference data - collect once per season)
-- =============================================================================
DROP TABLE IF EXISTS public.nba_teams CASCADE;
CREATE TABLE public.nba_teams (
  -- Core identifiers
  team_id INTEGER PRIMARY KEY,
  abbreviation TEXT NOT NULL,
  full_name TEXT NOT NULL,
  
  -- League structure
  conference TEXT,              -- "East" or "West"
  division TEXT,                -- "Atlantic", "Central", etc.
  city TEXT,
  
  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- GAMES TABLE (Daily game results and scores)
-- =============================================================================
DROP TABLE IF EXISTS public.nba_games CASCADE;
CREATE TABLE public.nba_games (
  -- Core identifiers
  game_id BIGINT PRIMARY KEY,
  game_date DATE NOT NULL,
  season INTEGER NOT NULL,        -- 2024 for 2024-25 season
  
  -- Teams
  home_team_id INTEGER NOT NULL REFERENCES nba_teams(team_id),
  away_team_id INTEGER NOT NULL REFERENCES nba_teams(team_id),
  home_team_abbrev TEXT,
  away_team_abbrev TEXT,
  
  -- Scores
  home_team_score INTEGER,
  away_team_score INTEGER,
  total_points INTEGER GENERATED ALWAYS AS (
    COALESCE(home_team_score, 0) + COALESCE(away_team_score, 0)
  ) STORED,
  point_differential INTEGER GENERATED ALWAYS AS (
    COALESCE(home_team_score, 0) - COALESCE(away_team_score, 0)
  ) STORED,
  
  -- Game info
  status TEXT,                    -- "Final", "In Progress", etc.
  period INTEGER,                 -- Current/final period
  time_remaining TEXT,            -- Time remaining in period
  postseason BOOLEAN DEFAULT false,
  
  -- Betting-relevant flags
  high_scoring BOOLEAN GENERATED ALWAYS AS (
    COALESCE(home_team_score, 0) + COALESCE(away_team_score, 0) >= 230
  ) STORED,
  low_scoring BOOLEAN GENERATED ALWAYS AS (
    COALESCE(home_team_score, 0) + COALESCE(away_team_score, 0) <= 195
  ) STORED,
  blowout BOOLEAN GENERATED ALWAYS AS (
    ABS(COALESCE(home_team_score, 0) - COALESCE(away_team_score, 0)) >= 20
  ) STORED,
  close_game BOOLEAN GENERATED ALWAYS AS (
    ABS(COALESCE(home_team_score, 0) - COALESCE(away_team_score, 0)) <= 5
  ) STORED,
  
  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- BOX SCORES TABLE (Player stats per game)
-- Core betting data: player props, performance, trends
-- =============================================================================
DROP TABLE IF EXISTS public.nba_box_scores CASCADE;
CREATE TABLE public.nba_box_scores (
  -- Core identifiers
  stat_id BIGINT PRIMARY KEY,
  game_id BIGINT NOT NULL REFERENCES nba_games(game_id),
  player_id INTEGER NOT NULL,
  team_id INTEGER NOT NULL REFERENCES nba_teams(team_id),
  
  -- Player info
  player_first_name TEXT,
  player_last_name TEXT,
  player_position TEXT,           -- "G", "F", "C", "G-F", etc.
  team_abbrev TEXT,
  
  -- Playing time
  minutes_played REAL,            -- Decimal minutes
  
  -- Shooting stats (CORE for betting)
  field_goals_made INTEGER,
  field_goals_attempted INTEGER,
  field_goal_pct REAL,
  three_pointers_made INTEGER,
  three_pointers_attempted INTEGER,
  three_point_pct REAL,
  free_throws_made INTEGER,
  free_throws_attempted INTEGER,
  free_throw_pct REAL,
  
  -- Rebounding (CORE for props)
  offensive_rebounds INTEGER,
  defensive_rebounds INTEGER,
  total_rebounds INTEGER,
  
  -- Playmaking & Defense (CORE for props)
  assists INTEGER,
  steals INTEGER,
  blocks INTEGER,
  turnovers INTEGER,
  personal_fouls INTEGER,
  
  -- Scoring (CORE for props)
  points INTEGER,
  
  -- Prop thresholds (commonly bet props)
  points_over_15_5 BOOLEAN GENERATED ALWAYS AS (points >= 16) STORED,
  points_over_19_5 BOOLEAN GENERATED ALWAYS AS (points >= 20) STORED,
  points_over_24_5 BOOLEAN GENERATED ALWAYS AS (points >= 25) STORED,
  points_over_29_5 BOOLEAN GENERATED ALWAYS AS (points >= 30) STORED,
  
  rebounds_over_8_5 BOOLEAN GENERATED ALWAYS AS (total_rebounds >= 9) STORED,
  rebounds_over_10_5 BOOLEAN GENERATED ALWAYS AS (total_rebounds >= 11) STORED,
  
  assists_over_4_5 BOOLEAN GENERATED ALWAYS AS (assists >= 5) STORED,
  assists_over_5_5 BOOLEAN GENERATED ALWAYS AS (assists >= 6) STORED,
  assists_over_7_5 BOOLEAN GENERATED ALWAYS AS (assists >= 8) STORED,
  
  -- Performance indicators
  double_double BOOLEAN GENERATED ALWAYS AS (
    (points >= 10)::INTEGER + (total_rebounds >= 10)::INTEGER + 
    (assists >= 10)::INTEGER + (steals >= 10)::INTEGER + (blocks >= 10)::INTEGER >= 2
  ) STORED,
  
  triple_double BOOLEAN GENERATED ALWAYS AS (
    (points >= 10)::INTEGER + (total_rebounds >= 10)::INTEGER + 
    (assists >= 10)::INTEGER + (steals >= 10)::INTEGER + (blocks >= 10)::INTEGER >= 3
  ) STORED,
  
  efficient_game BOOLEAN GENERATED ALWAYS AS (
    points >= 15 AND field_goal_pct >= 0.50 AND turnovers <= 3
  ) STORED,
  
  -- Metadata
  stat_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- SEASON AVERAGES TABLE (Player season stats for context)
-- Helps identify over/under opportunities
-- =============================================================================
DROP TABLE IF EXISTS public.nba_season_averages CASCADE;
CREATE TABLE public.nba_season_averages (
  -- Core identifiers
  player_id INTEGER NOT NULL,
  season INTEGER NOT NULL,
  
  -- Stats (averages per game)
  games_played INTEGER,
  minutes_played REAL,
  
  -- Shooting averages
  field_goals_made REAL,
  field_goals_attempted REAL,
  field_goal_pct REAL,
  three_pointers_made REAL,
  three_pointers_attempted REAL,
  three_point_pct REAL,
  free_throws_made REAL,
  free_throws_attempted REAL,
  free_throw_pct REAL,
  
  -- Counting stats averages
  offensive_rebounds REAL,
  defensive_rebounds REAL,
  total_rebounds REAL,
  assists REAL,
  steals REAL,
  blocks REAL,
  turnovers REAL,
  personal_fouls REAL,
  points REAL,
  
  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (player_id, season)
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Teams indexes
CREATE INDEX idx_teams_abbreviation ON nba_teams(abbreviation);
CREATE INDEX idx_teams_conference ON nba_teams(conference);

-- Games indexes
CREATE INDEX idx_games_date ON nba_games(game_date);
CREATE INDEX idx_games_season ON nba_games(season);
CREATE INDEX idx_games_home_team ON nba_games(home_team_id);
CREATE INDEX idx_games_away_team ON nba_games(away_team_id);
CREATE INDEX idx_games_status ON nba_games(status);
CREATE INDEX idx_games_high_scoring ON nba_games(high_scoring) WHERE high_scoring = true;
CREATE INDEX idx_games_close ON nba_games(close_game) WHERE close_game = true;

-- Box scores indexes (CRITICAL for queries)
CREATE INDEX idx_box_scores_game ON nba_box_scores(game_id);
CREATE INDEX idx_box_scores_player ON nba_box_scores(player_id);
CREATE INDEX idx_box_scores_team ON nba_box_scores(team_id);
CREATE INDEX idx_box_scores_date ON nba_box_scores(stat_date);
CREATE INDEX idx_box_scores_points ON nba_box_scores(points);
CREATE INDEX idx_box_scores_rebounds ON nba_box_scores(total_rebounds);
CREATE INDEX idx_box_scores_assists ON nba_box_scores(assists);
CREATE INDEX idx_box_scores_double_double ON nba_box_scores(double_double) WHERE double_double = true;

-- Season averages indexes
CREATE INDEX idx_season_avgs_player ON nba_season_averages(player_id);
CREATE INDEX idx_season_avgs_season ON nba_season_averages(season);

-- =============================================================================
-- USEFUL VIEWS FOR BETTING ANALYSIS
-- =============================================================================

-- Recent player performance (last 5 games)
CREATE OR REPLACE VIEW recent_player_performance AS
SELECT 
  player_id,
  player_first_name,
  player_last_name,
  team_abbrev,
  COUNT(*) as games_played,
  AVG(points) as avg_points,
  AVG(total_rebounds) as avg_rebounds,
  AVG(assists) as avg_assists,
  AVG(field_goal_pct) as avg_fg_pct,
  AVG(three_point_pct) as avg_3pt_pct,
  SUM(CASE WHEN double_double THEN 1 ELSE 0 END) as double_doubles,
  MAX(stat_date) as last_game_date
FROM nba_box_scores
WHERE stat_date >= CURRENT_DATE - INTERVAL '10 days'
  AND minutes_played >= 15  -- Only players with meaningful minutes
GROUP BY player_id, player_first_name, player_last_name, team_abbrev
HAVING COUNT(*) >= 3;  -- At least 3 games in last 10 days

-- Game scoring trends (for totals betting)
CREATE OR REPLACE VIEW game_scoring_trends AS
SELECT 
  game_date,
  season,
  COUNT(*) as games_played,
  AVG(total_points) as avg_total_points,
  AVG(home_team_score) as avg_home_score,
  AVG(away_team_score) as avg_away_score,
  SUM(CASE WHEN high_scoring THEN 1 ELSE 0 END) as high_scoring_games,
  SUM(CASE WHEN low_scoring THEN 1 ELSE 0 END) as low_scoring_games,
  SUM(CASE WHEN blowout THEN 1 ELSE 0 END) as blowout_games,
  SUM(CASE WHEN close_game THEN 1 ELSE 0 END) as close_games
FROM nba_games
WHERE status = 'Final'
GROUP BY game_date, season
ORDER BY game_date DESC;

-- Team performance summary
CREATE OR REPLACE VIEW team_performance AS
SELECT 
  t.team_id,
  t.abbreviation,
  t.full_name,
  COUNT(*) as games_played,
  SUM(CASE 
    WHEN (g.home_team_id = t.team_id AND g.home_team_score > g.away_team_score) OR
         (g.away_team_id = t.team_id AND g.away_team_score > g.home_team_score)
    THEN 1 ELSE 0 END) as wins,
  AVG(CASE 
    WHEN g.home_team_id = t.team_id THEN g.home_team_score
    ELSE g.away_team_score 
  END) as avg_points_for,
  AVG(CASE 
    WHEN g.home_team_id = t.team_id THEN g.away_team_score
    ELSE g.home_team_score 
  END) as avg_points_against
FROM nba_teams t
JOIN nba_games g ON t.team_id = g.home_team_id OR t.team_id = g.away_team_id
WHERE g.status = 'Final'
  AND g.season = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY t.team_id, t.abbreviation, t.full_name;

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

CREATE OR REPLACE FUNCTION get_player_recent_avg(
  p_player_id INTEGER,
  p_stat_name TEXT,
  p_games INTEGER DEFAULT 5
)
RETURNS NUMERIC AS $$
BEGIN
  RETURN (
    SELECT AVG(
      CASE p_stat_name
        WHEN 'points' THEN points
        WHEN 'rebounds' THEN total_rebounds
        WHEN 'assists' THEN assists
        WHEN 'steals' THEN steals
        WHEN 'blocks' THEN blocks
        ELSE 0
      END
    )
    FROM (
      SELECT *
      FROM nba_box_scores
      WHERE player_id = p_player_id
        AND minutes_played >= 15
      ORDER BY stat_date DESC
      LIMIT p_games
    ) recent_games
  );
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================
COMMENT ON TABLE nba_teams IS 'NBA teams - reference data from BallDontLie API';
COMMENT ON TABLE nba_games IS 'Daily NBA games with scores and results';
COMMENT ON TABLE nba_box_scores IS 'Player stats per game - core betting data';
COMMENT ON TABLE nba_season_averages IS 'Player season averages for context';

COMMENT ON VIEW recent_player_performance IS 'Last 10 days player performance for prop betting';
COMMENT ON VIEW game_scoring_trends IS 'Daily scoring trends for totals betting';
COMMENT ON VIEW team_performance IS 'Team win/loss and scoring averages';

-- =============================================================================
-- SCHEMA VERSION
-- =============================================================================
CREATE TABLE IF NOT EXISTS schema_info (
  version TEXT PRIMARY KEY,
  description TEXT,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO schema_info (version, description) 
VALUES ('2.0.0-balldontlie', 'Simplified NBA schema for BallDontLie API data')
ON CONFLICT (version) DO UPDATE SET 
  description = EXCLUDED.description,
  applied_at = CURRENT_TIMESTAMP;

SELECT 'NBA BALLDONTLIE SCHEMA COMPLETE: Clean, focused tables for betting analysis' AS status;