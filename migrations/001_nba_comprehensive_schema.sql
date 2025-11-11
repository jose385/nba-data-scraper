-- migrations/001_nba_comprehensive_schema.sql
-- COMPREHENSIVE NBA Schema - Claude-Optimized for Betting Analysis
-- Focuses on impossible-to-get tracking data while Claude researches context

CREATE SCHEMA IF NOT EXISTS public;

-- =============================================================================
-- GAME_INFO TABLE (NBA Game Results & Context)
-- Basic game information and results - foundation for all analysis
-- =============================================================================
DROP TABLE IF EXISTS public.game_info CASCADE;
CREATE TABLE public.game_info (
  -- Core identifiers (NBA uses different ID system than MLB)
  game_id TEXT PRIMARY KEY,           -- NBA format: "0022300156" 
  game_date DATE NOT NULL,
  season_year INTEGER NOT NULL,       -- 2024 for 2023-24 season
  season_type TEXT NOT NULL,          -- "Regular Season", "Playoffs", "Preseason"
  
  -- Teams and results
  home_team_id INTEGER NOT NULL,
  away_team_id INTEGER NOT NULL, 
  home_team_abbrev TEXT NOT NULL,     -- "LAL", "GSW", etc.
  away_team_abbrev TEXT NOT NULL,
  home_team_name TEXT,                -- "Los Angeles Lakers"
  away_team_name TEXT,
  
  -- Final results
  home_score INTEGER,
  away_score INTEGER,
  winning_team_id INTEGER,
  
  -- Game context (critical for NBA betting)
  arena_name TEXT,
  arena_city TEXT,
  arena_state TEXT,
  game_time_et TEXT,                  -- Start time affects performance
  attendance INTEGER,
  game_duration_minutes INTEGER,      -- Total game time including OT
  
  -- Game status and flow
  game_status TEXT,                   -- "Final", "In Progress", "Postponed"
  period_count INTEGER,               -- 4 for regulation, 5+ for OT
  overtime_periods INTEGER,           -- Number of OT periods
  
  -- Pace and style metrics (KEY for totals betting)
  total_possessions INTEGER,          -- Estimated possessions per team
  pace_estimate REAL,                 -- Possessions per 48 minutes
  total_points INTEGER GENERATED ALWAYS AS (COALESCE(home_score, 0) + COALESCE(away_score, 0)) STORED,
  
  -- Back-to-back context (CRITICAL for NBA performance)
  home_days_rest INTEGER,            -- Days since last game
  away_days_rest INTEGER,
  home_back_to_back BOOLEAN GENERATED ALWAYS AS (home_days_rest <= 1) STORED,
  away_back_to_back BOOLEAN GENERATED ALWAYS AS (away_days_rest <= 1) STORED,
  
  -- Travel context
  home_games_last_7 INTEGER,         -- Games at home in last 7
  away_games_last_7 INTEGER,         -- Games on road in last 7
  
  -- Betting context flags
  high_scoring_game BOOLEAN GENERATED ALWAYS AS (total_points >= 230) STORED,
  low_scoring_game BOOLEAN GENERATED ALWAYS AS (total_points <= 195) STORED,
  blowout_game BOOLEAN GENERATED ALWAYS AS (ABS(COALESCE(home_score, 0) - COALESCE(away_score, 0)) >= 20) STORED,
  overtime_game BOOLEAN GENERATED ALWAYS AS (overtime_periods > 0) STORED,
  
  -- Data quality
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- PLAYS TABLE (Comprehensive Play-by-Play Data)
-- NBA's equivalent to MLB's "games" table - the core data goldmine
-- Includes ALL possible NBA API columns to prevent runtime crashes
-- =============================================================================
DROP TABLE IF EXISTS public.plays CASCADE;
CREATE TABLE public.plays (
  -- =================================================================
  -- CORE IDENTIFIERS (Required for all plays)
  -- =================================================================
  game_id TEXT NOT NULL,
  play_id TEXT NOT NULL,              -- Unique identifier for each play
  event_num INTEGER,                  -- Sequence number within game
  period INTEGER NOT NULL,            -- Quarter/OT period (1-4+)
  clock_time TEXT,                    -- Time remaining "11:42"
  clock_seconds_remaining INTEGER,    -- Seconds left in period
  game_clock_seconds INTEGER,         -- Total seconds elapsed in game
  
  -- =================================================================
  -- PLAY DETAILS (Core event information)
  -- =================================================================
  event_type TEXT,                    -- "FIELD_GOAL_MADE", "REBOUND", etc.
  event_action_type INTEGER,          -- NBA's numeric event type
  event_subtype TEXT,                 -- More specific event description
  play_description TEXT,              -- Human readable description
  
  -- =================================================================
  -- SCORE TRACKING (Before and after each play)
  -- =================================================================
  score_home INTEGER,                 -- Home score after play
  score_away INTEGER,                 -- Away score after play
  score_margin INTEGER,               -- Home team lead/deficit
  
  -- =================================================================
  -- PLAYER INVOLVEMENT (Who did what)
  -- =================================================================
  player1_id INTEGER,                 -- Primary player (shooter, rebounder)
  player1_name TEXT,
  player1_team_id INTEGER,
  
  player2_id INTEGER,                 -- Secondary player (assister, steal from)
  player2_name TEXT, 
  player2_team_id INTEGER,
  
  player3_id INTEGER,                 -- Third player (blocker on assist)
  player3_name TEXT,
  player3_team_id INTEGER,
  
  -- =================================================================
  -- SHOT DATA (When applicable - NBA's "Statcast")
  -- =================================================================
  shot_attempted BOOLEAN,
  shot_made BOOLEAN,
  shot_type TEXT,                     -- "2PT Field Goal", "3PT Field Goal"
  shot_zone_basic TEXT,               -- "Mid-Range", "Restricted Area"
  shot_zone_area TEXT,                -- "Right Side(R)", "Center(C)"
  shot_zone_range TEXT,               -- "Less Than 8 ft.", "16-24 ft."
  shot_distance REAL,                 -- Distance from basket in feet
  
  -- Shot location coordinates (NBA's x,y system)
  loc_x INTEGER,                      -- X coordinate (court position)
  loc_y INTEGER,                      -- Y coordinate (court position)
  
  -- Advanced shot context
  shot_clock_remaining REAL,          -- Shot clock time when shot taken
  closest_defender_distance REAL,    -- Nearest defender in feet
  closest_defender_id INTEGER,       -- ID of nearest defender
  defender_contest_type TEXT,         -- "Open", "Tight", "Wide Open"
  
  -- Shot quality metrics
  shot_probability REAL,              -- Expected make probability (0-1)
  expected_points REAL,               -- Expected points from shot attempt
  points_added REAL,                  -- Actual - expected points
  
  -- =================================================================
  -- ADVANCED TRACKING DATA (NBA's Player Tracking)
  -- =================================================================
  player1_speed_mph REAL,            -- Speed of primary player
  player1_distance_traveled REAL,    -- Distance traveled on play
  ball_speed_mph REAL,               -- Speed of ball movement
  pass_distance REAL,                -- Distance of pass (if applicable)
  
  -- Possession context
  possession_team_id INTEGER,        -- Which team has possession
  possession_start_type TEXT,        -- How possession began
  possession_length_seconds REAL,    -- How long possession lasted
  
  -- =================================================================
  -- GAME SITUATION (Critical for betting context)
  -- =================================================================
  quarter INTEGER GENERATED ALWAYS AS (
    CASE 
      WHEN period <= 4 THEN period
      ELSE 4 + (period - 4)  -- OT periods
    END
  ) STORED,
  
  is_clutch_time BOOLEAN GENERATED ALWAYS AS (
    period >= 4 AND clock_seconds_remaining <= 300  -- Last 5 minutes
    AND ABS(score_margin) <= 5
  ) STORED,
  
  is_garbage_time BOOLEAN GENERATED ALWAYS AS (
    period >= 4 AND clock_seconds_remaining <= 120  -- Last 2 minutes
    AND ABS(score_margin) >= 15
  ) STORED,
  
  late_game BOOLEAN GENERATED ALWAYS AS (period >= 4 AND clock_seconds_remaining <= 120) STORED,
  
  -- =================================================================
  -- FOUL AND OFFICIATING DATA
  -- =================================================================
  foul_type TEXT,                     -- "Personal", "Technical", "Flagrant"
  foul_drawn_by INTEGER,             -- Player who drew the foul
  foul_severity INTEGER,              -- 1=Common, 2=Flagrant 1, 3=Flagrant 2
  free_throw_attempt BOOLEAN,
  free_throw_made BOOLEAN,
  free_throw_type TEXT,              -- "1 of 2", "Technical"
  
  -- =================================================================
  -- REBOUND DATA (When applicable)
  -- =================================================================
  rebound_type TEXT,                  -- "Offensive", "Defensive"
  rebound_distance REAL,             -- Distance from basket when rebounded
  rebounds_available INTEGER,        -- Total players in rebound area
  
  -- =================================================================
  -- ASSIST AND PASSING DATA
  -- =================================================================
  assist_type TEXT,                   -- Type of assist if applicable
  pass_type TEXT,                     -- "Standard", "Alley Oop", etc.
  secondary_assist_id INTEGER,       -- Hockey assist player
  
  -- =================================================================
  -- TURNOVER DATA
  -- =================================================================
  turnover_type TEXT,                 -- "Bad Pass", "Traveling", etc.
  steal_type TEXT,                    -- Type of steal if applicable
  forced_turnover BOOLEAN,           -- Whether turnover was forced
  
  -- =================================================================
  -- SUBSTITUTION DATA
  -- =================================================================
  sub_player_in INTEGER,             -- Player entering game
  sub_player_out INTEGER,            -- Player leaving game
  
  -- =================================================================
  -- TIMEOUT DATA
  -- =================================================================
  timeout_team_id INTEGER,           -- Team calling timeout
  timeout_type TEXT,                 -- "Full", "20-Second", "Official"
  timeouts_remaining_home INTEGER,   -- Timeouts left for home team
  timeouts_remaining_away INTEGER,   -- Timeouts left for away team
  
  -- =================================================================
  -- ADVANCED METRICS (Calculated fields)
  -- =================================================================
  win_probability_home REAL,         -- Home team win probability (0-1)
  win_probability_change REAL,       -- Change in win prob from play
  leverage_score REAL,               -- How important this play was (0-10)
  
  -- =================================================================
  -- LINEUP CONTEXT (Who was on court)
  -- =================================================================
  home_player1_id INTEGER,           -- Home team on-court players
  home_player2_id INTEGER,
  home_player3_id INTEGER, 
  home_player4_id INTEGER,
  home_player5_id INTEGER,
  
  away_player1_id INTEGER,           -- Away team on-court players
  away_player2_id INTEGER,
  away_player3_id INTEGER,
  away_player4_id INTEGER,
  away_player5_id INTEGER,
  
  -- =================================================================
  -- DATA QUALITY AND METADATA
  -- =================================================================
  data_source TEXT,                  -- "NBA_API", "ESPN", etc.
  tracking_data_available BOOLEAN,   -- Whether advanced tracking exists
  video_available BOOLEAN,           -- Whether video replay exists
  
  -- =================================================================
  -- PRIMARY KEY (Handles all scenarios)
  -- =================================================================
  PRIMARY KEY (game_id, play_id)
);

-- =============================================================================
-- PLAYER_TRACKING TABLE (Advanced Movement and Performance Data)
-- NBA's exclusive tracking data - impossible for Claude to research
-- =============================================================================
DROP TABLE IF EXISTS public.player_tracking CASCADE;
CREATE TABLE public.player_tracking (
  -- Core identifiers
  game_id TEXT NOT NULL,
  player_id INTEGER NOT NULL,
  period INTEGER NOT NULL,
  
  -- Player info
  player_name TEXT,
  team_id INTEGER,
  team_abbrev TEXT,
  position TEXT,                      -- "Guard", "Forward", "Center"
  
  -- Movement metrics (per period)
  distance_traveled REAL,            -- Total distance in feet
  avg_speed_mph REAL,                -- Average movement speed
  max_speed_mph REAL,                -- Peak speed reached
  
  -- Possession metrics
  touches INTEGER,                   -- Times player touched ball
  time_of_possession_seconds REAL,  -- Total time holding ball
  avg_dribbles_per_touch REAL,      -- Ball handling intensity
  
  -- Defensive metrics
  deflections INTEGER,               -- Ball deflections caused
  loose_balls_recovered INTEGER,    -- Hustle stat
  charges_drawn INTEGER,             -- Taking charges
  contests INTEGER,                  -- Shot contests
  
  -- Offensive metrics  
  drives INTEGER,                    -- Drives to basket
  drive_success_rate REAL,          -- Successful drive percentage
  catch_and_shoot_attempts INTEGER, -- C&S opportunities
  pull_up_attempts INTEGER,         -- Pull-up shot attempts
  
  -- Efficiency metrics
  points_per_touch REAL,            -- Scoring efficiency
  assist_to_pass_ratio REAL,        -- Playmaking efficiency
  turnover_rate REAL,               -- Turnover percentage
  
  -- Advanced positioning
  time_in_paint_seconds REAL,       -- Time spent in paint
  time_beyond_arc_seconds REAL,     -- Time spent beyond 3pt line
  avg_distance_from_basket REAL,    -- Average court position
  
  -- Fatigue indicators
  speed_decline_rate REAL,          -- Speed decrease over game
  fourth_quarter_efficiency REAL,   -- Late-game performance drop
  
  -- Data quality
  tracking_coverage_pct REAL,       -- % of period with tracking data
  
  PRIMARY KEY (game_id, player_id, period)
);

-- =============================================================================
-- SHOT_CHART TABLE (Detailed Shot Analysis)
-- Every shot attempt with full context - key for player props
-- =============================================================================
DROP TABLE IF EXISTS public.shot_chart CASCADE;
CREATE TABLE public.shot_chart (
  -- Core identifiers
  game_id TEXT NOT NULL,
  shot_id TEXT NOT NULL,             -- Unique shot identifier
  period INTEGER NOT NULL,
  clock_remaining TEXT,
  
  -- Shooter info
  shooter_id INTEGER NOT NULL,
  shooter_name TEXT,
  shooter_team_id INTEGER,
  
  -- Shot details
  shot_made BOOLEAN NOT NULL,
  shot_type TEXT NOT NULL,           -- "2PT", "3PT"
  shot_value INTEGER NOT NULL,       -- Points if made (2 or 3)
  action_type TEXT,                  -- "Jump Shot", "Layup", "Dunk"
  
  -- Shot location (precise coordinates)
  loc_x INTEGER,                     -- NBA coordinate system
  loc_y INTEGER,
  zone_name TEXT,                    -- "Right Wing", "Paint", etc.
  zone_abbrev TEXT,                  -- "RW", "P", etc.
  zone_range TEXT,                   -- Distance category
  shot_distance REAL,               -- Exact distance in feet
  
  -- Game context
  score_margin INTEGER,             -- Team lead/deficit when shot taken
  shot_clock REAL,                  -- Shot clock remaining
  game_clock_remaining INTEGER,     -- Time left in period
  
  -- Defensive context (CRITICAL for shot quality)
  closest_defender_id INTEGER,      -- Nearest defender
  closest_defender_distance REAL,   -- Distance to nearest defender
  closest_defender_name TEXT,
  defender_height_diff REAL,        -- Height difference (shooter - defender)
  
  -- Shot quality metrics
  contest_level TEXT,               -- "Open", "Tight", "Wide Open"
  shot_difficulty INTEGER,          -- 1-5 difficulty scale
  expected_make_pct REAL,          -- Model-predicted make percentage
  points_added REAL,               -- Actual - expected points
  
  -- Play context
  play_type TEXT,                   -- "Isolation", "Pick and Roll", etc.
  assist_player_id INTEGER,        -- Who assisted (if any)
  assist_player_name TEXT,
  secondary_assist_id INTEGER,     -- Hockey assist
  
  -- Shooting form and style
  release_height REAL,             -- Release point height
  release_angle REAL,              -- Arc of shot
  shot_speed_mph REAL,             -- Ball velocity
  
  -- Situational factors
  fast_break BOOLEAN,              -- Whether on fast break
  second_chance BOOLEAN,           -- Whether off offensive rebound
  clutch_shot BOOLEAN GENERATED ALWAYS AS (
    period >= 4 AND game_clock_remaining <= 300 AND ABS(score_margin) <= 5
  ) STORED,
  
  -- Performance streaks
  shooter_hot_streak BOOLEAN,      -- Made 3+ in a row recently
  shooter_cold_streak BOOLEAN,     -- Missed 4+ in a row recently
  
  PRIMARY KEY (game_id, shot_id)
);

-- =============================================================================
-- BOX_SCORES TABLE (Traditional and Advanced Stats)
-- Player performance for props and team analysis
-- =============================================================================
DROP TABLE IF EXISTS public.box_scores CASCADE;
CREATE TABLE public.box_scores (
  -- Core identifiers
  game_id TEXT NOT NULL,
  player_id INTEGER NOT NULL,
  team_id INTEGER NOT NULL,
  
  -- Player info
  player_name TEXT,
  team_abbrev TEXT,
  position TEXT,
  starter BOOLEAN,
  
  -- Traditional stats
  minutes_played REAL,
  field_goals_made INTEGER,
  field_goals_attempted INTEGER,
  field_goal_pct REAL,
  three_pointers_made INTEGER,
  three_pointers_attempted INTEGER,
  three_point_pct REAL,
  free_throws_made INTEGER,
  free_throws_attempted INTEGER,
  free_throw_pct REAL,
  
  offensive_rebounds INTEGER,
  defensive_rebounds INTEGER,
  total_rebounds INTEGER,
  assists INTEGER,
  steals INTEGER,
  blocks INTEGER,
  turnovers INTEGER,
  personal_fouls INTEGER,
  points INTEGER,
  
  -- Advanced stats (NBA's sophisticated metrics)
  plus_minus INTEGER,               -- +/- while on court
  true_shooting_pct REAL,          -- TS% - shooting efficiency
  effective_fg_pct REAL,           -- eFG% - accounts for 3PT value
  usage_rate REAL,                 -- % of possessions used
  pace REAL,                       -- Team pace while on court
  offensive_rating REAL,           -- Points per 100 possessions
  defensive_rating REAL,           -- Opponent points per 100 poss
  net_rating REAL,                 -- Offensive - Defensive rating
  
  -- Shooting efficiency by zone
  restricted_area_fg_made INTEGER,
  restricted_area_fg_att INTEGER,
  paint_fg_made INTEGER,
  paint_fg_att INTEGER,
  mid_range_fg_made INTEGER,
  mid_range_fg_att INTEGER,
  three_point_fg_made INTEGER,
  three_point_fg_att INTEGER,
  
  -- Hustle stats
  deflections INTEGER,
  loose_balls_recovered INTEGER,
  charges_drawn INTEGER,
  screen_assists INTEGER,
  screen_assist_points INTEGER,
  
  -- Player tracking derived
  distance_traveled REAL,
  avg_speed REAL,
  touches INTEGER,
  time_of_possession REAL,
  
  -- Betting-relevant flags
  double_double BOOLEAN GENERATED ALWAYS AS (
    (points >= 10)::INTEGER + (total_rebounds >= 10)::INTEGER + 
    (assists >= 10)::INTEGER + (steals >= 10)::INTEGER + (blocks >= 10)::INTEGER >= 2
  ) STORED,
  
  triple_double BOOLEAN GENERATED ALWAYS AS (
    (points >= 10)::INTEGER + (total_rebounds >= 10)::INTEGER + 
    (assists >= 10)::INTEGER + (steals >= 10)::INTEGER + (blocks >= 10)::INTEGER >= 3
  ) STORED,
  
  PRIMARY KEY (game_id, player_id)
);

-- =============================================================================
-- LINEUPS TABLE (On-Court Combinations)
-- Track which 5-man units were on court - critical for +/- analysis
-- =============================================================================
DROP TABLE IF EXISTS public.lineups CASCADE;
CREATE TABLE public.lineups (
  -- Core identifiers
  game_id TEXT NOT NULL,
  lineup_id TEXT NOT NULL,          -- Unique identifier for 5-man unit
  team_id INTEGER NOT NULL,
  period INTEGER NOT NULL,
  
  -- Lineup composition (5 players on court)
  player1_id INTEGER NOT NULL,
  player2_id INTEGER NOT NULL,  
  player3_id INTEGER NOT NULL,
  player4_id INTEGER NOT NULL,
  player5_id INTEGER NOT NULL,
  
  player1_name TEXT,
  player2_name TEXT,
  player3_name TEXT,
  player4_name TEXT,
  player5_name TEXT,
  
  -- Time on court
  start_time_seconds INTEGER,      -- When lineup entered
  end_time_seconds INTEGER,        -- When lineup exited
  duration_seconds INTEGER,        -- Time together
  
  -- Performance while on court
  points_for INTEGER,              -- Points scored by this lineup
  points_against INTEGER,          -- Points allowed
  plus_minus INTEGER,              -- Net rating
  possessions_for INTEGER,         -- Offensive possessions
  possessions_against INTEGER,     -- Defensive possessions
  
  -- Advanced metrics
  offensive_rating REAL,           -- Points per 100 possessions
  defensive_rating REAL,           -- Opponent points per 100 poss  
  pace REAL,                       -- Possessions per minute
  
  -- Lineup characteristics
  avg_height REAL,                 -- Average height of lineup
  total_salary INTEGER,            -- Combined salary (if available)
  experience_total INTEGER,        -- Combined years of experience
  
  -- Context flags
  starting_lineup BOOLEAN,         -- Whether this was starting 5
  closing_lineup BOOLEAN,          -- Whether lineup finished game
  clutch_lineup BOOLEAN,           -- Used in clutch situations
  
  PRIMARY KEY (game_id, lineup_id, team_id)
);

-- =============================================================================
-- RECENT_STATS TABLE (Pre-calculated Trends)  
-- Performance trends over recent games - eliminates complex calculations
-- =============================================================================
DROP TABLE IF EXISTS public.recent_stats CASCADE;
CREATE TABLE public.recent_stats (
  -- Core identifiers
  stat_date DATE NOT NULL,
  player_id INTEGER NOT NULL,
  stat_window TEXT NOT NULL,        -- "last_5", "last_10", "season", "home_only"
  
  -- Sample information
  games_played INTEGER,
  date_range_start DATE,
  date_range_end DATE,
  total_minutes REAL,
  
  -- Scoring trends
  avg_points REAL,
  avg_field_goals_made REAL,
  avg_three_pointers_made REAL,
  avg_free_throws_made REAL,
  true_shooting_pct REAL,
  
  -- Rebounding and playmaking
  avg_rebounds REAL,
  avg_assists REAL,
  avg_steals REAL,
  avg_blocks REAL,
  avg_turnovers REAL,
  
  -- Advanced metrics trends
  avg_usage_rate REAL,
  avg_plus_minus REAL,
  avg_pace REAL,
  
  -- Shooting by location trends
  restricted_area_pct REAL,
  paint_fg_pct REAL,
  mid_range_fg_pct REAL,
  three_point_pct REAL,
  
  -- Performance indicators
  hot_streak BOOLEAN,              -- Exceeding season averages significantly
  cold_streak BOOLEAN,             -- Below season averages significantly
  usage_spike BOOLEAN,             -- Usage rate 25%+ above normal
  efficiency_peak BOOLEAN,         -- TS% significantly above average
  
  -- Prop betting helpers
  points_over_under_15_5 TEXT,     -- "OVER" or "UNDER" based on average
  rebounds_over_under_8_5 TEXT,
  assists_over_under_5_5 TEXT,
  double_double_rate REAL,         -- % of recent games with double-double
  
  -- Matchup context
  vs_position_avg_points REAL,     -- Avg vs same position opponents
  home_road_split TEXT,            -- "HOME" or "ROAD"
  rest_context TEXT,               -- "RESTED", "B2B", "3IN4"
  
  PRIMARY KEY (stat_date, player_id, stat_window)
);

-- =============================================================================
-- COMPREHENSIVE INDEXES FOR NBA PERFORMANCE
-- =============================================================================

-- Game_info indexes
CREATE INDEX idx_game_info_date ON game_info(game_date);
CREATE INDEX idx_game_info_teams ON game_info(home_team_abbrev, away_team_abbrev);
CREATE INDEX idx_game_info_season ON game_info(season_year, season_type);
CREATE INDEX idx_game_info_overtime ON game_info(overtime_periods) WHERE overtime_periods > 0;
CREATE INDEX idx_game_info_b2b ON game_info(home_back_to_back, away_back_to_back);

-- Plays table indexes (core performance)
CREATE INDEX idx_plays_game ON plays(game_id);
CREATE INDEX idx_plays_player1 ON plays(player1_id) WHERE player1_id IS NOT NULL;
CREATE INDEX idx_plays_event_type ON plays(event_type);
CREATE INDEX idx_plays_period ON plays(period);
CREATE INDEX idx_plays_clutch ON plays(is_clutch_time) WHERE is_clutch_time = true;

-- Shot analysis indexes
CREATE INDEX idx_plays_shots ON plays(shot_attempted) WHERE shot_attempted = true;
CREATE INDEX idx_plays_shot_distance ON plays(shot_distance) WHERE shot_distance IS NOT NULL;
CREATE INDEX idx_plays_shot_quality ON plays(shot_probability) WHERE shot_probability IS NOT NULL;

-- Shot_chart indexes
CREATE INDEX idx_shot_chart_game ON shot_chart(game_id);
CREATE INDEX idx_shot_chart_shooter ON shot_chart(shooter_id);
CREATE INDEX idx_shot_chart_distance ON shot_chart(shot_distance);
CREATE INDEX idx_shot_chart_contest ON shot_chart(contest_level);
CREATE INDEX idx_shot_chart_clutch ON shot_chart(clutch_shot) WHERE clutch_shot = true;

-- Player tracking indexes  
CREATE INDEX idx_tracking_game_player ON player_tracking(game_id, player_id);
CREATE INDEX idx_tracking_distance ON player_tracking(distance_traveled);
CREATE INDEX idx_tracking_touches ON player_tracking(touches);

-- Box scores indexes
CREATE INDEX idx_box_scores_game ON box_scores(game_id);
CREATE INDEX idx_box_scores_player ON box_scores(player_id);
CREATE INDEX idx_box_scores_dd ON box_scores(double_double) WHERE double_double = true;
CREATE INDEX idx_box_scores_td ON box_scores(triple_double) WHERE triple_double = true;

-- Lineups indexes
CREATE INDEX idx_lineups_game ON lineups(game_id);
CREATE INDEX idx_lineups_team ON lineups(team_id);
CREATE INDEX idx_lineups_plus_minus ON lineups(plus_minus);

-- Recent stats indexes  
CREATE INDEX idx_recent_stats_player_date ON recent_stats(player_id, stat_date DESC);
CREATE INDEX idx_recent_stats_window ON recent_stats(stat_window);
CREATE INDEX idx_recent_stats_hot_cold ON recent_stats(hot_streak, cold_streak);

-- =============================================================================
-- DATA VALIDATION FUNCTIONS
-- =============================================================================

CREATE OR REPLACE FUNCTION validate_nba_data_quality()
RETURNS TABLE(
    validation_check TEXT,
    status TEXT,
    details TEXT
) AS $$
BEGIN
    -- Check shot tracking data coverage
    RETURN QUERY
    SELECT 
        'Shot Tracking Coverage'::TEXT,
        CASE 
            WHEN COUNT(*) FILTER (WHERE shot_distance IS NOT NULL) > 0 
            THEN 'PASS'::TEXT 
            ELSE 'WARNING'::TEXT 
        END,
        FORMAT('Shot distance available for %s%% of shot attempts', 
               ROUND(100.0 * COUNT(*) FILTER (WHERE shot_distance IS NOT NULL) / 
                     NULLIF(COUNT(*) FILTER (WHERE shot_attempted = true), 0), 1))::TEXT
    FROM plays
    WHERE game_id IN (
        SELECT game_id FROM game_info WHERE game_date >= CURRENT_DATE - INTERVAL '7 days'
    );
    
    -- Check player tracking data quality
    RETURN QUERY
    SELECT 
        'Player Tracking Data'::TEXT,
        CASE 
            WHEN COUNT(*) > 0 THEN 'PASS'::TEXT 
            ELSE 'WARNING'::TEXT 
        END,
        FORMAT('Tracking data available for %s players in recent games', COUNT(*))::TEXT
    FROM player_tracking pt
    JOIN game_info gi ON pt.game_id = gi.game_id
    WHERE gi.game_date >= CURRENT_DATE - INTERVAL '7 days'
    AND pt.distance_traveled IS NOT NULL;
    
    -- Check lineup data completeness
    RETURN QUERY
    SELECT 
        'Lineup Data'::TEXT,
        CASE 
            WHEN COUNT(*) > 0 THEN 'PASS'::TEXT 
            ELSE 'WARNING'::TEXT 
        END,
        FORMAT('%s unique lineups tracked in recent games', COUNT(*))::TEXT
    FROM lineups l
    JOIN game_info gi ON l.game_id = gi.game_id
    WHERE gi.game_date >= CURRENT_DATE - INTERVAL '7 days';
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================
COMMENT ON TABLE plays IS 'Comprehensive NBA play-by-play data with advanced tracking metrics - the core data goldmine';
COMMENT ON TABLE player_tracking IS 'Advanced movement and performance tracking data impossible to research elsewhere';
COMMENT ON TABLE shot_chart IS 'Detailed shot analysis with defensive context and quality metrics';
COMMENT ON TABLE box_scores IS 'Traditional and advanced player statistics for props and analysis';
COMMENT ON TABLE lineups IS 'On-court combinations and their performance - critical for +/- analysis';
COMMENT ON TABLE recent_stats IS 'Pre-calculated performance trends eliminating complex on-the-fly calculations';

COMMENT ON COLUMN plays.shot_probability IS 'Model-predicted shot make probability based on location and defense';
COMMENT ON COLUMN plays.closest_defender_distance IS 'Distance to nearest defender when shot taken - key for shot quality';
COMMENT ON COLUMN plays.leverage_score IS 'How important/impactful this play was for game outcome (0-10 scale)';
COMMENT ON COLUMN player_tracking.distance_traveled IS 'Total distance traveled in feet during period - fatigue indicator';
COMMENT ON COLUMN shot_chart.contest_level IS 'Open/Tight/Wide Open classification based on defender proximity';
COMMENT ON COLUMN lineups.offensive_rating IS 'Points scored per 100 possessions while this lineup was on court';

-- Schema version tracking
INSERT INTO schema_info (version, description) 
VALUES ('1.0.0-nba-complete', 'Complete NBA schema with comprehensive tracking and shot data')
ON CONFLICT (version) DO UPDATE SET 
    description = EXCLUDED.description,
    applied_at = CURRENT_TIMESTAMP;

SELECT 'NBA SCHEMA CREATION COMPLETE: Comprehensive tracking data and shot analysis ready for betting insights' AS status;