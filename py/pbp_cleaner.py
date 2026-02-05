#!/usr/bin/env python3
"""
PBP Score Repair Module
========================
Fixes three known data-quality pathologies in BallDontLie play-by-play data
so that Engines 4 (Markov possession sim) and 5 (event-rate micro-sim) can
safely consume the event stream.

Pathologies addressed:
  1. Score changes on non-scoring rows (subs, rebounds, timeouts)
  2. Scoring-flagged rows where the scoreboard doesn't move (FT 1-of-2)
  3. Negative score jumps (post-review corrections applied late)

Pipeline:
  Step A  – Rebuild cumulative score from scoring_play + score_value
  Step B  – Patch metadata defects (missing FT flags, missing score_value)
  Step C  – Reconcile rebuilt score to official box-score finals
  Step D  – Expose repaired score for possession builder

QA Gates (all must pass for Engines 4/5 to activate):
  • neg_jump_fix          == 0      (no negative jumps in repaired stream)
  • score_change_non_scoring_fix == 0  (no phantom scoring on dead-ball rows)
  • final_score_match_rate >= 0.995
  • possession_points_nonneg == 1.0

Usage:
  from py.pbp_cleaner import repair_pbp, run_qa_gates

  pbp_df = pd.read_csv("data/play_by_play_2025-10-22_2026-02-03.csv")
  games_df = pd.read_csv("data/games_2025-10-22_2026-02-03.csv")

  repaired, report = repair_pbp(pbp_df, games_df)
  gates = run_qa_gates(repaired, report)

Author: Jose
"""

import logging
from typing import Tuple

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# =============================================================================
# Step B — Patch metadata defects BEFORE score rebuild
# =============================================================================

def patch_metadata(pbp: pd.DataFrame) -> pd.DataFrame:
    """
    Fix two systematic metadata issues in BDL play-by-play:

    Patch 1: Free-throw rows where scoring_play is incorrectly False
             but the event text says "makes" (or variant).
             → Force scoring_play=True, score_value=1 if missing.

    Patch 2: Made-shot rows where score_value is missing.
             → Infer 3 vs 2 from type/text; default 2 as last resort.

    Returns a copy — original DataFrame is never mutated.
    """
    df = pbp.copy()

    # Ensure columns exist with safe defaults
    if "scoring_play" not in df.columns:
        df["scoring_play"] = False
    if "score_value" not in df.columns:
        df["score_value"] = np.nan

    # Normalise text columns for matching
    type_lower = df["type"].fillna("").str.lower()
    text_lower = df["text"].fillna("").str.lower()

    # ------------------------------------------------------------------
    # Patch 1: FT rows with incorrect scoring_play=False
    # ------------------------------------------------------------------
    is_ft_row = type_lower.str.contains("free throw", na=False)
    text_says_makes = text_lower.str.contains("makes", na=False)
    flag_is_false = df["scoring_play"].fillna(False) == False

    ft_fix_mask = is_ft_row & text_says_makes & flag_is_false
    n_ft_fix = ft_fix_mask.sum()

    df.loc[ft_fix_mask, "scoring_play"] = True
    # Fill missing score_value on these rows (FT = 1 point)
    df.loc[ft_fix_mask & df["score_value"].isna(), "score_value"] = 1

    if n_ft_fix > 0:
        logger.info(f"  Patch 1: fixed {n_ft_fix} FT rows with incorrect scoring_play=False")

    # ------------------------------------------------------------------
    # Patch 2: Made shots where score_value is null
    # ------------------------------------------------------------------
    is_scoring = df["scoring_play"].fillna(False) == True
    val_missing = df["score_value"].isna()
    needs_value = is_scoring & val_missing

    # Infer 3-pointer
    looks_like_three = (
        type_lower.str.contains("3pt|three.?point|3-point|3 point", regex=True, na=False)
        | text_lower.str.contains("3pt|three.?point|3-point|3 point", regex=True, na=False)
    )
    # Infer free throw
    looks_like_ft = is_ft_row | text_lower.str.contains("free throw", na=False)

    df.loc[needs_value & looks_like_three, "score_value"] = 3
    df.loc[needs_value & looks_like_ft, "score_value"] = 1
    # Everything else that's still missing → default 2
    still_missing = is_scoring & df["score_value"].isna()
    df.loc[still_missing, "score_value"] = 2

    n_inferred = needs_value.sum()
    if n_inferred > 0:
        logger.info(f"  Patch 2: inferred score_value on {n_inferred} rows")

    return df


# =============================================================================
# Step A — Rebuild cumulative score from event-level metadata
# =============================================================================

def rebuild_scores(pbp: pd.DataFrame, games: pd.DataFrame) -> pd.DataFrame:
    """
    Build monotonically-increasing home_score_fix / away_score_fix from
    scoring_play + score_value + team_abbr, using the games table to
    determine which team_abbr is home vs away for each game_id.

    The original home_score / away_score columns are renamed to
    home_score_raw / away_score_raw and kept for debugging.

    Returns a new DataFrame (original is not mutated).
    """
    df = pbp.copy()

    # ------------------------------------------------------------------
    # Build a game_id → (home_abbr, visitor_abbr) lookup from games table
    # ------------------------------------------------------------------
    # Support both V1 and V2 column naming conventions
    if "home_team_abbr" in games.columns:
        home_col, away_col = "home_team_abbr", "visitor_team_abbr"
    elif "home_team_abbrev" in games.columns:
        home_col, away_col = "home_team_abbrev", "away_team_abbrev"
    else:
        raise KeyError(
            "Games table must have home_team_abbr/visitor_team_abbr or "
            "home_team_abbrev/away_team_abbrev columns"
        )

    home_lookup = games.set_index("game_id")[home_col].to_dict()

    # ------------------------------------------------------------------
    # Rename original scoreboard columns
    # ------------------------------------------------------------------
    if "home_score" in df.columns:
        df.rename(columns={
            "home_score": "home_score_raw",
            "away_score": "away_score_raw",
        }, inplace=True)

    # ------------------------------------------------------------------
    # Compute per-row point allocation
    # ------------------------------------------------------------------
    is_scoring = df["scoring_play"].fillna(False).astype(bool)
    score_val = df["score_value"].fillna(0).astype(int)
    row_points = np.where(is_scoring, score_val, 0)

    # Map each row's team_abbr to home/away
    row_team = df["team_abbr"].fillna("")
    row_game = df["game_id"]
    row_home_team = row_game.map(home_lookup).fillna("")

    home_points = np.where(row_team == row_home_team, row_points, 0)
    away_points = np.where(
        (row_team != row_home_team) & (row_team != ""), row_points, 0
    )

    df["home_points_row"] = home_points.astype(int)
    df["away_points_row"] = away_points.astype(int)

    # ------------------------------------------------------------------
    # Cumulative sum within each game (respecting row order)
    # ------------------------------------------------------------------
    df = df.sort_values(["game_id", "order"], na_position="last")

    df["home_score_fix"] = df.groupby("game_id")["home_points_row"].cumsum()
    df["away_score_fix"] = df.groupby("game_id")["away_points_row"].cumsum()
    df["margin_fix"] = df["home_score_fix"] - df["away_score_fix"]

    return df


# =============================================================================
# Step C — Reconcile to official final scores
# =============================================================================

def reconcile_finals(
    pbp: pd.DataFrame,
    games: pd.DataFrame,
    tolerance: int = 1,
) -> pd.DataFrame:
    """
    Compare the last rebuilt score per game to the official box-score final.

    Adds column ``pbp_reliable`` (bool) to the returned summary DataFrame.
    Games outside tolerance are flagged unreliable for Engine 4/5 training.

    Returns a per-game summary DataFrame with columns:
      game_id, pbp_home_final, pbp_away_final,
      box_home_final, box_away_final,
      home_diff, away_diff, pbp_reliable
    """
    # Last row per game in the repaired PBP
    pbp_finals = (
        pbp.sort_values(["game_id", "order"])
        .groupby("game_id")
        .agg(
            pbp_home_final=("home_score_fix", "last"),
            pbp_away_final=("away_score_fix", "last"),
        )
        .reset_index()
    )

    # Official finals from games table
    # Handle both column conventions
    if "home_score" in games.columns:
        box_home, box_away = "home_score", "visitor_score"
    elif "home_team_score" in games.columns:
        box_home, box_away = "home_team_score", "away_team_score"
    else:
        raise KeyError("Games table must have home_score or home_team_score column")

    box_finals = games[["game_id", box_home, box_away]].rename(columns={
        box_home: "box_home_final",
        box_away: "box_away_final",
    })

    merged = pbp_finals.merge(box_finals, on="game_id", how="left")
    merged["home_diff"] = (merged["pbp_home_final"] - merged["box_home_final"]).abs()
    merged["away_diff"] = (merged["pbp_away_final"] - merged["box_away_final"]).abs()
    merged["pbp_reliable"] = (
        (merged["home_diff"] <= tolerance) & (merged["away_diff"] <= tolerance)
    )

    n_total = len(merged)
    n_reliable = merged["pbp_reliable"].sum()
    n_bad = n_total - n_reliable
    rate = n_reliable / n_total if n_total > 0 else 0.0

    logger.info(
        f"  Reconciliation: {n_reliable}/{n_total} games match "
        f"({rate:.1%}), {n_bad} flagged unreliable"
    )

    if n_bad > 0:
        bad = merged[~merged["pbp_reliable"]]
        for _, row in bad.head(5).iterrows():
            logger.warning(
                f"    Game {int(row['game_id'])}: "
                f"PBP {int(row['pbp_home_final'])}-{int(row['pbp_away_final'])} vs "
                f"Box {int(row['box_home_final'])}-{int(row['box_away_final'])}"
            )
        if n_bad > 5:
            logger.warning(f"    ... and {n_bad - 5} more")

    return merged


# =============================================================================
# Audit helpers (pre-repair diagnostics)
# =============================================================================

def audit_raw_pbp(pbp: pd.DataFrame) -> dict:
    """
    Run the three diagnostic checks on RAW (pre-repair) PBP and return counts.
    Useful for before/after comparison and logging.
    """
    df = pbp.sort_values(["game_id", "order"]).copy()

    # Previous-row scores within each game
    df["prev_home"] = df.groupby("game_id")["home_score_raw"].shift(1)
    df["prev_away"] = df.groupby("game_id")["away_score_raw"].shift(1)

    # Scoreboard delta
    df["delta_home"] = df["home_score_raw"] - df["prev_home"]
    df["delta_away"] = df["away_score_raw"] - df["prev_away"]
    df["delta_total"] = df["delta_home"].fillna(0) + df["delta_away"].fillna(0)

    # 1) Negative jumps
    neg_mask = (df["delta_home"] < 0) | (df["delta_away"] < 0)
    neg_rows = neg_mask.sum()
    neg_games = df.loc[neg_mask, "game_id"].nunique() if neg_rows > 0 else 0

    # 2) Score change on non-scoring rows
    is_not_scoring = df["scoring_play"].fillna(False) == False
    score_changed = df["delta_total"].abs() > 0
    phantom_mask = is_not_scoring & score_changed & df["prev_home"].notna()
    phantom_rows = phantom_mask.sum()
    phantom_games = df.loc[phantom_mask, "game_id"].nunique() if phantom_rows > 0 else 0

    # 3) Scoring flagged but scoreboard unchanged
    is_scoring = df["scoring_play"].fillna(False) == True
    no_change = df["delta_total"].abs() == 0
    silent_mask = is_scoring & no_change & df["prev_home"].notna()
    silent_rows = silent_mask.sum()
    silent_games = df.loc[silent_mask, "game_id"].nunique() if silent_rows > 0 else 0

    return {
        "negative_jump_rows": int(neg_rows),
        "negative_jump_games": int(neg_games),
        "phantom_scoring_rows": int(phantom_rows),
        "phantom_scoring_games": int(phantom_games),
        "silent_scoring_rows": int(silent_rows),
        "silent_scoring_games": int(silent_games),
        "total_rows": len(df),
        "total_games": df["game_id"].nunique(),
    }


def audit_repaired_pbp(pbp: pd.DataFrame) -> dict:
    """
    Run the same three checks on the REPAIRED score columns.
    All three counts should be 0 if the repair worked correctly.
    """
    df = pbp.sort_values(["game_id", "order"]).copy()

    df["prev_home_fix"] = df.groupby("game_id")["home_score_fix"].shift(1)
    df["prev_away_fix"] = df.groupby("game_id")["away_score_fix"].shift(1)

    df["delta_home_fix"] = df["home_score_fix"] - df["prev_home_fix"]
    df["delta_away_fix"] = df["away_score_fix"] - df["prev_away_fix"]
    df["delta_total_fix"] = df["delta_home_fix"].fillna(0) + df["delta_away_fix"].fillna(0)

    # 1) Negative jumps in repaired stream
    neg_mask = (df["delta_home_fix"] < 0) | (df["delta_away_fix"] < 0)
    neg_rows = neg_mask.sum()

    # 2) Score change on non-scoring rows (in repaired stream)
    is_not_scoring = df["scoring_play"].fillna(False) == False
    score_changed = df["delta_total_fix"].abs() > 0
    phantom_mask = is_not_scoring & score_changed & df["prev_home_fix"].notna()
    phantom_rows = phantom_mask.sum()

    # 3) Scoring flagged but repaired score unchanged
    is_scoring = df["scoring_play"].fillna(False) == True
    no_change = df["delta_total_fix"].abs() == 0
    silent_mask = is_scoring & no_change & df["prev_home_fix"].notna()
    silent_rows = silent_mask.sum()

    return {
        "neg_jump_fix": int(neg_rows),
        "score_change_non_scoring_fix": int(phantom_rows),
        "scoring_no_change_fix": int(silent_rows),
    }


# =============================================================================
# QA Gates — Engines 4/5 only activate when ALL pass
# =============================================================================

def run_qa_gates(
    repaired_pbp: pd.DataFrame,
    reconciliation: pd.DataFrame,
    match_rate_threshold: float = 0.995,
) -> dict:
    """
    Evaluate all QA gates. Returns a dict with gate results and a
    top-level 'engines_4_5_enabled' bool.

    Gates:
      neg_jump_fix              == 0
      score_change_non_scoring_fix == 0
      final_score_match_rate    >= match_rate_threshold
      possession_points_nonneg  == 1.0
    """
    # Post-repair audit
    post = audit_repaired_pbp(repaired_pbp)

    # Final score match rate
    n_total = len(reconciliation)
    n_match = reconciliation["pbp_reliable"].sum() if n_total > 0 else 0
    match_rate = n_match / n_total if n_total > 0 else 0.0

    # Possession points non-negative check
    # (every row's home_points_row and away_points_row should be >= 0)
    home_nonneg = (repaired_pbp["home_points_row"] >= 0).all()
    away_nonneg = (repaired_pbp["away_points_row"] >= 0).all()
    poss_nonneg = 1.0 if (home_nonneg and away_nonneg) else 0.0

    gates = {
        "neg_jump_fix": post["neg_jump_fix"],
        "score_change_non_scoring_fix": post["score_change_non_scoring_fix"],
        "final_score_match_rate": round(match_rate, 6),
        "final_score_match_threshold": match_rate_threshold,
        "possession_points_nonneg": poss_nonneg,
        "reliable_games": int(n_match),
        "total_games": int(n_total),
        "unreliable_games": int(n_total - n_match),
    }

    # Master switch
    gates["engines_4_5_enabled"] = (
        gates["neg_jump_fix"] == 0
        and gates["score_change_non_scoring_fix"] == 0
        and gates["final_score_match_rate"] >= match_rate_threshold
        and gates["possession_points_nonneg"] == 1.0
    )

    return gates


# =============================================================================
# Main entry point — full repair pipeline
# =============================================================================

def repair_pbp(
    pbp: pd.DataFrame,
    games: pd.DataFrame,
    tolerance: int = 1,
    match_rate_threshold: float = 0.995,
    verbose: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Full PBP repair pipeline.

    Args:
        pbp:    Raw play-by-play DataFrame (from flatten_play / CSV)
        games:  Games DataFrame with official final scores
        tolerance: Point tolerance for final-score reconciliation
        match_rate_threshold: Minimum match rate for QA gate
        verbose: Log before/after diagnostics

    Returns:
        (repaired_pbp, reconciliation_df, qa_gates_dict)

    The repaired_pbp DataFrame contains:
        - Original columns (home_score_raw, away_score_raw)
        - Repaired columns (home_score_fix, away_score_fix, margin_fix)
        - Per-row allocation (home_points_row, away_points_row)
        - Patched metadata (scoring_play, score_value may be updated)
    """
    n_rows = len(pbp)
    n_games = pbp["game_id"].nunique()
    logger.info(f"PBP Repair: {n_rows:,} rows, {n_games} games")

    # --- Pre-repair audit ---
    if verbose:
        # Need raw scores still named home_score/away_score for audit
        if "home_score" in pbp.columns:
            audit_df = pbp.rename(columns={
                "home_score": "home_score_raw",
                "away_score": "away_score_raw",
            })
        elif "home_score_raw" in pbp.columns:
            audit_df = pbp
        else:
            audit_df = None

        if audit_df is not None and "home_score_raw" in audit_df.columns:
            pre = audit_raw_pbp(audit_df)
            logger.info("  PRE-REPAIR audit:")
            logger.info(f"    Negative jumps:         {pre['negative_jump_rows']:,} rows / {pre['negative_jump_games']} games")
            logger.info(f"    Phantom scoring:        {pre['phantom_scoring_rows']:,} rows / {pre['phantom_scoring_games']} games")
            logger.info(f"    Silent scoring:         {pre['silent_scoring_rows']:,} rows / {pre['silent_scoring_games']} games")

    # --- Step B: Patch metadata ---
    logger.info("  Step B: Patching metadata defects...")
    patched = patch_metadata(pbp)

    # --- Step A: Rebuild scores ---
    logger.info("  Step A: Rebuilding cumulative scores from event metadata...")
    repaired = rebuild_scores(patched, games)

    # --- Step C: Reconcile to official finals ---
    logger.info("  Step C: Reconciling to official box-score finals...")
    reconciliation = reconcile_finals(repaired, games, tolerance=tolerance)

    # Tag each PBP row with reliability flag
    reliable_games = set(
        reconciliation.loc[reconciliation["pbp_reliable"], "game_id"]
    )
    repaired["pbp_reliable"] = repaired["game_id"].isin(reliable_games)

    # --- QA gates ---
    logger.info("  Running QA gates...")
    gates = run_qa_gates(repaired, reconciliation, match_rate_threshold)

    if verbose:
        logger.info("  POST-REPAIR QA gates:")
        logger.info(f"    neg_jump_fix:                {gates['neg_jump_fix']}")
        logger.info(f"    score_change_non_scoring_fix: {gates['score_change_non_scoring_fix']}")
        logger.info(f"    final_score_match_rate:       {gates['final_score_match_rate']:.4f} (threshold: {gates['final_score_match_threshold']})")
        logger.info(f"    possession_points_nonneg:     {gates['possession_points_nonneg']}")
        logger.info(f"    reliable games:               {gates['reliable_games']}/{gates['total_games']}")

        status = "✅ ENABLED" if gates["engines_4_5_enabled"] else "❌ BLOCKED"
        logger.info(f"    Engines 4/5: {status}")

        if not gates["engines_4_5_enabled"]:
            reasons = []
            if gates["neg_jump_fix"] != 0:
                reasons.append(f"neg_jump_fix={gates['neg_jump_fix']}")
            if gates["score_change_non_scoring_fix"] != 0:
                reasons.append(f"score_change_non_scoring_fix={gates['score_change_non_scoring_fix']}")
            if gates["final_score_match_rate"] < match_rate_threshold:
                reasons.append(f"match_rate={gates['final_score_match_rate']:.4f}")
            if gates["possession_points_nonneg"] != 1.0:
                reasons.append("negative possession points detected")
            logger.info(f"    Block reasons: {', '.join(reasons)}")

    return repaired, reconciliation, gates


# =============================================================================
# CLI — standalone repair + audit
# =============================================================================

def main():
    import argparse
    import sys
    import json

    parser = argparse.ArgumentParser(
        description="PBP Score Repair — fix scoring pathologies for Engines 4/5",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Repair and save
  python py/pbp_cleaner.py --pbp data/play_by_play_2025-10-22_2026-02-03.csv \\
                           --games data/games_2025-10-22_2026-02-03.csv \\
                           --output data/play_by_play_repaired.csv

  # Audit only (no output file)
  python py/pbp_cleaner.py --pbp data/play_by_play_2025-10-22_2026-02-03.csv \\
                           --games data/games_2025-10-22_2026-02-03.csv \\
                           --audit-only

  # With parquet output
  python py/pbp_cleaner.py --pbp data/play_by_play_2025-10-22_2026-02-03.csv \\
                           --games data/games_2025-10-22_2026-02-03.csv \\
                           --output data/play_by_play_repaired.parquet
        """
    )
    parser.add_argument("--pbp", required=True, help="Path to PBP CSV/parquet")
    parser.add_argument("--games", required=True, help="Path to games CSV/parquet")
    parser.add_argument("--output", help="Output path for repaired PBP (CSV or parquet)")
    parser.add_argument("--audit-only", action="store_true", help="Only run audit, no repair")
    parser.add_argument("--tolerance", type=int, default=1, help="Score reconciliation tolerance (default: 1)")
    parser.add_argument("--threshold", type=float, default=0.995, help="Match rate threshold (default: 0.995)")
    parser.add_argument("--save-report", help="Save QA report as JSON")
    parser.add_argument("--verbose", action="store_true", default=True)

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )

    # Load data
    logger.info("Loading data...")
    pbp_path = args.pbp
    games_path = args.games

    pbp = pd.read_parquet(pbp_path) if pbp_path.endswith(".parquet") else pd.read_csv(pbp_path)
    games = pd.read_parquet(games_path) if games_path.endswith(".parquet") else pd.read_csv(games_path)

    logger.info(f"  PBP: {len(pbp):,} rows, {pbp['game_id'].nunique()} games")
    logger.info(f"  Games: {len(games):,} rows")

    if args.audit_only:
        # Just run pre-repair audit
        if "home_score_raw" not in pbp.columns and "home_score" in pbp.columns:
            pbp = pbp.rename(columns={
                "home_score": "home_score_raw",
                "away_score": "away_score_raw",
            })
        if "home_score_raw" in pbp.columns:
            pre = audit_raw_pbp(pbp)
            print("\n📊 RAW PBP AUDIT")
            print("=" * 50)
            print(f"  Total rows:        {pre['total_rows']:,}")
            print(f"  Total games:       {pre['total_games']}")
            print(f"  Negative jumps:    {pre['negative_jump_rows']:,} rows / {pre['negative_jump_games']} games")
            print(f"  Phantom scoring:   {pre['phantom_scoring_rows']:,} rows / {pre['phantom_scoring_games']} games")
            print(f"  Silent scoring:    {pre['silent_scoring_rows']:,} rows / {pre['silent_scoring_games']} games")
        else:
            print("❌ PBP has no home_score or home_score_raw column to audit")
        return

    # Full repair
    print("\n🔧 PBP SCORE REPAIR")
    print("=" * 50)

    repaired, reconciliation, gates = repair_pbp(
        pbp, games,
        tolerance=args.tolerance,
        match_rate_threshold=args.threshold,
        verbose=args.verbose,
    )

    # Print summary
    print("\n📊 QA GATES")
    print("=" * 50)
    for key, val in gates.items():
        icon = ""
        if key == "engines_4_5_enabled":
            icon = "✅" if val else "❌"
        print(f"  {key}: {val} {icon}")

    # Save repaired PBP
    if args.output:
        if args.output.endswith(".parquet"):
            repaired.to_parquet(args.output, index=False)
        else:
            repaired.to_csv(args.output, index=False)
        print(f"\n✅ Repaired PBP saved to {args.output}")
        print(f"   {len(repaired):,} rows, {repaired['game_id'].nunique()} games")
        print(f"   Reliable games: {gates['reliable_games']}/{gates['total_games']}")

    # Save QA report
    if args.save_report:
        with open(args.save_report, "w") as f:
            json.dump(gates, f, indent=2)
        print(f"   QA report saved to {args.save_report}")


if __name__ == "__main__":
    main()