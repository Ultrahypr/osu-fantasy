"""Update player costs based on p_scores

This script recalculates player costs using a piecewise formula that adjusts
costs based on performance scores relative to the average.

The formula creates clear separation:
- pscore >= 1.6: Very high performers get major cost increases
- pscore >= 1.3: Good performers get moderate increases
- pscore >= 1.1: Above average get slight increases
- pscore >= 0.9: Average performers stay stable
- pscore < 0.9: Below average get cost decreases

Usage:
    python update_costs_by_pscore.py [--db PATH] [--max-cost 11000] [--dry-run]
"""

import argparse
import logging
import math
import sqlite3
from pathlib import Path
from typing import List, Tuple

DEFAULT_DB = "players.db"
TABLE_NAME = "2025owc"
DEFAULT_MAX_COST = 11000
MIN_COST = 5000

log = logging.getLogger("update_costs")


def round_down_to_100(value: float) -> int:
    """Round down to nearest 100."""
    return math.floor(value / 100) * 100


def calculate_player_value(pscore: float, avg_pscore: float = 1.0) -> float:
    """
    Core function: Map pscore to value multiplier.
    Tuned for subtle cost changes:
    - 1.8 → High value (~1.2x)
    - 1.5 → Good value (~1.08x)
    - 1.3 → Above average (~1.04x)
    - 1.0 → Average (1.0x)
    - <1.0 → Below average (<1.0x)
    """
    ratio = pscore / avg_pscore if avg_pscore > 0 else 1.0
    
    # Piecewise function with very gentle multipliers
    if ratio >= 1.6:
        return 1.0 + (ratio - 1.0) * 0.5  # 0.5x effect (reduced from 1.0)
    elif ratio >= 1.3:
        return 1.0 + (ratio - 1.0) * 0.3  # 0.3x effect (reduced from 0.6)
    elif ratio >= 1.1:
        return 1.0 + (ratio - 1.0) * 0.18  # 0.18x effect (reduced from 0.35)
    elif ratio >= 0.9:
        return 1.0 + (ratio - 1.0) * 0.08  # 0.08x effect (reduced from 0.15)
    else:
        return 1.0 + (ratio - 1.0) * 0.25  # 0.25x effect (reduced from 0.5)


def update_costs(
    current_costs: List[int],
    pscores: List[float],
    total_budget: int,
    max_cost: int = DEFAULT_MAX_COST
) -> Tuple[List[int], int]:
    """
    Calculate new costs based on p_scores with rounding to nearest 100.
    
    Returns:
        Tuple of (new_costs, total_market_value)
    """
    n = len(current_costs)
    if n == 0:
        return [], 0
    
    # Calculate average p_score
    avg_pscore = sum(pscores) / n
    
    # Calculate value multipliers for each player
    multipliers = [calculate_player_value(p, avg_pscore) for p in pscores]
    
    # Weight by current cost (rich get richer effect)
    weights = [mult * cost for mult, cost in zip(multipliers, current_costs)]
    total_weight = sum(weights)
    
    if total_weight == 0:
        # Fallback to equal distribution if weights sum to zero
        equal_share = round_down_to_100(total_budget / n)
        return [max(MIN_COST, min(equal_share, max_cost)) for _ in range(n)], equal_share * n
    
    # Allocate budget proportionally
    target_shares = [total_budget * (w / total_weight) for w in weights]
    
    # Apply caps, floors, and rounding
    new_costs = []
    for target in target_shares:
        # Apply max and min bounds
        price = min(target, max_cost)
        price = max(MIN_COST, price)
        
        # Round DOWN to nearest 100
        price = round_down_to_100(price)
        
        new_costs.append(int(price))
    
    return new_costs, sum(new_costs)


def update_database_costs(
    conn: sqlite3.Connection,
    max_cost: int = DEFAULT_MAX_COST,
    dry_run: bool = False
) -> None:
    """
    Update player costs in database based on their p_scores.
    """
    cursor = conn.cursor()
    
    # Fetch all players with costs and p_scores
    cursor.execute(
        f'SELECT id, username, cost, p_score, matches_played FROM "{TABLE_NAME}" ORDER BY id'
    )
    rows = cursor.fetchall()
    
    if not rows:
        log.warning("No players found in database")
        return
    
    # Filter to only players with p_scores (have played matches)
    players_with_pscores = [
        (row[0], row[1], row[2], row[3], row[4])
        for row in rows
        if row[3] is not None and row[3] > 0 and row[4] is not None and row[4] > 0
    ]
    
    if not players_with_pscores:
        log.warning("No players with p_scores found. Run calculate_pscores.py first.")
        return
    
    # Extract data
    player_ids = [p[0] for p in players_with_pscores]
    player_names = [p[1] for p in players_with_pscores]
    current_costs = [p[2] if p[2] else MIN_COST for p in players_with_pscores]
    pscores = [p[3] for p in players_with_pscores]
    matches_played = [p[4] for p in players_with_pscores]
    
    log.info(f"Updating costs for {len(player_ids)} players with p_scores")
    
    # Calculate total budget (sum of current costs)
    total_budget = sum(current_costs)
    log.info(f"Total budget: {total_budget:,}")
    
    # Calculate new costs
    new_costs, total_market_value = update_costs(current_costs, pscores, total_budget, max_cost)
    
    inflation = ((total_market_value / total_budget) - 1) * 100 if total_budget > 0 else 0
    log.info(f"Total market value: {total_market_value:,}")
    log.info(f"Inflation: {inflation:+.1f}%")
    
    # Show changes
    log.info("\n=== Cost Changes ===")
    for i, player_id in enumerate(player_ids):
        old_cost = current_costs[i]
        new_cost = new_costs[i]
        change = new_cost - old_cost
        change_pct = (change / old_cost) * 100 if old_cost > 0 else 0
        
        log.info(
            f"{player_names[i]:20} | pscore: {pscores[i]:.3f} | matches: {matches_played[i]} | "
            f"{old_cost:>6,} → {new_cost:>6,} ({change:+6,}, {change_pct:+5.1f}%)"
        )
    
    if dry_run:
        log.info("\n[DRY RUN] No changes written to database")
        return
    
    # Update database
    updated = 0
    for player_id, new_cost in zip(player_ids, new_costs):
        cursor.execute(
            f'UPDATE "{TABLE_NAME}" SET cost = ? WHERE id = ?',
            (new_cost, player_id)
        )
        updated += 1
    
    conn.commit()
    log.info(f"\nUpdated {updated} player costs in database")


def main():
    parser = argparse.ArgumentParser(description="Update player costs based on p_scores")
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to SQLite DB file")
    parser.add_argument(
        "--max-cost",
        type=int,
        default=DEFAULT_MAX_COST,
        help="Maximum cost cap (default: 11000)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show changes without updating database"
    )
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(message)s"
    )
    
    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"Database file not found: {db_path}")
        return 1
    
    log.info(f"Reading from {db_path}")
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    try:
        update_database_costs(conn, max_cost=args.max_cost, dry_run=args.dry_run)
    except Exception as e:
        log.error(f"Error updating costs: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()
    
    return 0


if __name__ == "__main__":
    exit(main())
