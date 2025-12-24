"""Calculate performance scores (p_score) for OWC 2025 players

This script reads match data from the osu! API and calculates performance scores
for each player based on the formula:

    pscore = (Σ(i=1 to n) Si/Mi) / n · √(n / Σ(j=1 to m) Nj)

where:
    n = amount of maps played by the player
    S = player score on a map
    M = median score on a map
    m = amount of matches played by the player
    N = mean amount of maps played (per player) in a match

The p_score is calculated as a weighted mean across all matches a player participates in.

Usage:
    python calculate_pscores.py --matches 119719487 119959585 120123456
    python calculate_pscores.py --match-file matches.txt
"""

import argparse
import json
import logging
import math
import os
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

try:
    import httpx
except ImportError:
    raise SystemExit("Missing dependency: please install httpx (pip install httpx)")

# Load environment variables
_here = Path(__file__).resolve().parent
load_dotenv(dotenv_path=_here.parent / ".env")

CLIENT_ID = os.getenv("OSU_CLIENT_ID1")
CLIENT_SECRET = os.getenv("OSU_CLIENT_SECRET1")
if not CLIENT_ID or not CLIENT_SECRET:
    raise SystemExit("Error: OSU_CLIENT_ID1 and OSU_CLIENT_SECRET1 required in backend/.env")

TOKEN_URL = "https://osu.ppy.sh/oauth/token"
API_BASE = "https://osu.ppy.sh/api/v2"

DEFAULT_DB = "players.db"
TABLE_NAME = "2025owc"

log = logging.getLogger("calculate_pscores")


def get_app_token(client_id: str, client_secret: str) -> str:
    """Get OAuth token for osu! API"""
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "public",
    }
    with httpx.Client(timeout=20.0) as client:
        r = client.post(TOKEN_URL, data=data, headers={"Accept": "application/json"})
    if r.status_code != 200:
        raise RuntimeError(f"Failed to obtain token: {r.status_code} {r.text}")
    payload = r.json()
    return payload.get("access_token")


def fetch_match(match_id: int, token: str) -> dict:
    """Fetch match data from osu! API"""
    url = f"{API_BASE}/matches/{match_id}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    with httpx.Client(timeout=30.0) as client:
        r = client.get(url, headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to fetch match {match_id}: {r.status_code} {r.text}")
    return r.json()


def calculate_match_pscore(match_data: dict) -> Dict[int, Tuple[float, int, int]]:
    """
    Calculate p_score for each player in a match.
    
    Returns:
        Dict[user_id, (pscore, maps_played, total_maps_in_match)]
    """
    events = match_data.get("events", [])
    
    # Collect all game events (maps played)
    games = [event for event in events if event.get("game")]
    
    if not games:
        log.warning("No games found in match")
        return {}
    
    # Calculate N: mean amount of maps played per player in this match
    player_map_counts = defaultdict(int)
    
    for event in games:
        game = event.get("game", {})
        scores = game.get("scores", [])
        
        for score_data in scores:
            user_id = score_data.get("user_id")
            if user_id:
                player_map_counts[user_id] += 1
    
    if not player_map_counts:
        log.warning("No player scores found in match")
        return {}
    
    # N = mean maps played per player in this match
    N_mean = statistics.mean(player_map_counts.values()) if player_map_counts else 1
    
    # Calculate p_score for each player
    player_scores = defaultdict(lambda: {"score_ratios": [], "maps_played": 0})
    
    for event in games:
        game = event.get("game", {})
        scores = game.get("scores", [])
        
        # Get all scores for this map to calculate median
        map_scores = [s.get("score", 0) for s in scores if s.get("score")]
        
        if not map_scores:
            continue
        
        median_score = statistics.median(map_scores)
        
        # Avoid division by zero
        if median_score == 0:
            continue
        
        # Calculate S/M for each player on this map
        for score_data in scores:
            user_id = score_data.get("user_id")
            player_score = score_data.get("score", 0)
            
            if user_id and player_score:
                ratio = player_score / median_score
                player_scores[user_id]["score_ratios"].append(ratio)
                player_scores[user_id]["maps_played"] += 1
    
    # Calculate final p_score for each player
    results = {}
    total_maps = len(games)
    
    for user_id, data in player_scores.items():
        score_ratios = data["score_ratios"]
        n = data["maps_played"]  # maps played by this player
        
        if n == 0:
            continue
        
        # pscore = (Σ(Si/Mi) / n) · √(n / N)
        avg_ratio = sum(score_ratios) / n
        normalization = math.sqrt(n / N_mean) if N_mean > 0 else 1
        pscore = avg_ratio * normalization
        
        results[user_id] = (pscore, n, total_maps)
    
    return results


def add_pscore_columns(conn: sqlite3.Connection):
    """Add p_score and matches_played columns if they don't exist"""
    cursor = conn.cursor()
    
    # Add p_score column
    try:
        cursor.execute(f'ALTER TABLE "{TABLE_NAME}" ADD COLUMN p_score REAL DEFAULT 0.0')
        conn.commit()
        log.info("Added 'p_score' column")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower():
            raise
    
    # Add matches_played column
    try:
        cursor.execute(f'ALTER TABLE "{TABLE_NAME}" ADD COLUMN matches_played INTEGER DEFAULT 0')
        conn.commit()
        log.info("Added 'matches_played' column")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower():
            raise
    
    # Add total_maps_played column to track total maps across all matches
    try:
        cursor.execute(f'ALTER TABLE "{TABLE_NAME}" ADD COLUMN total_maps_played INTEGER DEFAULT 0')
        conn.commit()
        log.info("Added 'total_maps_played' column")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower():
            raise


def get_user_id_by_profile_url(conn: sqlite3.Connection, user_id: int) -> Optional[int]:
    """Get database player ID from osu user ID"""
    cursor = conn.cursor()
    cursor.execute(
        f'SELECT id FROM "{TABLE_NAME}" WHERE profile_url LIKE ?',
        (f"%/users/{user_id}%",)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def update_player_pscores(conn: sqlite3.Connection, match_pscores: List[Dict[int, Tuple[float, int, int]]]):
    """
    Update player p_scores in database using weighted mean across matches.
    
    Args:
        match_pscores: List of dicts mapping user_id to (pscore, maps_played, total_maps)
    """
    cursor = conn.cursor()
    
    # Aggregate data across all matches for each player
    player_data = defaultdict(lambda: {
        "db_id": None,
        "pscores": [],  # List of (pscore, weight) tuples
        "matches": 0,
        "total_maps": 0
    })
    
    for match_results in match_pscores:
        for user_id, (pscore, maps_played, total_maps) in match_results.items():
            # Get database ID
            db_id = get_user_id_by_profile_url(conn, user_id)
            if not db_id:
                log.warning(f"User {user_id} not found in database, skipping")
                continue
            
            data = player_data[db_id]
            data["db_id"] = db_id
            data["pscores"].append((pscore, maps_played))  # Weight by maps played
            data["matches"] += 1
            data["total_maps"] += maps_played
    
    # Update each player
    updated = 0
    for db_id, data in player_data.items():
        pscores = data["pscores"]
        
        if not pscores:
            continue
        
        # Calculate weighted mean (weighted by maps played in each match)
        total_weight = sum(weight for _, weight in pscores)
        if total_weight == 0:
            continue
        
        weighted_pscore = sum(score * weight for score, weight in pscores) / total_weight
        
        # Update database
        cursor.execute(
            f'UPDATE "{TABLE_NAME}" SET p_score = ?, matches_played = ?, total_maps_played = ? WHERE id = ?',
            (weighted_pscore, data["matches"], data["total_maps"], db_id)
        )
        updated += 1
        
        log.info(f"Updated player ID {db_id}: p_score={weighted_pscore:.4f}, matches={data['matches']}, maps={data['total_maps']}")
    
    conn.commit()
    log.info(f"Updated {updated} players with p_scores")


def main():
    parser = argparse.ArgumentParser(description="Calculate p_scores for OWC 2025 players")
    parser.add_argument("--matches", nargs="+", type=int, help="Match IDs to process")
    parser.add_argument("--match-file", type=str, help="File containing match IDs (one per line)")
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to SQLite DB file")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(levelname)s: %(message)s"
    )
    
    # Get match IDs
    match_ids = []
    if args.matches:
        match_ids.extend(args.matches)
    if args.match_file:
        with open(args.match_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    try:
                        match_ids.append(int(line))
                    except ValueError:
                        log.warning(f"Invalid match ID: {line}")
    
    if not match_ids:
        log.error("No match IDs provided. Use --matches or --match-file")
        return 1
    
    log.info(f"Processing {len(match_ids)} matches")
    
    # Connect to database
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    add_pscore_columns(conn)
    
    # Get API token
    token = get_app_token(CLIENT_ID, CLIENT_SECRET)
    
    # Fetch and process each match
    all_match_pscores = []
    
    for match_id in match_ids:
        log.info(f"Fetching match {match_id}")
        try:
            match_data = fetch_match(match_id, token)
            match_pscores = calculate_match_pscore(match_data)
            
            if match_pscores:
                all_match_pscores.append(match_pscores)
                log.info(f"Calculated p_scores for {len(match_pscores)} players in match {match_id}")
            else:
                log.warning(f"No p_scores calculated for match {match_id}")
        
        except Exception as e:
            log.error(f"Error processing match {match_id}: {e}")
            continue
    
    # Update database with aggregated p_scores
    if all_match_pscores:
        update_player_pscores(conn, all_match_pscores)
    else:
        log.warning("No p_scores calculated from any matches")
    
    conn.close()
    log.info("Done!")
    return 0


if __name__ == "__main__":
    exit(main())
