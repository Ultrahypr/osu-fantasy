"""Update playing status for OWC 2025 players

This script adds a 'playing' field to the players database and marks players
from countries that are actively competing in the current stage of the tournament.

Usage:
    python update_playing_status.py [--db PATH]
"""

import argparse
import sqlite3
import logging
from pathlib import Path

DEFAULT_DB = "players.db"
TABLE_NAME = "2025owc"

# Countries actively playing in current stage (from match results)
PLAYING_COUNTRIES = {
    "Vietnam",
    "Malaysia",
    "New Zealand",
    "Finland",
    "Taiwan",
    "Argentina",
    "Belgium",
    "France",
    "Italy",
    "Chile",
    "Spain",
    "Brazil",
    "Sweden",
    "Japan",
    "Greece",
    "China",
    "Thailand",
    "Philippines",
    "Netherlands",
    "Indonesia",
    "Denmark",
    "Singapore",
    "Hong Kong",
    "Türkiye",
    "Turkey",  # Alternative name for Türkiye
    "Ukraine",
    "Portugal",
    "Peru",
}

log = logging.getLogger("update_playing")


def add_playing_column(conn: sqlite3.Connection):
    """Add 'playing' column to the table if it doesn't exist"""
    try:
        cursor = conn.cursor()
        cursor.execute(f'ALTER TABLE "{TABLE_NAME}" ADD COLUMN playing INTEGER DEFAULT 0')
        conn.commit()
        log.info("Added 'playing' column to table")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            log.info("'playing' column already exists")
        else:
            raise


def update_playing_status(conn: sqlite3.Connection):
    """Update playing status based on country"""
    cursor = conn.cursor()
    
    # First, set all players to not playing
    cursor.execute(f'UPDATE "{TABLE_NAME}" SET playing = 0')
    log.info("Reset all players to not playing")
    
    # Get all unique countries in the database
    cursor.execute(f'SELECT DISTINCT country FROM "{TABLE_NAME}" WHERE country IS NOT NULL')
    db_countries = [row[0] for row in cursor.fetchall()]
    
    # Update players from playing countries
    updated_count = 0
    matched_countries = []
    
    for country in db_countries:
        # Check if this country (or a variation) is in the playing list
        is_playing = False
        for playing_country in PLAYING_COUNTRIES:
            # Case-insensitive matching and handle variations
            if country.lower() == playing_country.lower():
                is_playing = True
                break
            # Handle country codes and variations
            if country.upper() in ["TUR", "TR"] and playing_country in ["Türkiye", "Turkey"]:
                is_playing = True
                break
        
        if is_playing:
            cursor.execute(f'UPDATE "{TABLE_NAME}" SET playing = 1 WHERE country = ?', (country,))
            count = cursor.rowcount
            updated_count += count
            matched_countries.append(country)
            log.info(f"Marked {count} players from {country} as playing")
    
    conn.commit()
    log.info(f"Total: Marked {updated_count} players as playing from {len(matched_countries)} countries")
    log.info(f"Countries marked as playing: {', '.join(sorted(matched_countries))}")
    
    # Report countries in PLAYING_COUNTRIES that weren't found in DB
    unmatched = []
    for playing_country in PLAYING_COUNTRIES:
        found = False
        for db_country in db_countries:
            if db_country.lower() == playing_country.lower():
                found = True
                break
        if not found and playing_country != "Turkey":  # Skip "Turkey" as it's just an alias for Türkiye
            unmatched.append(playing_country)
    
    if unmatched:
        log.warning(f"Countries in match list but not found in database: {', '.join(sorted(unmatched))}")


def main():
    parser = argparse.ArgumentParser(description="Update playing status for OWC 2025 players")
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to SQLite DB file (default: players.db)")
    parser.add_argument("--quiet", action="store_true", help="Suppress info messages")
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(levelname)s: %(message)s"
    )
    
    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"Database file not found: {db_path}")
        return 1
    
    log.info(f"Updating playing status in {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    try:
        add_playing_column(conn)
        update_playing_status(conn)
        log.info("Done!")
    except Exception as e:
        log.error(f"Error updating database: {e}")
        return 1
    finally:
        conn.close()
    
    return 0


if __name__ == "__main__":
    exit(main())
