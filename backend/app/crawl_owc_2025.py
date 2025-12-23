"""Crawler for OWC 2025 participants

This script fetches the participants listed on the OWC 2025 wiki page and
scrapes each player's osu! profile for username and avatar URL.
The country is extracted directly from the wiki list "Country" column.
Results are stored in a SQLite database `players.db` in the same folder by default
in a table named `2025owc`.

Usage:
    python crawl_owc_2025.py [--db PATH] [--recreate] [--limit N]
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import time
from datetime import datetime
from typing import List, Optional, Dict
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Default wiki URL (provided by the user)
DEFAULT_PAGE = "https://osu.ppy.sh/wiki/en/Tournaments/OWC/2025"
DEFAULT_DB = "players.db"
TABLE_NAME = "2025owc"

HEADERS = {
    "User-Agent": "osu-owc-crawler/1.0 (+https://github.com/)",
}

log = logging.getLogger("owc_crawler")


def get_soup(url: str, session: Optional[requests.Session] = None, timeout: int = 15) -> BeautifulSoup:
    if session is None:
        session = requests.Session()
    resp = session.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


def find_participants_section(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    # Find heading that contains 'Participants' (case-insensitive), then return a container
    for heading in soup.find_all(re.compile(r"^h[1-6]$")):
        if heading.get_text(strip=True).lower().startswith("participants") or "participants" in heading.get_text(strip=True).lower():
            # collect sibling tags until the next heading of equal or higher level
            contents = []
            for sib in heading.next_siblings:
                if sib.name and re.match(r"^h[1-6]$", getattr(sib, "name", "")):
                    break
                contents.append(sib)
            wrapper = BeautifulSoup("", "lxml")
            # join HTML of all contents
            html = "".join(str(x) for x in contents)
            wrapper.append(BeautifulSoup(html, "lxml"))
            return wrapper
    return None


def extract_country_from_cell(cell) -> Optional[str]:
    """
    Extract country name from a table cell. Handles various wiki formats:
    - Flag images with alt/title text
    - Text spans next to flags
    - Direct text content
    """
    # First, look for flag images
    for img in cell.find_all("img"):
        cls = " ".join(img.get("class", [])) if img.get("class") else ""
        src = img.get("src", "")

        # Check if this is a flag image
        if any(k in cls.lower() for k in ("flag", "country")) or \
           any(k in src.lower() for k in ("flag", "/flags/", "/countries/")):
            # Try alt text
            if img.get("alt"):
                return img.get("alt").strip()
            # Try title
            if img.get("title"):
                return img.get("title").strip()
            # Try to extract country code from src
            m = re.search(r"/([A-Z]{2})(?:\.png|\.svg|\.gif)?(?:\?|$)", src, re.I)
            if m:
                return m.group(1).upper()

    # Look for a span or link with country name near a flag
    for span in cell.find_all(["span", "a"]):
        text = span.get_text(strip=True)
        # Skip if it's a player link
        href = span.get("href", "") if span.name == "a" else ""
        if "/users/" in href:
            continue
        if text and len(text) > 1 and len(text) < 50:
            # Check if it looks like a country name (not a number or very short code)
            if not text.isdigit() and not re.match(r"^#?\d+$", text):
                return text

    # Fallback to cell text (excluding player names)
    text = cell.get_text(strip=True)
    # Clean up - remove player-like text
    if text and len(text) > 1 and len(text) < 50:
        if not text.isdigit():
            return text

    return None


def extract_players_with_countries(section_soup: BeautifulSoup, base_url: str = "https://osu.ppy.sh") -> List[Dict[str, Optional[str]]]:
    """
    Parses the participants table to extract (profile_url, country).
    Assumes the table layout: [Country] | [Members...]
    """
    results = []
    seen_urls = set()
    found_table_data = False

    # Look for tables within the participants section
    tables = section_soup.find_all("table")

    for table in tables:
        # Iterate over all rows
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])

            # We need at least 2 columns: Country and Members
            if len(cells) < 2:
                continue

            # Check for header row (skip if first cell says "Country")
            first_cell_text = cells[0].get_text(strip=True)
            if "country" in first_cell_text.lower() or "team" in first_cell_text.lower():
                continue

            # Column 0: Country Name - use improved extraction
            country_name = extract_country_from_cell(cells[0])

            # If the country cell is empty, try to get it from flag in the same row
            if not country_name:
                # Look for any flag image in the entire row
                for img in row.find_all("img"):
                    cls = " ".join(img.get("class", [])) if img.get("class") else ""
                    src = img.get("src", "")
                    if any(k in cls.lower() for k in ("flag", "country")) or \
                       any(k in src.lower() for k in ("flag", "/flags/")):
                        if img.get("alt"):
                            country_name = img.get("alt").strip()
                            break
                        if img.get("title"):
                            country_name = img.get("title").strip()
                            break

            # Column 1 (and onwards): Member links
            # We iterate all subsequent cells just in case of colspan or extra columns
            for cell in cells[1:]:
                for a in cell.find_all("a", href=True):
                    href = a["href"]
                    if "/users/" in href:
                        full = urljoin(base_url, href)

                        if full not in seen_urls:
                            results.append({
                                "profile_url": full,
                                "country": country_name
                            })
                            seen_urls.add(full)
                            found_table_data = True

    # Fallback: If no tables were found or no links extracted from tables,
    # revert to grabbing all links in the section without country data.
    if not found_table_data:
        log.warning("No structured table found in Participants section. Falling back to raw link extraction (Countries will be empty).")
        for a in section_soup.find_all("a", href=True):
            href = a["href"]
            if "/users/" in href:
                full = urljoin(base_url, href)
                if full not in seen_urls:
                    results.append({
                        "profile_url": full,
                        "country": None
                    })
                    seen_urls.add(full)

    # Sort for consistent order
    return sorted(results, key=lambda x: x["profile_url"])


def parse_profile_page(profile_url: str, session: requests.Session, known_country: Optional[str] = None) -> dict:
    import json as json_lib
    data = {"username": None, "profile_url": profile_url, "avatar_url": None, "country": known_country}
    try:
        soup = get_soup(profile_url, session=session)
    except Exception as e:
        log.warning("Failed to fetch profile %s: %s", profile_url, e)
        return data

    # Try to extract user data from embedded JSON (osu! pages often have this)
    def extract_from_json(soup: BeautifulSoup) -> dict:
        result = {}
        # Look for script tags with JSON data
        for script in soup.find_all("script", type="application/json"):
            try:
                json_data = json_lib.loads(script.string)
                # Look for user data in various possible locations
                user = None
                if isinstance(json_data, dict):
                    user = json_data.get("user") or json_data.get("currentUser")
                    if not user and "props" in json_data:
                        props = json_data.get("props", {})
                        if isinstance(props, dict):
                            user = props.get("user") or props.get("pageProps", {}).get("user")
                if user and isinstance(user, dict):
                    if user.get("username"):
                        result["username"] = user["username"]
                    if user.get("avatar_url"):
                        result["avatar_url"] = user["avatar_url"]
                    if user.get("country"):
                        country_data = user["country"]
                        if isinstance(country_data, dict):
                            result["country"] = country_data.get("name") or country_data.get("code")
                        elif isinstance(country_data, str):
                            result["country"] = country_data
                    if user.get("country_code"):
                        result["country_code"] = user["country_code"]
            except (json_lib.JSONDecodeError, TypeError, AttributeError):
                continue

        # Also look for data attributes on elements
        for el in soup.find_all(attrs={"data-initial-data": True}):
            try:
                json_data = json_lib.loads(el.get("data-initial-data"))
                if isinstance(json_data, dict) and "user" in json_data:
                    user = json_data["user"]
                    if user.get("username"):
                        result["username"] = user["username"]
                    if user.get("avatar_url"):
                        result["avatar_url"] = user["avatar_url"]
                    if user.get("country"):
                        country_data = user["country"]
                        if isinstance(country_data, dict):
                            result["country"] = country_data.get("name") or country_data.get("code")
                        elif isinstance(country_data, str):
                            result["country"] = country_data
            except (json_lib.JSONDecodeError, TypeError, AttributeError):
                continue

        return result

    json_data = extract_from_json(soup)
    if json_data.get("username"):
        data["username"] = json_data["username"]
    if json_data.get("avatar_url"):
        data["avatar_url"] = json_data["avatar_url"]
    if json_data.get("country") and not data["country"]:
        data["country"] = json_data["country"]

    # username: try og:title, then first h1
    def clean_username(raw: Optional[str]) -> Optional[str]:
        if not raw:
            return raw
        raw = raw.strip()
        # split on common separators and take the left-most meaningful part
        parts = re.split(r"\s*[-|·•:|\|]\s*", raw)
        name = parts[0].strip()
        # remove trailing words like 'play', 'profile', 'osu' if they remain
        name = re.sub(r"\b(?:play|profile|osu!|osu)\b$", "", name, flags=re.I).strip()
        # Normalize whitespace
        name = re.sub(r"\s+", " ", name)
        return name or None

    if not data["username"]:
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            data["username"] = clean_username(og_title.get("content"))
        else:
            h1 = soup.find("h1")
            if h1:
                data["username"] = clean_username(h1.get_text(strip=True))

    # avatar: prefer og:image
    if not data["avatar_url"]:
        og_image = soup.find("meta", property="og:image")
        if og_image and og_image.get("content"):
            data["avatar_url"] = urljoin(profile_url, og_image.get("content"))
        else:
            # try common avatar selectors
            avatar_img = soup.find("img", class_=re.compile(r"avatar|profile|user", re.I))
            if avatar_img and avatar_img.get("src"):
                data["avatar_url"] = urljoin(profile_url, avatar_img.get("src"))

    # If country was not found on the wiki (known_country is None), try to scrape it from profile
    if not data["country"]:
        def _extract_country(soup: BeautifulSoup) -> Optional[str]:
            # Look for flag-country class which is common on osu! profiles
            flag_el = soup.find(class_=re.compile(r"flag-country|profile-flag", re.I))
            if flag_el:
                # Check for title or data attributes
                if flag_el.get("title"):
                    return flag_el.get("title").strip()
                # Check parent for country info
                parent = flag_el.parent
                if parent:
                    text = parent.get_text(strip=True)
                    if text and len(text) < 50:
                        return text

            # Prefer images near the main header (h1) first
            h1 = soup.find("h1")
            search_areas = [h1.parent if h1 and h1.parent else None, h1, soup]
            for area in search_areas:
                if not area:
                    continue
                for img in area.find_all("img"):
                    cls = " ".join(img.get("class", [])) if img.get("class") else ""
                    src = img.get("src", "")
                    alt = img.get("alt")
                    title = img.get("title")
                    if any(k in cls.lower() for k in ("flag", "flag-icon", "country")) or any(k in src.lower() for k in ("flag", "/flags/", "/countries/")):
                        if alt and alt.strip():
                            return alt.strip()
                        if title and title.strip():
                            return title.strip()
                        # try to extract country code from filename: '/flags/us.png' -> 'US'
                        m = re.search(r"/([a-z]{2,3})(?:\.[a-z]{2,4})$", src, re.I)
                        if m:
                            return m.group(1).upper()
            # data attributes often used for structured data
            el = soup.find(attrs={"data-country": True})
            if el:
                return el.get("data-country")
            el = soup.find(attrs={"data-country-code": True})
            if el:
                return el.get("data-country-code")
            # fallback text search
            if h1:
                for sibling in h1.next_siblings:
                    if isinstance(sibling, str):
                        txt = sibling.strip()
                        if txt:
                            m = re.search(r"([A-Za-z][A-Za-z ]{1,40})", txt)
                            if m: return m.group(1).strip()
                    else:
                        text = getattr(sibling, "get_text", lambda: "")()
                        if text:
                            m = re.search(r"country[:\s]*([A-Za-z ]{2,40})", text, re.I)
                            if m: return m.group(1).strip()
            return None

        data["country"] = _extract_country(soup)

    # final fallback username from URL path
    if not data["username"]:
        path = urlparse(profile_url).path.rstrip("/")
        if path:
            data["username"] = path.split("/")[-1]

    return data


def ensure_table(conn: sqlite3.Connection):
    conn.execute(f"""CREATE TABLE IF NOT EXISTS "{TABLE_NAME}" (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT,
        profile_url TEXT UNIQUE,
        avatar_url TEXT,
        country TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()


def upsert_player(conn: sqlite3.Connection, record: dict):
    conn.execute(f"""INSERT INTO "{TABLE_NAME}" (username, profile_url, avatar_url, country, updated_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(profile_url) DO UPDATE SET
        username=excluded.username,
        avatar_url=excluded.avatar_url,
        country=excluded.country,
        updated_at=excluded.updated_at
    """,
                 (record.get("username"), record.get("profile_url"), record.get("avatar_url"), record.get("country"), datetime.utcnow()))
    conn.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to SQLite DB file (default: players.db)")
    parser.add_argument("--url", default=DEFAULT_PAGE, help="OWC 2025 wiki page URL")
    parser.add_argument("--recreate", action="store_true", help="Drop and recreate the table")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of profiles to fetch (0 = all)")
    parser.add_argument("--sleep", type=float, default=1.0, help="Seconds to sleep between profile requests")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if not args.quiet else logging.WARNING, format="%(levelname)s: %(message)s")

    session = requests.Session()

    log.info("Fetching OWC page: %s", args.url)
    try:
        soup = get_soup(args.url, session=session)
    except Exception as e:
        log.error("Failed to download page: %s", e)
        return

    section = find_participants_section(soup)
    if section is None:
        log.error("Could not find a 'Participants' section on the page. Exiting.")
        return

    # EXTRACTED: get list of {'profile_url': ..., 'country': ...}
    players_data = extract_players_with_countries(section, base_url=args.url)
    if not players_data:
        log.error("No participant profile links found in the Participants section.")
        return

    if args.limit > 0:
        players_data = players_data[: args.limit]

    log.info("Found %d participant profiles to inspect", len(players_data))

    conn = sqlite3.connect(args.db)
    if args.recreate:
        conn.execute(f"DROP TABLE IF EXISTS \"{TABLE_NAME}\"")
        conn.commit()
        log.info("Dropped existing table and will recreate it")

    ensure_table(conn)

    for i, p_data in enumerate(players_data, start=1):
        link = p_data["profile_url"]
        wiki_country = p_data["country"]
        
        log.info("[%d/%d] Processing %s (Country: %s)", i, len(players_data), link, wiki_country or "Unknown")
        
        # Pass the wiki_country to the parser so it doesn't need to guess
        rec = parse_profile_page(link, session, known_country=wiki_country)
        
        upsert_player(conn, rec)
        time.sleep(args.sleep)

    conn.close()
    log.info("Done. Data stored in %s (table: %s)", args.db, TABLE_NAME)


if __name__ == "__main__":
    main()
