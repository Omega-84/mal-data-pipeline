"""Fetch the top 1000 anime by popularity from Jikan API and write to dim_anime.csv."""

import csv
import sys
import time
from pathlib import Path

import requests

BASE_URL = "https://api.jikan.moe/v4"
TIMEOUT = 10
MAX_RETRIES = 3
OUTPUT = Path(__file__).parent.parent / "pipeline" / "assets" / "seeds" / "dim_anime.csv"

TARGET = 500
PAGE_SIZE = 25
PAGES = TARGET // PAGE_SIZE  # 20 pages


def fetch_page(page: int) -> list[dict]:
    url = f"{BASE_URL}/anime"
    params = {"order_by": "popularity", "limit": PAGE_SIZE, "page": page}
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 429:
                wait = 2 * (attempt + 1)
                print(f"  429 rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json().get("data", [])
        except requests.RequestException as e:
            print(f"  Error page {page} attempt {attempt + 1}: {e}")
            time.sleep(2)
    return []


def main():
    rows = []
    seen = set()

    for page in range(1, PAGES + 1):
        print(f"Fetching page {page}/{PAGES} ({len(rows)} anime so far)...")
        items = fetch_page(page)
        if not items:
            print(f"  No data returned for page {page}, stopping.")
            break
        for item in items:
            anime_id = item.get("mal_id")
            if anime_id and anime_id not in seen:
                seen.add(anime_id)
                rows.append({
                    "anime_id": anime_id,
                    "title": item.get("title", ""),
                    "title_english": item.get("title_english") or "",
                })
        time.sleep(0.4)

    print(f"\nFetched {len(rows)} anime. Writing to {OUTPUT}...")
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["anime_id", "title", "title_english"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done. {OUTPUT} updated with {len(rows)} anime.")


if __name__ == "__main__":
    main()
