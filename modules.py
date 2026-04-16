import requests
import time
from typing import Dict

BASE_URL = "https://api.jikan.moe/v4/"
TIMEOUT = 10
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds to wait after a 429


def _get(url: str, params: dict = None) -> dict | None:
    """GET with timeout and retry on 429/5xx."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=TIMEOUT)
        except requests.exceptions.RequestException:
            if attempt == MAX_RETRIES - 1:
                return None
            time.sleep(RETRY_DELAY)
            continue

        if response.status_code == 200:
            return response.json()
        if response.status_code == 429:
            time.sleep(RETRY_DELAY * (attempt + 1))
            continue
        if response.status_code in (404, 400):
            return None  # not retryable
        # 5xx — retry
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY)

    return None


def get_anime_data(anime_id: int) -> Dict:
    result = _get(f"{BASE_URL}anime/{anime_id}/full")
    if not result:
        return {}
    try:
        data = result["data"]
        dct = {"anime_id": anime_id}
        for field in ["title", "title_english", "title_japanese", "status", "airing", "score", "rank", "popularity", "synopsis", "year", "rating"]:
            dct[field] = data.get(field)
        dct["image_url"] = data["images"]["jpg"]["large_image_url"]
        dct["airing_start"] = data["aired"].get("from")
        dct["airing_end"] = data["aired"].get("to")
        dct["studios"] = ", ".join(s["name"] for s in data.get("studios", []))
        dct["genres"] = ", ".join(g["name"] for g in data.get("genres", []))
        dct["anime_type"] = ", ".join(d["name"] for d in data.get("demographics", []))
        return dct
    except Exception:
        return {}


def get_character_data(anime_id: int) -> Dict:
    result = _get(f"{BASE_URL}anime/{anime_id}/characters")
    if not result:
        return {}
    try:
        data = result["data"]
        characters = [
            {
                "character_id": c["character"]["mal_id"],
                "name": c["character"]["name"],
                "image_url": c["character"]["images"]["jpg"]["image_url"],
            }
            for c in data
            if c["role"] == "Main"
        ]
        return {"anime_id": anime_id, "characters": characters}
    except Exception:
        return {}


def get_episode_data(anime_id: int) -> Dict:
    url = f"{BASE_URL}anime/{anime_id}/episodes"
    episodes = []
    page = 1

    while True:
        result = _get(url, params={"page": page})
        if not result:
            break
        data = result["data"]
        if not data:
            break
        episodes.extend(
            {"episode_id": e["mal_id"], "title": e["title"], "score": e["score"], "filler": e.get("filler")}
            for e in data
        )
        if not data:
            break
        page += 1
        time.sleep(0.4)

    return {"anime_id": anime_id, "episodes": episodes}
