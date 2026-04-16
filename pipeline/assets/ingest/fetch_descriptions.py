"""@bruin

name: ingest.fetch_descriptions
description: Fetch anime metadata from Jikan API and upload as JSON to GCS

@bruin"""

import csv, json, os, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, PROJECT_ROOT)

from modules import get_anime_data

BUCKET_NAME = "jikan_anime_data_bucket"
SEED_CSV    = os.path.join(PROJECT_ROOT, "pipeline", "assets", "seeds", "dim_anime.csv")


def load_anime_ids():
    with open(SEED_CSV, encoding="utf-8") as f:
        return [int(row["anime_id"]) for row in csv.DictReader(f)]


def upload_one(client: storage.Client, anime_id: int):
    bucket    = client.bucket(BUCKET_NAME)
    blob_name = f"descriptions/anime_{anime_id}.json"
    blob      = bucket.blob(blob_name)

    if blob.exists():
        return anime_id, "skipped"

    data = get_anime_data(anime_id)
    if not data:
        return anime_id, "failed"

    blob.upload_from_string(json.dumps(data, ensure_ascii=False), content_type="application/json")
    time.sleep(0.4)  # stay under Jikan rate limit
    return anime_id, "uploaded"


def main():
    client    = storage.Client()
    anime_ids = load_anime_ids()
    uploaded = skipped = failed = 0

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(upload_one, client, aid): aid for aid in anime_ids}
        for future in as_completed(futures):
            aid, status = future.result()
            if status == "uploaded":
                uploaded += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed += 1
                print(f"  FAILED: anime_{aid}")

    print(f"Descriptions — uploaded: {uploaded}, skipped: {skipped}, failed: {failed}")


main()
