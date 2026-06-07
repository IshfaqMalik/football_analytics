import requests
import json
import time
import sys

container = sys.argv[1]
# Config
API_KEY = "45673d207487483cbc20155ff24fbbca"
BASE_URL = "https://api.football-data.org/v4"
STORAGE_PATH = f"abfss://{container}@datalake060211.dfs.core.windows.net/landing"

COMPETITIONS = [
    "PL",
    "BL1",
    "SA",
    "PD",
    "FL1",
    "CL",
]  # Premier League, Bundesliga, Serie A, La Liga, Ligue 1, Champions League

headers = {"X-Auth-Token": API_KEY}


def fetch_and_save(endpoint, save_path):
    """Fetch from API and save raw JSON to ADLS."""
    time.sleep(6)  # Rate limit: 10 requests per minute
    response = requests.get(f"{BASE_URL}/{endpoint}", headers=headers)
    response.raise_for_status()

    data = response.json()
    json_str = json.dumps(data, indent=2)

    dbutils.fs.put(save_path, json_str, overwrite=True)
    print(f"Saved {save_path}")
    return data


# Ingest all competitions
for comp in COMPETITIONS:
    # Matches
    fetch_and_save(f"competitions/{comp}/matches", f"{STORAGE_PATH}/matches/{comp}/matches.json")

    # Teams
    fetch_and_save(f"competitions/{comp}/teams", f"{STORAGE_PATH}/teams/{comp}/teams.json")

    # Standings
    fetch_and_save(f"competitions/{comp}/standings", f"{STORAGE_PATH}/standings/{comp}/standings.json")

    # Scorers
    fetch_and_save(f"competitions/{comp}/scorers", f"{STORAGE_PATH}/scorers/{comp}/scorers.json")

print("API ingestion complete")
