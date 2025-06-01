import requests
import datetime
from supabase import create_client, Client
import os

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "nasdaq_ticks")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_bars():
    now = datetime.datetime.utcnow()
    from_date = (now - datetime.timedelta(days=8)).strftime("%Y-%m-%d")
    to_date = now.strftime("%Y-%m-%d")

    url = f"https://api.polygon.io/v2/aggs/ticker/NDX/range/1/minute/{from_date}/{to_date}?adjusted=true&sort=asc&limit=50000&apiKey={POLYGON_API_KEY}"

    response = requests.get(url)
    if response.status_code != 200:
        print("Erreur API Polygon:", response.status_code, response.text)
        return

    data = response.json().get("results", [])
    rows = []
    for bar in data:
        row = {
            "timestamp": datetime.datetime.utcfromtimestamp(bar["t"] / 1000).isoformat(),
            "open": bar["o"],
            "high": bar["h"],
            "low": bar["l"],
            "close": bar["c"],
            "volume": bar["v"]
        }
        rows.append(row)

    print(f"Insertion de {len(rows)} lignes...")
    for row in rows:
        supabase.table(SUPABASE_TABLE).upsert(row).execute()

if __name__ == "__main__":
    fetch_bars()
