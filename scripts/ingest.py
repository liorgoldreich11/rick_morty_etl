import json
import time
import requests

# API endpoints
URL_CHAR = "https://rickandmortyapi.com/api/character"
URL_EP = "https://rickandmortyapi.com/api/episode"


def fetch_with_retry(url, params=None, max_retries=5, backoff=1):
    """Fetch URL with exponential backoff retries"""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            wait = backoff * (2 ** attempt)
            print(f"Error {e}, retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"Failed after {max_retries} retries: {url}")


def ingest_endpoint(conn, base_url, raw_table):
    page = 1
    cur = conn.cursor()
    while True:
        data = fetch_with_retry(base_url, params={"page": page})
        payload = json.dumps(data)

        cur.execute(f"""
            MERGE INTO RAW.{raw_table} tgt
            USING (SELECT %s AS PAGE_NUMBER) src
            ON tgt.PAGE_NUMBER = src.PAGE_NUMBER
            WHEN NOT MATCHED THEN
              INSERT (PAGE_NUMBER, RESPONSE_VARIANT)
              VALUES (%s, PARSE_JSON(%s))
        """, (page, page, payload))

        print(f"Ingested {raw_table} page {page}, {len(data.get('results', []))} records")

        if not data.get("info", {}).get("next"):
            break
        page += 1

