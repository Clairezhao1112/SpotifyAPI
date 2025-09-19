import os, sys, re, math, requests, pandas as pd
from datetime import datetime, timezone

# ===== Output goes under data/raw/ =====
RAW_DIR = os.path.join("data", "raw")

# ===== Credentials (prefer env vars) =====
CID = os.getenv("SPOTIFY_CID", "a2fcd932726e496dbfb04cec705edea4")
SEC = os.getenv("SPOTIFY_SEC", "1c46ff7084d945f795227d3e408ad2a8")
TM_KEY = os.getenv("TM_API_KEY")  # REQUIRED for Ticketmaster Discovery API

# ---------- utils ----------
def slugify(txt: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", txt.lower()).strip("_")

def ensure_output_dir(path: str):
    if os.path.exists(path) and not os.path.isdir(path):
        raise SystemExit(f"ERROR: {path!r} exists but is a FILE. Please remove/rename it.")
    os.makedirs(path, exist_ok=True)

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# ---------- Spotify ----------
def get_spotify_token():
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(CID, SEC),
        timeout=20
    )
    r.raise_for_status()
    return r.json()["access_token"]

def spotify_search_artist(artist_name: str, headers: dict):
    r = requests.get(
        "https://api.spotify.com/v1/search",
        headers=headers,
        params={"q": artist_name, "type": "artist", "limit": 1},
        timeout=20
    )
    r.raise_for_status()
    data = r.json()
    items = data.get("artists", {}).get("items", [])
    if not items:
        raise SystemExit("Artist not found on Spotify.")
    return items[0]  # full artist object

def spotify_get_albums(artist_id: str, headers: dict):
    albums = []
    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
    params = {"include_groups": "album", "limit": 50, "market": "US"}
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        albums.extend(data.get("items", []))
        url = data.get("next")
        params = None
    return albums

def spotify_get_album_tracks(album_id: str, headers: dict):
    tracks = []
    url = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
    params = {"limit": 50, "market": "US"}
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        tracks.extend(data.get("items", []))
        url = data.get("next")
        params = None
    return tracks

def spotify_batch_track_details(track_ids, headers: dict):
    """Fetch track popularity (and other details) in batches of 50."""
    details = {}
    for batch in chunks(track_ids, 50):
        r = requests.get(
            "https://api.spotify.com/v1/tracks",
            headers=headers,
            params={"ids": ",".join(batch), "market": "US"},
            timeout=20
        )
        r.raise_for_status()
        for t in r.json().get("tracks", []):
            if t:
                details[t["id"]] = {
                    "popularity": t.get("popularity"),
                    "explicit": t.get("explicit"),
                    "duration_ms": t.get("duration_ms"),
                    "external_url": (t.get("external_urls") or {}).get("spotify")
                }
    return details

# ---------- Ticketmaster (Discovery API) ----------
TM_BASE = "https://app.ticketmaster.com/discovery/v2"

def tm_request(path, params):
    if not TM_KEY:
        raise SystemExit("ERROR: TM_API_KEY not set in environment.")
    q = {"apikey": TM_KEY, **params}
    r = requests.get(f"{TM_BASE}/{path}", params=q, timeout=25)
    try:
        data = r.json()
    except ValueError:
        r.raise_for_status()
        data = {}
    if "fault" in data:
        raise SystemExit(f"Ticketmaster API error: {data['fault'].get('faultstring')}")
    r.raise_for_status()
    return data

def tm_find_attraction_id(artist_name: str):
    data = tm_request("attractions.json", {"keyword": artist_name, "size": 50})
    items = data.get("_embedded", {}).get("attractions", []) or []
    # exact case-insensitive match preferred
    for a in items:
        if a.get("name", "").strip().lower() == artist_name.strip().lower():
            return a.get("id")
    return items[0]["id"] if items else None

def tm_get_events(artist_name: str, country="US"):
    rows = []
    attraction_id = tm_find_attraction_id(artist_name)
    params = {"size": 200, "sort": "date,asc", "countryCode": country}
    if attraction_id:
        params["attractionId"] = attraction_id
    else:
        params["keyword"] = artist_name  # fallback if no attraction found

    data = tm_request("events.json", params)
    events = data.get("_embedded", {}).get("events", []) or []
    for e in events:
        venues = (e.get("_embedded", {}).get("venues") or [{}])
        v = venues[0] if venues else {}
        start = (e.get("dates", {}) or {}).get("start", {}) or {}
        price = (e.get("priceRanges") or [{}])[0] if e.get("priceRanges") else {}

        rows.append({
            "record_type": "event",
            "artist": artist_name,
            "album_name": None,
            "album_release_date": None,
            "track_name": None,
            "track_number": None,
            "explicit": None,
            "duration_ms": None,
            "popularity": None,
            "spotify_url": None,
            "tm_event_name": e.get("name"),
            "tm_event_datetime": start.get("dateTime") or start.get("localDate"),
            "tm_venue": v.get("name"),
            "tm_city": (v.get("city") or {}).get("name"),
            "tm_state": (v.get("state") or {}).get("stateCode") or (v.get("state") or {}).get("name"),
            "tm_country": (v.get("country") or {}).get("countryCode"),
            "tm_min_price": price.get("min"),
            "tm_max_price": price.get("max"),
            "tm_currency": price.get("currency"),
            "tm_url": e.get("url"),
            "tm_attraction_id": attraction_id
        })
    return pd.DataFrame(rows)

# ---------- Main ----------
def main():
    ensure_output_dir(RAW_DIR)

    artist = input("Enter artist name: ").strip()
    artist_slug = slugify(artist)

    # Spotify auth & artist
    tok = get_spotify_token()
    h = {"Authorization": f"Bearer {tok}"}
    artist_obj = spotify_search_artist(artist, h)
    spotify_artist_id = artist_obj["id"]

    # Spotify albums + tracks (collect track IDs to enrich popularity)
    albums = spotify_get_albums(spotify_artist_id, h)
    track_rows, track_ids = [], []

    for alb in albums:
        alb_id = alb["id"]
        alb_name = alb["name"]
        alb_date = alb.get("release_date")

        tracks = spotify_get_album_tracks(alb_id, h)
        for tr in tracks:
            tid = tr["id"]
            track_rows.append({
                "record_type": "track",
                "artist": artist,
                "spotify_artist_id": spotify_artist_id,
                "album_id": alb_id,
                "album_name": alb_name,
                "album_release_date": alb_date,
                "track_id": tid,
                "track_name": tr.get("name"),
                "track_number": tr.get("track_number"),
                "disc_number": tr.get("disc_number"),
                # placeholders to fill from batch details:
                "explicit": tr.get("explicit"),
                "duration_ms": tr.get("duration_ms"),
                "popularity": None,
                "spotify_url": None,
                # event columns (None for tracks)
                "tm_event_name": None,
                "tm_event_datetime": None,
                "tm_venue": None,
                "tm_city": None,
                "tm_state": None,
                "tm_country": None,
                "tm_min_price": None,
                "tm_max_price": None,
                "tm_currency": None,
                "tm_url": None,
                "tm_attraction_id": None
            })
            if tid:
                track_ids.append(tid)

    # Enrich tracks with popularity/urls via batched /v1/tracks
    if track_ids:
        detail_map = spotify_batch_track_details(track_ids, h)
        for row in track_rows:
            info = detail_map.get(row["track_id"], {})
            row["popularity"] = info.get("popularity", row["popularity"])
            row["explicit"]   = info.get("explicit", row["explicit"])
            row["duration_ms"]= info.get("duration_ms", row["duration_ms"])
            row["spotify_url"]= info.get("external_url")

    df_tracks = pd.DataFrame(track_rows)

    # Ticketmaster events
    try:
        df_events = tm_get_events(artist, country="US")
    except SystemExit as e:
        print(str(e))
        df_events = pd.DataFrame([])

    # Combine into ONE file
    combined = pd.concat([df_tracks, df_events], ignore_index=True)

    # Single CSV under data/raw/
    base = f"{artist_slug}_spotify_ticketmaster_all.csv"
    out_csv = os.path.join(RAW_DIR, base)
    combined.to_csv(out_csv, index=False)

    print(f"[done] Saved: {out_csv}")
    print(f"  Tracks: {len(df_tracks):,} | Events: {len(df_events):,} | Total rows: {len(combined):,}")

if __name__ == "__main__":
    main()
