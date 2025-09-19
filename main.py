import os, re, json, time, requests, pandas as pd
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# ===== Output location =====
RAW_DIR = os.path.join("data", "raw")

# ===== Spotify creds (env preferred) =====
CID = os.getenv("SPOTIFY_CID", "a2fcd932726e496dbfb04cec705edea4")
SEC = os.getenv("SPOTIFY_SEC", "1c46ff7084d945f795227d3e408ad2a8")

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)

def ensure_output_dir(path: str):
    if os.path.exists(path) and not os.path.isdir(path):
        raise SystemExit(f"ERROR: {path!r} exists but is a file")
    os.makedirs(path, exist_ok=True)

def slugify(txt: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", txt.lower()).strip("_")

# ---------------- Spotify ----------------
def get_spotify_token():
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(CID, SEC),
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def spotify_search_artist(artist_name: str, h: dict):
    r = requests.get(
        "https://api.spotify.com/v1/search",
        headers=h,
        params={"q": artist_name, "type": "artist", "limit": 1},
        timeout=20,
    )
    r.raise_for_status()
    items = r.json().get("artists", {}).get("items", [])
    if not items:
        raise SystemExit("Artist not found on Spotify.")
    return items[0]

def spotify_get_albums(artist_id: str, h: dict):
    albums, url = [], f"https://api.spotify.com/v1/artists/{artist_id}/albums"
    params = {"include_groups": "album", "limit": 50, "market": "US"}
    while url:
        r = requests.get(url, headers=h, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        albums.extend(data.get("items", []))
        url, params = data.get("next"), None
    return albums

def spotify_get_album_tracks(album_id: str, h: dict):
    tracks, url = [], f"https://api.spotify.com/v1/albums/{album_id}/tracks"
    params = {"limit": 50, "market": "US"}
    while url:
        r = requests.get(url, headers=h, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        tracks.extend(data.get("items", []))
        url, params = data.get("next"), None
    return tracks

def spotify_batch_track_details(track_ids, h: dict):
    details = {}
    for i in range(0, len(track_ids), 50):
        batch = track_ids[i:i+50]
        r = requests.get(
            "https://api.spotify.com/v1/tracks",
            headers=h,
            params={"ids": ",".join(batch), "market": "US"},
            timeout=20,
        )
        r.raise_for_status()
        for t in r.json().get("tracks", []):
            if t:
                details[t["id"]] = {
                    "popularity": t.get("popularity"),
                    "explicit": t.get("explicit"),
                    "duration_ms": t.get("duration_ms"),
                    "spotify_url": (t.get("external_urls") or {}).get("spotify"),
                }
    return details

# ------------- Ticketmaster (WEB SCRAPE) -------------
def tm_find_event_urls(artist_name: str, max_urls: int = 80):
    """Grab /event/ links from the search page; dedupe & normalize."""
    q = artist_name.replace(" ", "+")
    url = f"https://www.ticketmaster.com/search?q={q}"
    r = requests.get(url, headers={"User-Agent": UA}, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/event/" in href:
            if href.startswith("http"):
                full = href
            else:
                full = "https://www.ticketmaster.com" + href
            urls.append(full.split("?")[0])  # strip tracking params

    # Deduplicate & cap
    uniq = []
    seen = set()
    for u in urls:
        if u not in seen:
            uniq.append(u)
            seen.add(u)
        if len(uniq) >= max_urls:
            break
    return uniq

def tm_parse_event_jsonld(html: str):
    """Extract schema.org Event from JSON-LD blocks."""
    soup = BeautifulSoup(html, "html.parser")
    blocks = soup.find_all("script", type="application/ld+json")
    for tag in blocks:
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue

        # JSON-LD can be a dict or a list of dicts
        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            # Sometimes wrapped in "@graph"
            if isinstance(obj, dict) and "@graph" in obj:
                for g in obj["@graph"]:
                    if isinstance(g, dict) and g.get("@type") in ("Event", ["Event"]):
                        return g
            if isinstance(obj, dict) and obj.get("@type") in ("Event", ["Event"]):
                return obj
    return None

def tm_scrape_events(artist_name: str, delay_sec: float = 0.4):
    """Scrape event detail pages and extract Event JSON-LD into rows."""
    rows = []
    urls = tm_find_event_urls(artist_name)
    for u in urls:
        try:
            r = requests.get(u, headers={"User-Agent": UA, "Referer": "https://www.ticketmaster.com/"}, timeout=25)
            r.raise_for_status()
            ev = tm_parse_event_jsonld(r.text)
            if not ev:
                continue

            # Flatten common fields from JSON-LD
            name = ev.get("name")
            start = ev.get("startDate") or ev.get("start_date")
            loc = ev.get("location") or {}
            if isinstance(loc, list):
                loc = loc[0] if loc else {}
            venue = loc.get("name")
            address = loc.get("address") or {}
            if isinstance(address, list):
                address = address[0] if address else {}
            city = address.get("addressLocality")
            state = address.get("addressRegion")
            country = address.get("addressCountry")

            offers = ev.get("offers") or {}
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            min_price = offers.get("lowPrice") or offers.get("price")
            max_price = offers.get("highPrice") or offers.get("price")
            currency = offers.get("priceCurrency")

            rows.append({
                "record_type": "event",
                "artist": artist_name,
                "album_name": None,
                "album_release_date": None,
                "track_id": None,
                "track_name": None,
                "track_number": None,
                "explicit": None,
                "duration_ms": None,
                "popularity": None,
                "spotify_url": None,
                "tm_event_name": name,
                "tm_event_datetime": start,
                "tm_venue": venue,
                "tm_city": city,
                "tm_state": state,
                "tm_country": country if isinstance(country, str) else (country or {}).get("name"),
                "tm_min_price": min_price,
                "tm_max_price": max_price,
                "tm_currency": currency,
                "tm_url": u,
            })
        except requests.HTTPError:
            continue
        except Exception:
            continue
        time.sleep(delay_sec)  # be polite
    return pd.DataFrame(rows)

# ---------------- Main ----------------
def main():
    ensure_output_dir(RAW_DIR)

    artist = input("Enter artist name: ").strip()
    artist_slug = slugify(artist)

    # Spotify
    tok = get_spotify_token()
    h = {"Authorization": f"Bearer {tok}"}
    artist_obj = spotify_search_artist(artist, h)
    artist_id = artist_obj["id"]

    albums = spotify_get_albums(artist_id, h)
    track_rows, track_ids = [], []
    for alb in albums:
        alb_id = alb["id"]
        alb_name = alb["name"]
        alb_date = alb.get("release_date")

        tracks = spotify_get_album_tracks(alb_id, h)
        for tr in tracks:
            tid = tr.get("id")
            track_rows.append({
                "record_type": "track",
                "artist": artist,
                # removed spotify_artist_id, album_id, disc_number
                "album_name": alb_name,
                "album_release_date": alb_date,
                "track_id": tid,
                "track_name": tr.get("name"),
                "track_number": tr.get("track_number"),
                "explicit": tr.get("explicit"),
                "duration_ms": tr.get("duration_ms"),
                "popularity": None,      # fill from batch
                "spotify_url": None,     # fill from batch
                # event columns for unified schema
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
            })
            if tid:
                track_ids.append(tid)

    # Enrich tracks with popularity/url
    if track_ids:
        info = spotify_batch_track_details(track_ids, h)
        for row in track_rows:
            meta = info.get(row["track_id"], {})
            row["popularity"]  = meta.get("popularity", row["popularity"])
            row["duration_ms"] = meta.get("duration_ms", row["duration_ms"])
            row["explicit"]    = meta.get("explicit", row["explicit"])
            row["spotify_url"] = meta.get("spotify_url", row["spotify_url"])

    df_tracks = pd.DataFrame(track_rows)

    # Ticketmaster (SCRAPE)
    df_events = tm_scrape_events(artist)

    # Combine and save
    cols = [
        "record_type","artist",
        "album_name","album_release_date",
        "track_id","track_name","track_number","explicit","duration_ms","popularity","spotify_url",
        "tm_event_name","tm_event_datetime","tm_venue","tm_city","tm_state","tm_country",
        "tm_min_price","tm_max_price","tm_currency","tm_url",
    ]
    combined = pd.concat([df_tracks[cols], df_events[cols]], ignore_index=True)

    out_csv = os.path.join(RAW_DIR, f"{artist_slug}_spotify_ticketmaster_all.csv")
    combined.to_csv(out_csv, index=False)

    print(f"[done] Saved: {out_csv}")
    print(f"  Tracks: {len(df_tracks):,} | Events (scraped): {len(df_events):,} | Total rows: {len(combined):,}")

if __name__ == "__main__":
    main()
