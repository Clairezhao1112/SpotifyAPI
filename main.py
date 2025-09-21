import os, re, json, requests, pandas as pd
from bs4 import BeautifulSoup


CID = os.getenv("SPOTIFY_CID", "a2fcd932726e496dbfb04cec705edea4")
SEC = os.getenv("SPOTIFY_SEC", "1c46ff7084d945f795227d3e408ad2a8")


def get_spotify_data(artist):
    """
    Fetch track-level data for a given artist using the Spotify Web API.
    Steps:
      1) Get an app-only access token via Client Credentials.
      2) Search for the artist to retrieve a Spotify artist_id.
      3) List all 'album' releases for that artist (handles pagination).
      4) For each album, fetch its tracks.
      5) In batches of 50 track IDs, request detailed track info
         to enrich with 'popularity' and public 'url'.
    """
    
    token = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(CID, SEC)
    ).json()["access_token"]
    h = {"Authorization": f"Bearer {token}"}

    # Find artist_id via search endpoint ----
    artist_id = requests.get(
        "https://api.spotify.com/v1/search",
        headers=h,
        params={"q": artist, "type": "artist", "limit": 1}
    ).json()["artists"]["items"][0]["id"]

    # Gather all album objects (handles pagination via "next") 
    albums = []
    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums?include_groups=album&limit=50"
    while url:
        data = requests.get(url, headers=h).json()
        albums.extend(data["items"])
        url = data.get("next") 

    # For each album, fetch tracks and collect basic fields ----
    tracks = []
    for album in albums:
        album_tracks = requests.get(
            f"https://api.spotify.com/v1/albums/{album['id']}/tracks",
            headers=h
        ).json()["items"]

        for track in album_tracks:
            tracks.append({
                "type": "track",
                "artist": artist,
                "album": album["name"],
                "date": album.get("release_date"),
                "name": track["name"],
                "id": track["id"], 
                "duration": track.get("duration_ms")
            })

    # Batch-enrich tracks with popularity and public URL 
    # tracks accepts up to 50 IDs per call
    for i in range(0, len(tracks), 50):
        batch_ids = [t["id"] for t in tracks[i:i+50]]
        details = requests.get(
            "https://api.spotify.com/v1/tracks",
            headers=h,
            params={"ids": ",".join(batch_ids)}
        ).json()["tracks"]

        for j, detail in enumerate(details):
            if detail:  
                tracks[i + j].update({
                    "popularity": detail.get("popularity"),
                    "url": (detail.get("external_urls") or {}).get("spotify")
                })

    return tracks


def get_ticketmaster_events(artist):

    ua = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

    #  Search results page 
    search = requests.get(
        f"https://www.ticketmaster.com/search?q={artist.replace(' ', '+')}",
        headers=ua
    )

    #  Collect up to 30 candidate event links 
    soup = BeautifulSoup(search.text, "html.parser")
    urls = [
        a["href"] for a in soup.find_all("a", href=True)
        if "/event/" in a["href"]
    ][:30]

    events = []

    #  Visit each candidate event page ----
    for url in urls:
        try:
            # Normalize to absolute URL and drop query params
            full_url = url if url.startswith("http") else f"https://www.ticketmaster.com{url}"
            page = requests.get(full_url.split("?")[0], headers=ua, timeout=10)

            # ---- (4) Parse JSON-LD blocks; look for Event objects ----
            psoup = BeautifulSoup(page.text, "html.parser")
            for script in psoup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")

                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if isinstance(item, dict):
                            candidates = item.get("@graph", [item])
                            for obj in candidates:
                                if obj.get("@type") == "Event":
                                    # Extract venue and address details if present
                                    loc = obj.get("location", {})
                                    addr = loc.get("address", {}) if isinstance(loc, dict) else {}

                                    events.append({
                                        "type": "event",
                                        "artist": artist,
                                        "name": obj.get("name"),
                                        "date": obj.get("startDate"),
                                        "venue": loc.get("name"),
                                        "city": addr.get("addressLocality"),
                                        "url": full_url
                                    })
                                    break
                    break
                except:
                    continue
        except:
            continue

    return events


def main():
    """
    CLI entrypoint:
      - Prompt for artist name.
      - Fetch Spotify tracks and Ticketmaster events. Combine into a single DataFrame and save as CSV under data/raw/.
    """
    artist = input("Artist name: ").strip()

    tracks = get_spotify_data(artist)
    events = get_ticketmaster_events(artist)

    all_data = tracks + events
    df = pd.DataFrame(all_data)

    os.makedirs("data/raw", exist_ok=True)

    filename = f"data/raw/{re.sub(r'[^a-z0-9]+', '_', artist.lower()).strip('_')}_data.csv"
    df.to_csv(filename, index=False)

    print(f"Saved {len(tracks)} tracks, {len(events)} events to {filename}")

if __name__ == "__main__":
    main()