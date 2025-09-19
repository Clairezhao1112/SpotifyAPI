import os, re, json, requests, pandas as pd
from bs4 import BeautifulSoup

# Config
CID = os.getenv("SPOTIFY_CID", "a2fcd932726e496dbfb04cec705edea4")
SEC = os.getenv("SPOTIFY_SEC", "1c46ff7084d945f795227d3e408ad2a8")

def get_spotify_data(artist):
    # Get token and search artist
    token = requests.post("https://accounts.spotify.com/api/token", 
                         data={"grant_type": "client_credentials"}, auth=(CID, SEC)).json()["access_token"]
    h = {"Authorization": f"Bearer {token}"}
    
    artist_id = requests.get("https://api.spotify.com/v1/search", headers=h, 
                           params={"q": artist, "type": "artist", "limit": 1}).json()["artists"]["items"][0]["id"]
    
    # Get albums and tracks
    albums = []
    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums?include_groups=album&limit=50"
    while url:
        data = requests.get(url, headers=h).json()
        albums.extend(data["items"])
        url = data.get("next")
    
    tracks = []
    for album in albums:
        album_tracks = requests.get(f"https://api.spotify.com/v1/albums/{album['id']}/tracks", headers=h).json()["items"]
        for track in album_tracks:
            tracks.append({
                "type": "track", "artist": artist, "album": album["name"], 
                "date": album.get("release_date"), "name": track["name"], 
                "id": track["id"], "duration": track.get("duration_ms")
            })
    
    # Get popularity/URLs in batches
    for i in range(0, len(tracks), 50):
        batch = [t["id"] for t in tracks[i:i+50]]
        details = requests.get("https://api.spotify.com/v1/tracks", headers=h, 
                             params={"ids": ",".join(batch)}).json()["tracks"]
        for j, detail in enumerate(details):
            if detail:
                tracks[i+j].update({"popularity": detail.get("popularity"), 
                                  "url": detail.get("external_urls", {}).get("spotify")})
    return tracks

def get_ticketmaster_events(artist):
    ua = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    
    # Get event URLs
    search = requests.get(f"https://www.ticketmaster.com/search?q={artist.replace(' ', '+')}", headers=ua)
    urls = [a["href"] for a in BeautifulSoup(search.text, "html.parser").find_all("a", href=True) 
            if "/event/" in a["href"]][:30]  # Limit to 30
    
    events = []
    for url in urls:
        try:
            full_url = url if url.startswith("http") else f"https://www.ticketmaster.com{url}"
            page = requests.get(full_url.split("?")[0], headers=ua, timeout=10)
            
            # Extract event JSON
            for script in BeautifulSoup(page.text, "html.parser").find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    items = data if isinstance(data, list) else [data]
                    
                    for item in items:
                        if isinstance(item, dict):
                            candidates = item.get("@graph", [item])
                            for obj in candidates:
                                if obj.get("@type") == "Event":
                                    loc = obj.get("location", {})
                                    addr = loc.get("address", {}) if isinstance(loc, dict) else {}
                                    
                                    events.append({
                                        "type": "event", "artist": artist, "name": obj.get("name"),
                                        "date": obj.get("startDate"), "venue": loc.get("name"),
                                        "city": addr.get("addressLocality"), "url": full_url
                                    })
                                    break
                    break
                except: continue
        except: continue
    return events

def main():
    artist = input("Artist name: ").strip()
    
    tracks = get_spotify_data(artist)
    events = get_ticketmaster_events(artist)
    
    # Combine data
    all_data = tracks + events
    df = pd.DataFrame(all_data)
    
    # Save
    os.makedirs("data/raw", exist_ok=True)
    filename = f"data/raw/{re.sub(r'[^a-z0-9]+', '_', artist.lower()).strip('_')}_data.csv"
    df.to_csv(filename, index=False)
    
    print(f"Saved {len(tracks)} tracks, {len(events)} events to {filename}")

if __name__ == "__main__":
    main()