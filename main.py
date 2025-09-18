import requests, pandas as pd
from bs4 import BeautifulSoup

# === Your Spotify credentials ===
CID = "a2fcd932726e496dbfb04cec705edea4"
SEC = "1c46ff7084d945f795227d3e408ad2a8"

def get_token():
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(CID, SEC)
    )
    r.raise_for_status()
    return r.json()["access_token"]

artist = input("Enter artist name: ").strip()
tok = get_token()

# --- Spotify: search + top tracks ---
h = {"Authorization": f"Bearer {tok}"}
search = requests.get(
    "https://api.spotify.com/v1/search",
    headers=h,
    params={"q": artist, "type": "artist", "limit": 1}
).json()

if not search["artists"]["items"]:
    raise SystemExit("Artist not found on Spotify")

aid = search["artists"]["items"][0]["id"]

top = requests.get(
    f"https://api.spotify.com/v1/artists/{aid}/top-tracks",
    headers=h,
    params={"market": "US"}
).json()["tracks"]

spotify_rows = []
for t in top:
    spotify_rows.append({
        "artist": artist,
        "track": t["name"],                         # <-- SONG NAME
        "popularity": t["popularity"],
        "album": t["album"]["name"],                # <-- ALBUM NAME
        "release_date": t["album"]["release_date"]  # <-- ALBUM RELEASE DATE
    })

# Print Song — Album — Release Date to console
print("\nSpotify Top Tracks:")
for r in spotify_rows:
    print(f"- {r['track']} — {r['album']} — {r['release_date']}")

df_sp = pd.DataFrame(spotify_rows)

# --- Ticketmaster scrape (simple search page) ---
url = f"https://www.ticketmaster.com/search?q={artist.replace(' ', '+')}"
html = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}).text
soup = BeautifulSoup(html, "html.parser")

tm_rows = []
for a in soup.find_all("a", href=True):
    if "/event/" in a["href"]:
        tm_rows.append({
            "artist": artist,
            "event": a.get_text(strip=True),
            "ticket_url": "https://www.ticketmaster.com" + a["href"]
        })
df_tm = pd.DataFrame(tm_rows).drop_duplicates()

# --- Save both ---
df = pd.concat([df_sp, df_tm], axis=1)
out = f"{artist.replace(' ', '_')}_spotify_ticketmaster.csv"
df.to_csv(out, index=False)
print("\nSaved", out)