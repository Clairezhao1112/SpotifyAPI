import requests, pandas as pd
from bs4 import BeautifulSoup

# === Your Spotify credentials ===
CID = "a2fcd932726e496dbfb04cec705edea4"
SEC = "1c46ff7084d945f795227d3e408ad2a8"


def get_token():
    """Get Spotify access token"""
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(CID, SEC),
        timeout=15
    )
    r.raise_for_status()
    return r.json()["access_token"]

def get_all_albums(artist_id, headers):
    """Fetch all albums with pagination"""
    albums = []
    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
    params = {"include_groups": "album", "limit": 50}
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        albums.extend(data["items"])
        url = data.get("next")  # pagination
        params = None  # only needed first time
    return albums

# --- Input ---
artist = input("Enter artist name: ").strip()

# --- Spotify: search artist ---
tok = get_token()
h = {"Authorization": f"Bearer {tok}"}

search = requests.get(
    "https://api.spotify.com/v1/search",
    headers=h,
    params={"q": artist, "type": "artist", "limit": 1},
    timeout=15
).json()

if not search["artists"]["items"]:
    exit("Artist not found on Spotify")

aid = search["artists"]["items"][0]["id"]

# --- Fetch all albums + tracks ---
spotify_rows = []
albums = get_all_albums(aid, h)

for alb in albums:
    alb_id = alb["id"]
    alb_name = alb["name"]
    alb_date = alb["release_date"]

    tracks = requests.get(
        f"https://api.spotify.com/v1/albums/{alb_id}/tracks",
        headers=h,
        params={"limit": 50},
        timeout=15
    ).json()["items"]

    for tr in tracks:
        spotify_rows.append({
            "source": "spotify",
            "artist": artist,
            "album": alb_name,
            "album_release_date": alb_date,
            "track_name": tr["name"],
            "event": None,
            "event_date": None,
            "venue": None,
            "city": None,
            "ticket_url": None
        })

df_sp = pd.DataFrame(spotify_rows)

# --- Ticketmaster: scrape events ---
url = f"https://www.ticketmaster.com/search?q={artist.replace(' ', '+')}"
html = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15).text
soup = BeautifulSoup(html, "html.parser")

tm_rows = []
for card in soup.find_all("div", class_="event-card"):
    link = card.find("a", href=True)
    date = card.find("time")
    venue = card.find("span", class_="event-venue")
    city = card.find("span", class_="event-city")

    tm_rows.append({
        "source": "ticketmaster",
        "artist": artist,
        "album": None,
        "album_release_date": None,
        "track_name": None,
        "event": card.get_text(" ", strip=True),
        "event_date": date["datetime"] if date and date.has_attr("datetime") else None,
        "venue": venue.get_text(strip=True) if venue else None,
        "city": city.get_text(strip=True) if city else None,
        "ticket_url": "https://www.ticketmaster.com" + link["href"] if link else None
    })

df_tm = pd.DataFrame(tm_rows)

# --- Combine into one CSV ---
combined = pd.concat([df_sp, df_tm], ignore_index=True)
combined.to_csv(f"{artist.replace(' ', '_')}_spotify_ticketmaster.csv", index=False)

print(f"Saved: {artist.replace(' ', '_')}_spotify_ticketmaster.csv")