import os, re, json, requests, pandas as pd
from datetime import date
from bs4 import BeautifulSoup

DEEPSEEK_API_KEY = "sk-a7f42564324a433b836f39b479e4dfa8"
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}


def get_latest_csv():

    # Return the newest *_data.csv from data/raw/ by modification time.

    files = [f for f in os.listdir("data/raw") if f.endswith("_data.csv")]
    return os.path.join("data/raw", max(files, key=lambda f: os.path.getmtime(os.path.join("data/raw", f))))


def get_prices(url):
    """
    Extract min/max ticket prices from a Ticketmaster event page.

    Preference order:
      1) JSON-LD (script[type="application/ld+json"]) -> Event.offers.price/lowPrice/highPrice
      2) Fallback regex for literal $ amounts in HTML

    Returns: {"min_price": float|None, "max_price": float|None}
    """
    try:
        html = requests.get(url.split("?")[0], headers=UA, timeout=10).text
        prices = []

        # ---- Preferred: parse structured JSON-LD blocks ----
        for script in BeautifulSoup(html, "html.parser").find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                for item in (data if isinstance(data, list) else [data]):
                    for obj in item.get("@graph", [item]):
                        if obj.get("@type") == "Event":
                            offers = obj.get("offers", [])
                            offers = offers if isinstance(offers, list) else [offers]
                            for offer in offers:
                                for key in ["lowPrice", "highPrice", "price"]:
                                    try:
                                        price = float(str(offer.get(key, "")).replace("$", "").replace(",", ""))
                                        if price > 0:
                                            prices.append(price)
                                    except:
                                        pass
            except:
                pass

        # ---- Fallback: any $xx.xx-like amounts in raw HTML ----
        if not prices:
            prices = [
                float(m.group(1))
                for m in re.finditer(r'\$([0-9]+(?:\.[0-9]{2})?)', html)
                if m.group(1)
            ]

        if prices:
            prices = sorted(set(prices))
            return {"min_price": prices[0], "max_price": prices[-1]}
    except:
        pass

    return {"min_price": None, "max_price": None}


def calc_metrics(row):
    """
    Compute derived fields for an event:
      - days_to_event: days from today to event date (None if unknown)
      - hype_score: composite of popularity, price spread, and time urgency (~0–100)
      - sellout_risk: 🔴/🟡/🟢 label based on hype and near-term timing
    """
    try:
        days = (pd.to_datetime(row.get("date")).date() - date.today()).days
    except:
        days = None

    pop = row.get("popularity", 0) or 0
    min_p, max_p = row.get("min_price", 0) or 0, row.get("max_price", 0) or 0
    spread = (max_p / min_p) if min_p and max_p > 0 else 1  # avoid div-by-zero
    urgency = max(0, 1 - (days / 60)) if (days is not None and days >= 0) else 0.5

    hype = round(
        pop * 0.4 +                # popularity weight
        min(spread * 10, 40) * 0.4 +  # clamp spread contribution
        urgency * 20,              # urgency contribution
        1
    )

    risk = (
        "🔴 High" if hype >= 70 and (spread >= 3 or (days is not None and days <= 14))
        else "🟡 Medium" if hype >= 40
        else "🟢 Low"
    )
    return {"days_to_event": days, "hype_score": hype, "sellout_risk": risk}


def add_ai_data(events):
    """
    Call DeepSeek once per event to generate a brief (8–12 words) blurb.
    On any error, fall back to "<artist> live at <venue>".

    NOTE: Uses hardcoded DEEPSEEK_API_KEY (no env vars).
    """
    artist = events['artist'].iloc[0] if not events.empty else "Unknown"

    for i, row in events.iterrows():
        try:
            venue = row.get('venue', '')
            city = row.get('city', '')
            prompt = (
                f"Write a brief 8-12 word description for this {artist} concert "
                f"at {venue} in {city}. Return only the description, no JSON."
            )

            resp = requests.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "deepseek-chat",
                    "temperature": 0.3,
                    "max_tokens": 100,
                    "messages": [{"role": "user", "content": prompt}]
                },
                timeout=20
            ).json()["choices"][0]["message"]["content"]

            events.at[i, 'description'] = resp.strip('"').strip()[:50]
        except:
            events.at[i, 'description'] = f"{artist} live at {venue}"[:50]

    return events


def main():
    """
    Pipeline:
      1) Load the latest combined CSV (tracks + events) from data/raw/.
      2) Split into track rows and event rows.
      3) Map mean popularity per artist from tracks onto events (if tracks exist).
      4) Scrape prices for each event URL; compute metrics; add AI blurbs.
      5) Sort events: High risk first, then by hype descending.
      6) Save combined (tracks + enriched events) to data/enriched/.
      7) Print a compact summary to stdout.
    """
    df = pd.read_csv(get_latest_csv())
    tracks = df[df.get("type") == "track"].copy()
    events = df[df.get("type") == "event"].copy()

    if events.empty:
        return print("No events found")

    # Popularity backfill from tracks (artist-level mean)
    if not tracks.empty:
        mean_pop = tracks.groupby("artist")["popularity"].mean().to_dict()
        events["popularity"] = events["artist"].map(mean_pop)

    # Scrape ticket prices per event
    for i, row in events.iterrows():
        if pd.notna(row.get("url")):
            for k, v in get_prices(row["url"]).items():
                events.at[i, k] = v

    # Compute metrics and add AI blurbs
    metrics = events.apply(calc_metrics, axis=1, result_type='expand')
    events = pd.concat([events, metrics], axis=1)
    events = add_ai_data(events)

    # Sort: High risk first (🔴 -> 🟡 -> 🟢), then by hype desc
    risk_order = {"🔴 High": 0, "🟡 Medium": 1, "🟢 Low": 2}
    events = events.sort_values(
        ["sellout_risk", "hype_score"],
        key=lambda x: x.map(risk_order) if x.name == "sellout_risk" else x,
        ascending=[True, False]
    )

    # Save enriched CSV
    os.makedirs("data/enriched", exist_ok=True)
    output = f"data/enriched/{os.path.basename(get_latest_csv()).replace('_data.csv', '')}_enriched.csv"
    pd.concat([tracks, events]).to_csv(output, index=False)

    # Console summary
    print(f"✅ SAVED: {output} | {len(events)} events")
    print("=" * 100)
    print(f"🎵 {events['artist'].iloc[0]} - EVENTS")
    print("=" * 100)

    for _, e in events.iterrows():
        name = e.get('name', 'TBA')[:30]
        venue = f"{e.get('venue', 'TBA')[:20]} - {e.get('city', 'TBA')[:10]}"
        risk = e.get('sellout_risk', '🟢 Low')
        hype = e.get('hype_score', 0)
        days = e.get('days_to_event', 'TBA')
        desc = e.get('description', 'Concert')[:35]
        print(f"{name:<30} | {venue:<32} | {risk} {hype:>4.1f}🔥 {str(days):>3}d | {desc}")

    print("=" * 100)
    high_risk = len(events[events['sellout_risk'].str.contains('🔴')])
    print(f"🎯 {len(events)} events | {high_risk} high-risk | {events['hype_score'].mean():.1f} avg hype")


if __name__ == "__main__":
    main()
