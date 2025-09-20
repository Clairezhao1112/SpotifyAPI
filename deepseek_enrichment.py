import os, re, json, requests, pandas as pd
from datetime import date
from bs4 import BeautifulSoup

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-a7f42564324a433b836f39b479e4dfa8")
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

def get_latest_csv():
    files = [f for f in os.listdir("data/raw") if f.endswith("_data.csv")]
    return os.path.join("data/raw", max(files, key=lambda f: os.path.getmtime(os.path.join("data/raw", f))))

def get_prices(url):
    try:
        html = requests.get(url.split("?")[0], headers=UA, timeout=10).text
        prices = []
        for script in BeautifulSoup(html, "html.parser").find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                for item in (data if isinstance(data, list) else [data]):
                    for obj in item.get("@graph", [item]):
                        if obj.get("@type") == "Event":
                            for offer in (obj.get("offers", []) if isinstance(obj.get("offers", []), list) else [obj.get("offers", {})]):
                                for key in ["lowPrice", "highPrice", "price"]:
                                    try: 
                                        price = float(str(offer.get(key, "")).replace("$", "").replace(",", ""))
                                        if price > 0: prices.append(price)
                                    except: pass
            except: pass
        if not prices:
            prices = [float(m.group(1)) for m in re.finditer(r'\$([0-9]+(?:\.[0-9]{2})?)', html) if m.group(1)]
        if prices:
            prices = sorted(set(prices))
            return {"min_price": prices[0], "max_price": prices[-1]}
    except: pass
    return {"min_price": None, "max_price": None}

def calc_metrics(row):
    try: days = (pd.to_datetime(row.get("date")).date() - date.today()).days
    except: days = None
    
    pop = row.get("popularity", 0) or 0
    min_p, max_p = row.get("min_price", 0) or 0, row.get("max_price", 0) or 0
    spread = (max_p / min_p) if min_p and max_p > 0 else 1
    urgency = max(0, 1 - (days / 60)) if days and days >= 0 else 0.5
    hype = round(pop * 0.4 + min(spread * 10, 40) * 0.4 + urgency * 20, 1)
    
    risk = "🔴 High" if hype >= 70 and (spread >= 3 or (days and days <= 14)) else "🟡 Medium" if hype >= 40 else "🟢 Low"
    return {"days_to_event": days, "hype_score": hype, "sellout_risk": risk}

def add_ai_data(events):
    artist = events['artist'].iloc[0] if not events.empty else "Unknown"
    for i, row in events.iterrows():
        try:
            prompt = f"Concert analysis JSON: {{'category': 'Stadium/Arena/Club', 'vibe': 'High Energy/Intimate'}} for {artist} at {row.get('venue', '')}"
            resp = requests.post("https://api.deepseek.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                json={"model": "deepseek-chat", "temperature": 0.1, "max_tokens": 150, "messages": [{"role": "user", "content": prompt}]}
            ).json()["choices"][0]["message"]["content"]
            
            data = json.loads(resp.strip('```json').strip('```'))
            events.at[i, 'category'] = data.get('category', 'Concert')
            events.at[i, 'vibe'] = data.get('vibe', 'High Energy')
        except:
            events.at[i, 'category'] = 'Concert'
            events.at[i, 'vibe'] = 'High Energy'
    return events

def main():
    df = pd.read_csv(get_latest_csv())
    tracks, events = df[df.get("type") == "track"].copy(), df[df.get("type") == "event"].copy()
    
    if events.empty: return print("No events found")
    
    # Add popularity and pricing
    if not tracks.empty: events["popularity"] = events["artist"].map(tracks.groupby("artist")["popularity"].mean().to_dict())
    
    for i, row in events.iterrows():
        if pd.notna(row.get("url")):
            for k, v in get_prices(row["url"]).items(): events.at[i, k] = v
    
    # Calculate metrics and add AI data
    metrics = events.apply(calc_metrics, axis=1, result_type='expand')
    events = pd.concat([events, metrics], axis=1)
    events = add_ai_data(events)
    
    # Sort and save
    risk_order = {"🔴 High": 0, "🟡 Medium": 1, "🟢 Low": 2}
    events = events.sort_values(["sellout_risk", "hype_score"], key=lambda x: x.map(risk_order) if x.name == "sellout_risk" else x, ascending=[True, False])
    
    os.makedirs("data/enriched", exist_ok=True)
    output = f"data/enriched/{os.path.basename(get_latest_csv()).replace('_data.csv', '')}_enriched.csv"
    pd.concat([tracks, events]).to_csv(output, index=False)
    
    # Display
    print(f"✅ SAVED: {output} | {len(events)} events")
    print("="*100)
    print(f"🎵 {events['artist'].iloc[0]} - EVENTS")
    print("="*100)
    
    for _, e in events.iterrows():
        name = e.get('name', 'TBA')[:30]
        venue = f"{e.get('venue', 'TBA')[:20]} - {e.get('city', 'TBA')[:10]}"
        risk = e.get('sellout_risk', '🟢 Low')
        hype = e.get('hype_score', 0)
        days = e.get('days_to_event', 'TBA')
        cat = e.get('category', 'Show')[:10]
        print(f"{name:<30} | {venue:<32} | {risk} {hype:>4.1f}🔥 {str(days):>3}d {cat}")
    
    print("="*100)
    high_risk = len(events[events['sellout_risk'].str.contains('🔴')])
    print(f"🎯 {len(events)} events | {high_risk} high-risk | {events['hype_score'].mean():.1f} avg hype")

if __name__ == "__main__": main()