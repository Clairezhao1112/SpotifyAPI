import os, re, json, requests, pandas as pd
from datetime import date
from bs4 import BeautifulSoup

# Config
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-a7f42564324a433b836f39b479e4dfa8")
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

def get_latest_csv():
    raw_dir = "data/raw"
    files = [f for f in os.listdir(raw_dir) if f.endswith("_data.csv")]
    if not files: raise FileNotFoundError("No data files found")
    return os.path.join(raw_dir, max(files, key=lambda f: os.path.getmtime(os.path.join(raw_dir, f))))

def deepseek_chat(prompt):
    response = requests.post(
        "https://api.deepseek.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
        json={
            "model": "deepseek-chat", "temperature": 0.1, "max_tokens": 300,
            "messages": [{"role": "user", "content": prompt}]
        }
    )
    return response.json()["choices"][0]["message"]["content"]

def get_event_prices(url):
    try:
        html = requests.get(url.split("?")[0], headers=UA, timeout=10).text
        prices = []
        
        # Extract from JSON-LD
        for script in BeautifulSoup(html, "html.parser").find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if isinstance(item, dict):
                        for obj in item.get("@graph", [item]):
                            if obj.get("@type") == "Event":
                                offers = obj.get("offers", [])
                                if isinstance(offers, dict): offers = [offers]
                                for offer in offers:
                                    for key in ["lowPrice", "highPrice", "price"]:
                                        try:
                                            price = float(str(offer.get(key, "")).replace("$", "").replace(",", ""))
                                            if price > 0: prices.append(price)
                                        except: pass
            except: pass
        
        # Fallback: regex price extraction
        if not prices:
            for match in re.finditer(r'\$([0-9]+(?:\.[0-9]{2})?)', html):
                try: prices.append(float(match.group(1)))
                except: pass
        
        if prices:
            prices = sorted(set(prices))
            return {
                "min_price": prices[0], 
                "max_price": prices[-1],
                "avg_price": round(sum(prices)/len(prices), 2)
            }
    except: pass
    return {"min_price": None, "max_price": None, "avg_price": None}

def calculate_metrics(row):
    # Days to event
    try:
        event_date = pd.to_datetime(row.get("date"), errors="coerce")
        days_out = (event_date.date() - date.today()).days if pd.notna(event_date) else None
    except: days_out = None
    
    # Hype score (0-100)
    popularity = row.get("popularity", 0) or 0
    min_p, max_p = row.get("min_price", 0) or 0, row.get("max_price", 0) or 0
    price_spread = (max_p / min_p) if min_p and max_p and min_p > 0 else 1
    urgency = max(0, 1 - (days_out / 60)) if days_out and days_out >= 0 else 0.5
    
    hype = round(popularity * 0.4 + min(price_spread * 10, 40) * 0.4 + urgency * 20, 1)
    
    # Risk assessment
    if hype >= 70 and (price_spread >= 3 or (days_out and days_out <= 14)):
        risk = "🔴 High"
    elif hype >= 40:
        risk = "🟡 Medium" 
    else:
        risk = "🟢 Low"
    
    return {"days_to_event": days_out, "hype_score": hype, "sellout_risk": risk}

def enrich_with_ai(events_df):
    """Add AI-generated insights to events"""
    for i, row in events_df.iterrows():
        try:
            prompt = f"""Analyze this concert event and return JSON with exactly these fields:
{{"category": "Concert/Festival/Tour", "tags": ["genre", "vibe"], "summary": "brief description", "sentiment": "Positive/Neutral/Negative"}}

Event: {row.get('name', 'Unknown')} by {row.get('artist', 'Unknown')} in {row.get('city', 'Unknown')} at {row.get('venue', 'Unknown')}"""
            
            response = deepseek_chat(prompt)
            
            # Extract JSON from response
            try:
                ai_data = json.loads(response)
                events_df.at[i, 'category'] = ai_data.get('category', 'Concert')
                events_df.at[i, 'tags'] = ', '.join(ai_data.get('tags', [])[:3])
                events_df.at[i, 'ai_summary'] = ai_data.get('summary', '')[:50]
                events_df.at[i, 'sentiment'] = ai_data.get('sentiment', 'Neutral')
            except:
                # Fallback values
                events_df.at[i, 'category'] = 'Concert'
                events_df.at[i, 'tags'] = 'live-music'
                events_df.at[i, 'ai_summary'] = f"{row.get('artist', '')} live performance"
                events_df.at[i, 'sentiment'] = 'Positive'
                
        except Exception as e:
            print(f"AI enrichment failed for row {i}: {e}")
            continue
    
    return events_df

def main():
    # Get latest data file
    csv_path = get_latest_csv()
    print(f"Processing: {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    # Separate tracks and events, organize events
    tracks = df[df.get("type") == "track"].copy()
    events = df[df.get("type") == "event"].copy()
    
    if events.empty:
        print("No events found in data")
        return
    
    # Calculate artist popularity from tracks
    if not tracks.empty and "popularity" in tracks.columns:
        artist_pop = tracks.groupby("artist")["popularity"].mean().round(1).to_dict()
        events["popularity"] = events["artist"].map(artist_pop)
    
    # Organize event columns
    event_cols = ["artist", "name", "date", "venue", "city", "url"]
    events = events[[col for col in event_cols if col in events.columns]].copy()
    
    # Add pricing data
    print("Fetching pricing data...")
    price_data = []
    for url in events["url"].dropna():
        prices = get_event_prices(url)
        price_data.append(prices)
    
    if price_data:
        price_df = pd.DataFrame(price_data)
        events = pd.concat([events.reset_index(drop=True), price_df], axis=1)
    
    # Calculate metrics
    print("Calculating metrics...")
    metrics = events.apply(calculate_metrics, axis=1, result_type='expand')
    events = pd.concat([events, metrics], axis=1)
    
    # AI enrichment
    print("Adding AI insights...")
    events = enrich_with_ai(events)
    
    # Sort by risk and hype
    risk_order = {"🔴 High": 0, "🟡 Medium": 1, "🟢 Low": 2}
    events["risk_sort"] = events["sellout_risk"].map(risk_order).fillna(3)
    events = events.sort_values(["risk_sort", "hype_score"], ascending=[True, False]).drop("risk_sort", axis=1)
    
    # Save enriched data
    os.makedirs("data/enriched", exist_ok=True)
    base_name = os.path.basename(csv_path).replace("_data.csv", "")
    output_path = f"data/enriched/{base_name}_enriched.csv"
    
    # Create final organized dataset with all original data plus enrichments
    final_df = pd.concat([
        tracks,  # Keep all track data
        events   # Enriched events
    ], ignore_index=True)
    
    final_df.to_csv(output_path, index=False)
    
    print(f"\nSaved enriched data: {output_path}")
    print(f"Total records: {len(final_df)} ({len(tracks)} tracks, {len(events)} events)")
    
    # Show preview of enriched events
    display_cols = ["artist", "name", "city", "days_to_event", "hype_score", "sellout_risk", "category", "tags"]
    available_cols = [col for col in display_cols if col in events.columns]
    print(f"\nEnriched Events Preview:")
    print(events[available_cols].head(10).to_string(index=False))

if __name__ == "__main__":
    main()