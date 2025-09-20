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
    artist_name = events_df['artist'].iloc[0] if not events_df.empty else "Unknown"
    
    for i, row in events_df.iterrows():
        try:
            # More specific prompt for concert analysis
            event_name = row.get('name', '')
            venue = row.get('venue', '')
            city = row.get('city', '')
            
            prompt = f"""Analyze this {artist_name} concert event and return JSON:
{{"category": "Stadium Tour/Arena Show/Festival/Intimate Venue", "tour_type": "World Tour/Residency/Festival/Special Event", "tags": ["pop", "sold-out", "vip"], "summary": "15-word description", "vibe": "High Energy/Intimate/Festival/Exclusive"}}

Event: "{event_name}" by {artist_name}
Venue: {venue} in {city}
Context: This is a live concert performance by {artist_name}."""
            
            response = deepseek_chat(prompt)
            
            # Extract JSON from response  
            try:
                ai_data = json.loads(response.strip('```json').strip('```').strip())
                events_df.at[i, 'category'] = ai_data.get('category', 'Concert')
                events_df.at[i, 'tour_type'] = ai_data.get('tour_type', 'Tour')
                events_df.at[i, 'tags'] = ', '.join(ai_data.get('tags', [])[:4])
                events_df.at[i, 'summary'] = ai_data.get('summary', f'{artist_name} live at {venue}')[:60]
                events_df.at[i, 'vibe'] = ai_data.get('vibe', 'High Energy')
            except:
                # Better fallback values
                events_df.at[i, 'category'] = 'Arena Show' if 'arena' in venue.lower() or 'center' in venue.lower() else 'Concert'
                events_df.at[i, 'tour_type'] = 'World Tour' if len(events_df) > 10 else 'Tour'
                events_df.at[i, 'tags'] = f'{artist_name.lower().replace(" ", "-")}, live, concert'
                events_df.at[i, 'summary'] = f'{artist_name} performing live at {venue}'
                events_df.at[i, 'vibe'] = 'High Energy'
                
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
    
    print(f"\n✅ SAVED: {output_path}")
    print(f"📊 TOTAL: {len(final_df)} records ({len(tracks)} tracks, {len(events)} events)\n")
    
    # Side-by-side display: Albums/Tracks (Left) | Events (Right)
    print("=" * 120)
    print(f"🎵 {events['artist'].iloc[0] if not events.empty else 'ARTIST'} - MUSIC CATALOG & LIVE EVENTS")
    print("=" * 120)
    
    # Left side - Albums/Tracks summary
    if not tracks.empty:
        album_summary = tracks.groupby('album').agg({
            'name': 'count',
            'popularity': 'mean'
        }).round(1).reset_index()
        album_summary.columns = ['Album', 'Tracks', 'Avg_Pop']
        
        print("🎼 DISCOGRAPHY".ljust(60) + "🎤 UPCOMING CONCERTS")
        print("-" * 60 + "-" * 60)
        
        # Display albums and events side by side
        max_rows = max(len(album_summary), len(events))
        
        for i in range(max_rows):
            left_line = ""
            right_line = ""
            
            # Left side - Albums
            if i < len(album_summary):
                album = album_summary.iloc[i]
                left_line = f"{album['Album'][:35]:<35} {album['Tracks']:>2}📀 {album['Avg_Pop']:>4.1f}⭐"
            else:
                left_line = " " * 60
            
            # Right side - Events  
            if i < len(events):
                event = events.iloc[i]
                venue_city = f"{event.get('venue', 'TBA')[:20]} - {event.get('city', 'TBA')}"
                risk_emoji = event.get('sellout_risk', '🟢 Low')[:7]
                hype = event.get('hype_score', 0)
                days = event.get('days_to_event', 'TBA')
                
                right_line = f"{venue_city[:35]:<35} {risk_emoji} {hype:>4.1f}🔥 {str(days):>3}d"
            
            print(left_line + right_line)
        
        print("-" * 120)
        
        # Footer with key insights
        if not events.empty:
            high_risk = len(events[events['sellout_risk'].str.contains('🔴', na=False)])
            avg_hype = events['hype_score'].mean()
            next_show_days = events['days_to_event'].min()
            
            print(f"🎯 INSIGHTS: {high_risk} high-risk shows | {avg_hype:.1f} avg hype | Next show in {next_show_days} days")
            
            # Show top 3 hottest events
            print(f"\n🔥 HOTTEST SHOWS:")
            top_events = events.head(3)
            for _, event in top_events.iterrows():
                category = event.get('category', 'Concert')
                vibe = event.get('vibe', 'High Energy')
                summary = event.get('summary', '')[:50]
                print(f"   • {event.get('name', 'Event')[:30]} - {category} | {vibe} | {summary}")
    
    else:
        # Just events if no tracks
        print("🎤 LIVE EVENTS SCHEDULE")
        print("-" * 60)
        for _, event in events.head(10).iterrows():
            venue_info = f"{event.get('venue', 'TBA')} - {event.get('city', 'TBA')}"
            risk = event.get('sellout_risk', '🟢 Low')
            hype = event.get('hype_score', 0)
            print(f"{venue_info[:40]:<40} {risk} {hype:>4.1f}🔥")
    
    print("=" * 120)

if __name__ == "__main__":
    main()