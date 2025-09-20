# deepseek_enrichment.py
import os, re, sys, json, time, argparse, datetime as dt, requests, pandas as pd
from bs4 import BeautifulSoup

# ========= Config =========
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
TODAY = dt.date.today()

# --- DeepSeek API (OpenAI-compatible) ---
DEEPSEEK_BASE = os.getenv("DEEPSEEK_BASE", "https://api.deepseek.com/v1")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-a7f42564324a433b836f39b479e4dfa8").strip()
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

def _ds_headers():
    if not DEEPSEEK_API_KEY:
        sys.exit("Missing DEEPSEEK_API_KEY (export it or set in code).")
    return {"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"}

def ds_chat(messages, temperature=0.2, max_tokens=512):
    url = f"{DEEPSEEK_BASE}/chat/completions"
    body = {"model": DEEPSEEK_MODEL, "messages": messages, "temperature": temperature, "max_tokens": max_tokens}
    r = requests.post(url, headers=_ds_headers(), json=body, timeout=30)
    r.raise_for_status()
    data = r.json()
    # OpenAI-compatible shape
    return data["choices"][0]["message"]["content"]

# ========= Helpers =========
def slugify(s): return re.sub(r"[^a-z0-9]+", "_", (s or "").lower()).strip("_")

def detect_latest_csv(artist=None):
    raw = os.path.join("data", "raw")
    if not os.path.isdir(raw): sys.exit("No data/raw directory found. Run main.py first.")
    files = [f for f in os.listdir(raw) if f.endswith("_data.csv")]
    if not files: sys.exit("No *_data.csv in data/raw. Run main.py first.")
    if artist:
        pref = slugify(artist)
        cand = [f for f in files if f.startswith(pref)]
        files = cand or files
    files.sort(key=lambda f: os.path.getmtime(os.path.join(raw, f)), reverse=True)
    return os.path.join(raw, files[0])

# ========= Price extraction (same idea, plus a fallback) =========
def parse_offers_from_jsonld(html):
    prices, soup = [], BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", type="application/ld+json"):
        txt = tag.string or ""
        try: data = json.loads(txt)
        except Exception: continue
        objs = data if isinstance(data, list) else [data]
        for obj in objs:
            if not isinstance(obj, dict): continue
            for ev in (obj.get("@graph", [obj])):
                if not (isinstance(ev, dict) and ev.get("@type") in ("Event", ["Event"])): continue
                offers = ev.get("offers", [])
                if isinstance(offers, dict): offers = [offers]
                for off in offers:
                    for k in ("lowPrice", "highPrice", "price"):
                        v = off.get(k)
                        try:
                            if isinstance(v, str): v = float(v.replace(",", "").strip("$"))
                            if isinstance(v, (int, float)): prices.append(float(v))
                        except Exception: pass
    return prices

def parse_price_ranges_inline_json(html):
    prices = []
    m = re.search(r'"priceRanges"\s*:\s*(\[[^\]]+\])', html)
    if m:
        try:
            arr = json.loads(m.group(1))
            if isinstance(arr, list):
                for pr in arr:
                    for k in ("min","max","minPrice","maxPrice","price"):
                        v = pr.get(k)
                        try:
                            if isinstance(v, str): v = float(v.replace(",", "").strip("$"))
                            if isinstance(v, (int, float)): prices.append(float(v))
                        except Exception: pass
        except Exception: pass
    if not prices:
        for m2 in re.finditer(r'(?i)(price|min|max)\D{0,12}\$?\s*([0-9]+(?:\.[0-9]{1,2})?)', html):
            try: prices.append(float(m2.group(2)))
            except Exception: pass
    return prices

def quantile_map(prices):
    if not prices: return {"vip_price":None,"floor_price":None,"mid_price":None,"upper_price":None}
    ps = sorted({float(p) for p in prices if isinstance(p,(int,float)) and p>0})
    if not ps: return {"vip_price":None,"floor_price":None,"mid_price":None,"upper_price":None}
    if len(ps) >= 4:
        q0, q1, q2, q3 = ps[0], ps[len(ps)//3], ps[(2*len(ps))//3], ps[-1]
        return {"upper_price":q0,"mid_price":q1,"floor_price":q2,"vip_price":q3}
    if len(ps) == 3:
        return {"upper_price":ps[0],"mid_price":ps[1],"floor_price":ps[1],"vip_price":ps[2]}
    if len(ps) == 2:
        lo, hi = ps[0], ps[1]
        mid = round((2*lo+hi)/3, 2); flr = round((lo+2*hi)/3, 2)
        return {"upper_price":lo,"mid_price":mid,"floor_price":flr,"vip_price":hi}
    p = ps[0]
    return {"upper_price":p,"mid_price":p,"floor_price":p,"vip_price":p}

def scrape_event_prices(url):
    try:
        full = url if url.startswith("http") else f"https://www.ticketmaster.com{url}"
        html = requests.get(full.split("?")[0], headers=UA, timeout=15).text
        prices = parse_offers_from_jsonld(html) or parse_price_ranges_inline_json(html)
        return quantile_map(prices)
    except Exception:
        return {"vip_price":None,"floor_price":None,"mid_price":None,"upper_price":None}

# ========= Enrichment logic =========
def artist_popularity(df_tracks):
    if "popularity" not in df_tracks.columns or df_tracks.empty:
        return pd.DataFrame(columns=["artist","artist_popularity_avg"])
    return (df_tracks.dropna(subset=["popularity"])
            .groupby("artist", as_index=False)["popularity"].mean()
            .rename(columns={"popularity":"artist_popularity_avg"}))

def days_to_event(series_like):
    s = pd.Series(series_like) if not isinstance(series_like, pd.Series) else series_like
    s = pd.to_datetime(s, utc=True, errors="coerce")
    ref = pd.Timestamp(TODAY, tz="UTC")
    return ((s - ref).dt.days).astype("Int64")

def hype_index_row(r):
    pop = r.get("artist_popularity_avg")
    popularity_norm = (float(pop)/100.0) if isinstance(pop,(int,float)) else 0.0
    vip, upper = r.get("vip_price"), r.get("upper_price")
    spread_norm = max(0.0, min(1.0, ((vip/upper)-1.0)/4.0)) if (isinstance(vip,(int,float)) and isinstance(upper,(int,float)) and upper>0) else 0.0
    d = r.get("days_to_event")
    recency = max(0.0, min(1.0, 1 - (d/120.0))) if (pd.notna(d) and isinstance(d,(int,float)) and d>=0) else 0.5
    return round(100*(0.5*popularity_norm + 0.3*spread_norm + 0.2*recency), 2)

def sellout_risk(r):
    hype = r.get("hype_index") or 0
    vip, upper = r.get("vip_price"), r.get("upper_price")
    spread = (vip/upper) if (isinstance(vip,(int,float)) and isinstance(upper,(int,float)) and upper>0) else 1.0
    d = r.get("days_to_event")
    soon = (pd.notna(d) and isinstance(d,(int,float)) and d <= 21)
    if hype >= 80 and (spread >= 3 or soon): return "🔴 High"
    if hype >= 60: return "🟡 Medium"
    return "🟢 Low"

# ---- DeepSeek row-level enrichment ----
DS_SYSTEM = (
    "You are an analyst. Given a JSON event with fields (artist, name, city, venue, date, prices), "
    "return a STRICT JSON object with keys: "
    "category (string), tags (list of 1-5 short tags), event_summary (<=20 words), "
    "sentiment_label (Positive/Neutral/Negative), sentiment_score (float -1..1), "
    "moderation_flag (Safe/Unsafe), anomaly_note (short reason or empty), "
    "language (BCP47 like 'en' or 'es'), description_en (<=30 words, English). "
    "Keep it terse; no prose. If missing data, infer cautiously."
)

def ds_enrich_event_rows(df_events, batch_size=6, sleep_s=0.6):
    # Ensure target columns exist
    cols = ["category","tags","event_summary","sentiment_label","sentiment_score",
            "moderation_flag","anomaly_note","language","description_en"]
    for c in cols:
        if c not in df_events.columns: df_events[c] = None

    # Build minimal payload per row for the prompt
    records = df_events.to_dict(orient="records")
    for i in range(0, len(records), batch_size):
        chunk = records[i:i+batch_size]
        # Compose a batched user message with numbered items (saves tokens)
        items = []
        for idx, r in enumerate(chunk):
            items.append({
                "idx": i+idx,
                "artist": r.get("artist"),
                "name": r.get("name"),
                "city": r.get("city"),
                "venue": r.get("venue"),
                "date": r.get("date") or r.get("tm_event_datetime"),
                "prices": {
                    "vip": r.get("vip_price"),
                    "upper": r.get("upper_price"),
                    "mid": r.get("mid_price"),
                    "floor": r.get("floor_price")
                }
            })
        user_payload = {"events": items}
        try:
            content = ds_chat(
                messages=[
                    {"role": "system", "content": DS_SYSTEM},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)}
                ],
                temperature=0.1,
                max_tokens=700
            )
            # Expecting a JSON array/object keyed by idx
            # Try to parse robustly
            parsed = None
            try:
                parsed = json.loads(content)
            except Exception:
                # Try to extract JSON substring
                m = re.search(r'(\{.*\}|\[.*\])', content, flags=re.S)
                if m:
                    try: parsed = json.loads(m.group(1))
                    except Exception: parsed = None
            if not parsed: 
                time.sleep(sleep_s)
                continue

            # Normalize to dict keyed by idx -> enrichment
            if isinstance(parsed, list):
                idx_map = {int(x.get("idx")): x for x in parsed if isinstance(x, dict) and "idx" in x}
            elif isinstance(parsed, dict) and "events" in parsed:
                idx_map = {int(x.get("idx")): x for x in parsed["events"] if isinstance(x, dict) and "idx" in x}
            elif isinstance(parsed, dict):
                # Maybe already a flat dict keyed by idx
                idx_map = {int(k): v for k, v in parsed.items() if re.fullmatch(r"\d+", str(k))}
            else:
                idx_map = {}

            for idx, ev in idx_map.items():
                for c in ["category","event_summary","moderation_flag","anomaly_note","language","description_en","sentiment_label"]:
                    if c in ev:
                        df_events.at[idx, c] = ev.get(c)
                if "sentiment_score" in ev:
                    try: df_events.at[idx, "sentiment_score"] = float(ev["sentiment_score"])
                    except Exception: df_events.at[idx, "sentiment_score"] = None
                if "tags" in ev:
                    v = ev["tags"]
                    if isinstance(v, list): df_events.at[idx, "tags"] = ", ".join([str(t) for t in v[:5]])
                    else: df_events.at[idx, "tags"] = str(v)
        except requests.HTTPError as e:
            # Mild backoff and continue
            time.sleep(1.2)
        time.sleep(sleep_s)
    return df_events

# ========= Main enrichment pipeline =========
def enrich(csv_path):
    df = pd.read_csv(csv_path)

    # Split
    df_tracks = df[df.get("type") == "track"].copy()
    df_events = df[df.get("type") == "event"].copy()
    if df_events.empty:
        sys.exit("No events in CSV. Re-run main.py or check your input file.")

    # Popularity by artist
    pop = artist_popularity(df_tracks)
    df_events = df_events.merge(pop, on="artist", how="left")

    # Prices per event (cached)
    cache, cols = {}, ["vip_price","floor_price","mid_price","upper_price"]
    for c in cols: df_events[c] = None
    for i, row in df_events.iterrows():
        url = row.get("url")
        if isinstance(url, str) and url:
            if url not in cache: cache[url] = scrape_event_prices(url)
            for k in cols: df_events.at[i, k] = cache[url][k]

    # Days to event, Hype, Risk
    date_series = df_events["date"] if "date" in df_events.columns else df_events.get("tm_event_datetime")
    df_events["days_to_event"] = days_to_event(date_series)
    df_events["hype_index"] = df_events.apply(hype_index_row, axis=1)
    df_events["sell_out_risk"] = df_events.apply(sellout_risk, axis=1)

    # DeepSeek enrichment (categorize, summarize, sentiment, moderation, anomaly, translation)
    df_events = ds_enrich_event_rows(df_events, batch_size=6, sleep_s=0.6)

    # Select + sort for showcase
    show = ["artist","city","venue","name",
            "vip_price","upper_price","days_to_event","hype_index","sell_out_risk",
            "category","tags","event_summary","sentiment_label","sentiment_score",
            "moderation_flag","anomaly_note","language","description_en","url"]
    for c in show:
        if c not in df_events.columns: df_events[c] = None

    risk_order = {"🔴 High":0,"🟡 Medium":1,"🟢 Low":2}
    df_events["_r"] = df_events["sell_out_risk"].map(risk_order).fillna(3)
    df_events = df_events.sort_values(by=["_r","hype_index","days_to_event"], ascending=[True,False,True]) \
                         .drop(columns=["_r"]).reset_index(drop=True)
    return df_events[show]

# ========= CLI =========
def main():
    ap = argparse.ArgumentParser(description="Enrich events with seat tiers, hype index, and DeepSeek AI tags/summaries.")
    ap.add_argument("--input","-i", help="Path to *_data.csv (from main.py)")
    ap.add_argument("--artist","-a", help="Artist name to locate latest CSV under data/raw")
    ap.add_argument("--outdir","-o", default=os.path.join("data","enriched"),
                    help="Existing output dir (must be data/enriched)")
    args = ap.parse_args()

    csv_path = args.input or detect_latest_csv(args.artist)

    # Use ONLY data/enriched; require it to exist (do not create another folder)
    if not os.path.isdir(args.outdir):
        sys.exit(f"Output directory not found: {args.outdir}\nCreate it first: mkdir -p data/enriched")

    wanted_root = os.path.join("data","enriched")
    if not os.path.abspath(args.outdir).endswith(os.path.normpath(wanted_root)):
        print(f"[warn] For consistency, writing to {wanted_root}.")
        args.outdir = wanted_root

    enriched = enrich(csv_path)
    base = re.sub(r"_data\.csv$", "", os.path.basename(csv_path))
    out_csv = os.path.join(args.outdir, f"{base}_enriched.csv")
    enriched.to_csv(out_csv, index=False)

    print(f"[done] Enriched events → {out_csv}")
    print(enriched.to_string(index=False))

if __name__ == "__main__":
    main()
