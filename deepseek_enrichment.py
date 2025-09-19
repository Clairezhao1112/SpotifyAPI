# deepseek_enrichment.py
import os, re, sys, json, argparse, datetime as dt, requests, pandas as pd
from bs4 import BeautifulSoup

UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
TODAY = dt.date.today()

# ---------- basics ----------
def slugify(s): return re.sub(r"[^a-z0-9]+", "_", (s or "").lower()).strip("_")

def detect_latest_csv(artist=None):
    raw = os.path.join("data", "raw")
    if not os.path.isdir(raw):
        sys.exit("No data/raw directory found. Run main.py first.")
    files = [f for f in os.listdir(raw) if f.endswith("_data.csv")]
    if not files:
        sys.exit("No *_data.csv files in data/raw. Run main.py first.")
    if artist:
        pref = slugify(artist)
        cand = [f for f in files if f.startswith(pref)]
        files = cand or files
    files = sorted(files, key=lambda f: os.path.getmtime(os.path.join(raw, f)), reverse=True)
    return os.path.join(raw, files[0])

# ---------- price extraction ----------
def parse_offers_from_jsonld(html):
    prices = []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", type="application/ld+json"):
        txt = tag.string or ""
        try:
            data = json.loads(txt)
        except Exception:
            continue
        objs = data if isinstance(data, list) else [data]
        for obj in objs:
            if not isinstance(obj, dict): 
                continue
            candidates = obj.get("@graph", [obj])
            for ev in candidates:
                if not (isinstance(ev, dict) and ev.get("@type") in ("Event", ["Event"])):
                    continue
                offers = ev.get("offers", [])
                if isinstance(offers, dict): offers = [offers]
                for off in offers:
                    for k in ("lowPrice", "highPrice", "price"):
                        v = off.get(k)
                        try:
                            if isinstance(v, str): v = float(v.replace(",", "").strip("$"))
                            if isinstance(v, (int, float)): prices.append(float(v))
                        except Exception:
                            pass
    return prices

def parse_price_ranges_inline_json(html):
    """
    Fallback: Ticketmaster often embeds "priceRanges" in inline JSON.
    This parser finds it and extracts min/max values.
    """
    prices = []
    # 1) Quick regex to pull the JSON array for priceRanges
    m = re.search(r'"priceRanges"\s*:\s*(\[[^\]]+\])', html)
    if m:
        try:
            arr = json.loads(m.group(1))
            if isinstance(arr, list):
                for pr in arr:
                    for k in ("min", "max", "minPrice", "maxPrice", "price"):
                        v = pr.get(k)
                        try:
                            if isinstance(v, str): v = float(v.replace(",", "").strip("$"))
                            if isinstance(v, (int, float)): prices.append(float(v))
                        except Exception:
                            pass
        except Exception:
            pass
    # 2) Looser scan: any numeric $"xx.xx" near "price" words
    if not prices:
        for m2 in re.finditer(r'(?i)(price|min|max)\D{0,12}\$?\s*([0-9]+(?:\.[0-9]{1,2})?)', html):
            try:
                prices.append(float(m2.group(2)))
            except Exception:
                pass
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
        mid  = round((2*lo+hi)/3, 2)
        flr  = round((lo+2*hi)/3, 2)
        return {"upper_price":lo,"mid_price":mid,"floor_price":flr,"vip_price":hi}
    p = ps[0]
    return {"upper_price":p,"mid_price":p,"floor_price":p,"vip_price":p}

def scrape_event_prices(url):
    try:
        full = url if url.startswith("http") else f"https://www.ticketmaster.com{url}"
        html = requests.get(full.split("?")[0], headers=UA, timeout=15).text
        prices = parse_offers_from_jsonld(html)
        if not prices:
            prices = parse_price_ranges_inline_json(html)
        return quantile_map(prices)
    except Exception:
        return {"vip_price":None,"floor_price":None,"mid_price":None,"upper_price":None}

# ---------- enrichment pieces ----------
def artist_popularity(df_tracks):
    if "popularity" not in df_tracks.columns or df_tracks.empty:
        return pd.DataFrame(columns=["artist","artist_popularity_avg"])
    pop = (df_tracks.dropna(subset=["popularity"])
           .groupby("artist", as_index=False)["popularity"].mean()
           .rename(columns={"popularity":"artist_popularity_avg"}))
    return pop

def days_to_event(series_like):
    s = pd.Series(series_like) if not isinstance(series_like, pd.Series) else series_like
    s = pd.to_datetime(s, utc=True, errors="coerce")
    ref = pd.Timestamp(TODAY, tz="UTC")
    out = (s - ref).dt.days
    return out.astype("Int64")

def hype_index_row(r):
    pop = r.get("artist_popularity_avg")
    popularity_norm = (float(pop)/100.0) if isinstance(pop,(int,float)) else 0.0
    vip, upper = r.get("vip_price"), r.get("upper_price")
    if isinstance(vip,(int,float)) and isinstance(upper,(int,float)) and upper>0:
        spread_norm = max(0.0, min(1.0, (vip/upper - 1.0)/4.0))
    else:
        spread_norm = 0.0
    d = r.get("days_to_event")
    if pd.notna(d) and isinstance(d,(int,float)) and d >= 0:
        recency = max(0.0, min(1.0, 1 - (d/120.0)))
    else:
        recency = 0.5
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

def enrich(csv_path):
    df = pd.read_csv(csv_path)

    # from main.py: tracks(type=track, ... popularity, url) / events(type=event, ... url)
    df_tracks = df[df.get("type") == "track"].copy()
    df_events = df[df.get("type") == "event"].copy()
    if df_events.empty:
        sys.exit("No events in CSV. Re-run main.py or check your input file.")

    # popularity
    pop = artist_popularity(df_tracks)
    df_events = df_events.merge(pop, on="artist", how="left")

    # seat price tiers
    cache, cols = {}, ["vip_price","floor_price","mid_price","upper_price"]
    for c in cols: df_events[c] = None
    for i, row in df_events.iterrows():
        url = row.get("url")
        if isinstance(url, str) and url:
            if url not in cache: cache[url] = scrape_event_prices(url)
            for k in cols: df_events.at[i, k] = cache[url][k]

    # days to event
    date_series = df_events["date"] if "date" in df_events.columns else df_events.get("tm_event_datetime")
    df_events["days_to_event"] = days_to_event(date_series)

    # hype + risk
    df_events["hype_index"] = df_events.apply(hype_index_row, axis=1)
    df_events["sell_out_risk"] = df_events.apply(sellout_risk, axis=1)

    # ensure showcase columns
    for c in ["artist","city","venue","name","url"]:
        if c not in df_events.columns: df_events[c] = None

    show = ["artist","city","venue","name","vip_price","upper_price","days_to_event","hype_index","sell_out_risk","url"]
    df_show = df_events[show].copy()

    # sort
    risk_order = {"🔴 High":0,"🟡 Medium":1,"🟢 Low":2}
    df_show["_r"] = df_show["sell_out_risk"].map(risk_order).fillna(3)
    df_show = df_show.sort_values(by=["_r","hype_index","days_to_event"], ascending=[True,False,True]) \
                     .drop(columns=["_r"]).reset_index(drop=True)
    return df_show

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Enrich Ticketmaster events with seat tiers, hype index, and sell-out risk.")
    ap.add_argument("--input","-i", help="Path to *_data.csv (from main.py)")
    ap.add_argument("--artist","-a", help="Artist name to locate latest CSV under data/raw")
    # Force output to data/enriched ONLY
    ap.add_argument("--outdir","-o", default=os.path.join("data","enriched"),
                    help="Existing output dir (must be data/enriched or a subdir)")
    args = ap.parse_args()

    csv_path = args.input or detect_latest_csv(args.artist)

    # REQUIRE existing data/enriched (do not create a new top-level folder)
    if not os.path.isdir(args.outdir):
        sys.exit(f"Output directory not found: {args.outdir}\n"
                 f"Create it first under data/enriched/, e.g., mkdir -p data/enriched")

    # also sanity-check we're under data/enriched
    wanted_root = os.path.join("data","enriched")
    if not os.path.abspath(args.outdir).endswith(os.path.normpath(wanted_root)):
        print(f"[warn] Writing outside {wanted_root}. Overriding to {wanted_root}.")
        args.outdir = wanted_root

    enriched = enrich(csv_path)
    base = re.sub(r"_data\.csv$", "", os.path.basename(csv_path))
    out_csv = os.path.join(args.outdir, f"{base}_enriched.csv")
    enriched.to_csv(out_csv, index=False)

    print(f"[done] Enriched events → {out_csv}")
    print(enriched.to_string(index=False))

if __name__ == "__main__":
    main()
