#!/usr/bin/env python3
"""
THT Dual Scan — runs every 5 min, writes docs/results.json for the dashboard.
Pulls daily OHLCV from Yahoo, computes THT Fair Value Bands + B-Xtrender,
detects flips between yesterday and today, writes results to JSON.
"""
import json, urllib.request, math, os, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
TICKERS = json.load(open(os.path.join(HERE, "sp500_tickers.json")))
try:
    SHARES = {k: v["shares"] for k, v in json.load(open(os.path.join(HERE, "shares_outstanding.json"))).items()}
except FileNotFoundError:
    SHARES = {}

def live_mcap(sym, price):
    """Live mcap in $B = price × shares_outstanding / 1e9. Falls back to MCAPS dict."""
    s = SHARES.get(sym)
    if s and price:
        return round(price * s / 1e9)
    return MCAPS.get(sym, 0)

# ─── Indicator math (ported from THT Pine source) ─────────────────────────
def sma(values, length):
    if len(values) < length: return None
    return sum(values[-length:]) / length

def ema_series(values, length):
    if len(values) < length: return [None] * len(values)
    k = 2 / (length + 1)
    out = [None] * (length - 1)
    e = sum(values[:length]) / length
    out.append(e)
    for v in values[length:]:
        e = v * k + e * (1 - k)
        out.append(e)
    return out

def rsi_series(values, length=14):
    n = len(values)
    if n < length + 1: return [None] * n
    out = [None] * length
    gains, losses = [0.0], [0.0]
    for i in range(1, n):
        d = values[i] - values[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[1:length+1]) / length
    avg_l = sum(losses[1:length+1]) / length
    rs = avg_g / avg_l if avg_l else float("inf")
    out.append(100 - 100/(1+rs))
    for i in range(length+1, n):
        avg_g = (avg_g * (length-1) + gains[i]) / length
        avg_l = (avg_l * (length-1) + losses[i]) / length
        rs = avg_g / avg_l if avg_l else float("inf")
        out.append(100 - 100/(1+rs))
    return out

def fvb_state(closes, length=20):
    if len(closes) < length + 2: return None
    basis_today = sma(closes, length)
    basis_yest  = sma(closes[:-1], length)
    return {
        "today_bull": closes[-1] > basis_today,
        "yest_bull":  closes[-2] > basis_yest,
        "basis": basis_today,
        "price": closes[-1],
    }

def bxt_state(closes):
    if len(closes) < 80: return None
    e5  = ema_series(closes, 5)
    e20 = ema_series(closes, 20)
    diff = []
    for i in range(len(closes)):
        if e5[i] is not None and e20[i] is not None:
            diff.append(e5[i] - e20[i])
        else:
            diff.append(None)
    diff_clean = [d for d in diff if d is not None]
    if len(diff_clean) < 16: return None
    rsi_vals = rsi_series(diff_clean, 15)
    short_term = [r - 50 if r is not None else None for r in rsi_vals]
    short_clean = [s for s in short_term if s is not None]
    if len(short_clean) < 2: return None
    return {"today": short_clean[-1], "yest": short_clean[-2]}

# ─── Yahoo fetch ───────────────────────────────────────────────────────────
def fetch(sym):
    sym_q = sym.replace(".", "-")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym_q}?range=1y&interval=1d"
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        result = data["chart"]["result"][0]
        closes = [c for c in result["indicators"]["quote"][0]["close"] if c is not None]
        return sym, closes
    except Exception:
        return sym, None

def fetch_ath(sym):
    sym_q = sym.replace(".","-")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym_q}?range=max&interval=1wk"
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        result = data["chart"]["result"][0]
        high = [c for c in result["indicators"]["quote"][0]["high"] if c is not None]
        return sym, max(high) if high else None
    except Exception:
        return sym, None

# ─── Hardcoded names + market caps ─────────────────────────────────────────
NAMES = {
    "BRK.B":"Berkshire Hathaway B","KO":"Coca-Cola","PEP":"PepsiCo","WMB":"Williams","EQT":"EQT Corp","D":"Dominion Energy",
    "AXP":"American Express","AMAT":"Applied Materials","NEM":"Newmont","COF":"Capital One","EMR":"Emerson Electric",
    "IR":"Ingersoll Rand","WSM":"Williams-Sonoma","CPAY":"Corpay","ALGN":"Align Technology","PCAR":"PACCAR","PNR":"Pentair",
    "DD":"DuPont","DPZ":"Domino's Pizza","BKNG":"Booking Holdings","CCL":"Carnival","LUV":"Southwest Airlines",
    "O":"Realty Income","CCI":"Crown Castle","YUM":"Yum! Brands","DLR":"Digital Realty","GM":"General Motors",
    "IRM":"Iron Mountain","LRCX":"Lam Research","BAC":"Bank of America","MA":"Mastercard","JPM":"JPMorgan",
    "INCY":"Incyte","STE":"STERIS","ALLE":"Allegion","APTV":"Aptiv","SHW":"Sherwin-Williams","DECK":"Deckers",
    "TGT":"Target","HD":"Home Depot","COST":"Costco","WMT":"Walmart","CMG":"Chipotle","ULTA":"Ulta Beauty",
    "GPC":"Genuine Parts","FFIV":"F5 Inc.","PSKY":"Paramount Skydance","BF.B":"Brown-Forman","WM":"Waste Management",
    "AMT":"American Tower","CL":"Colgate-Palmolive","ADBE":"Adobe","CRM":"Salesforce","INTU":"Intuit","BX":"Blackstone",
    "MDLZ":"Mondelez","ZBH":"Zimmer Biomet","ZTS":"Zoetis","HRL":"Hormel Foods","IDXX":"IDEXX Labs","MTB":"M&T Bank",
    "PNW":"Pinnacle West","CB":"Chubb","WTW":"Willis Towers Watson","CPRT":"Copart","EBAY":"eBay","BG":"Bunge",
    "VZ":"Verizon","FDS":"FactSet","DTE":"DTE Energy","AEE":"Ameren","ADM":"ADM","FSLR":"First Solar","PPL":"PPL Corp",
    "TRGP":"Targa Resources","UDR":"UDR Inc.","HIG":"Hartford","NKE":"Nike","GEN":"Gen Digital",
    "MPC":"Marathon Petroleum","PSX":"Phillips 66","VLO":"Valero Energy","WRB":"W.R. Berkley","DVA":"DaVita",
    "ACGL":"Arch Capital","COP":"ConocoPhillips","OXY":"Occidental","ADSK":"Autodesk","ROP":"Roper Technologies",
    "PAYX":"Paychex","ANET":"Arista","AVGO":"Broadcom","ETN":"Eaton","CIEN":"Ciena","COHR":"Coherent",
    "SYY":"Sysco","RL":"Ralph Lauren","EXPE":"Expedia","PPG":"PPG","GLW":"Corning","GRMN":"Garmin",
    "ARES":"Ares","ARE":"Alexandria RE","BIIB":"Biogen","BAX":"Baxter","NRG":"NRG","LITE":"Lumentum","CTAS":"Cintas",
    "POOL":"Pool Corp","DRI":"Darden","HSIC":"Henry Schein","LNT":"Alliant Energy","TPL":"Texas Pacific Land",
    "TRMB":"Trimble","SNA":"Snap-on","VRSK":"Verisk","PLTR":"Palantir","MLM":"Martin Marietta","ZBRA":"Zebra",
    "WELL":"Welltower","HLT":"Hilton","MAR":"Marriott","C":"Citi","ECL":"Ecolab","KEYS":"Keysight",
    "GWW":"Grainger","RMD":"ResMed","SBAC":"SBA Comms","UHS":"Universal Health","EQIX":"Equinix",
    "DLTR":"Dollar Tree","BRO":"Brown & Brown","ALB":"Albemarle","A":"Agilent","OTIS":"Otis","PNC":"PNC",
}
MCAPS = {
    "BRK.B":1100,"KO":330,"PEP":210,"WMB":70,"EQT":40,"D":50,
    "AXP":230,"AMAT":150,"NEM":120,"COF":110,"EMR":80,"IR":35,"WSM":24,"CPAY":22,"ALGN":17,"PCAR":50,"PNR":15,
    "DD":35,"DPZ":15,"BKNG":190,"CCL":30,"LUV":22,"O":50,"CCI":45,"YUM":45,"DLR":60,"GM":85,"IRM":34,"LRCX":92,
    "INCY":15,"STE":22,"ALLE":11,"APTV":15,"SHW":75,"DECK":17,"TGT":62,"HD":330,"COST":450,"WMT":700,
    "CMG":80,"ULTA":25,"GPC":20,"FFIV":15,"PSKY":15,"BF.B":13,"WM":80,"AMT":95,"CL":80,"ADBE":200,"CRM":280,
    "INTU":175,"BX":150,"MDLZ":95,"ZBH":25,"ZTS":80,"HRL":15,"IDXX":50,"MTB":25,"PNW":10,"CB":110,"WTW":35,
    "CPRT":60,"EBAY":30,"BG":13,"MA":520,"VZ":190,"FDS":18,"DTE":30,"AEE":25,"ADM":25,"FSLR":25,"PPL":25,
    "TRGP":40,"UDR":13,"HIG":42,"NKE":67,"GEN":17,"MPC":50,"PSX":50,"VLO":50,"WRB":18,"DVA":15,
    "ACGL":52,"COP":135,"OXY":42,"ADSK":60,"ROP":62,"PAYX":55,"ANET":150,"AVGO":1300,"ETN":110,"CIEN":40,
    "COHR":48,"SYY":35,"RL":25,"EXPE":24,"PPG":24,"GLW":35,"GRMN":56,"ARES":58,"ARE":7,"BIIB":21,"BAX":15,
    "NRG":25,"LITE":11,"CTAS":85,"POOL":11,"DRI":24,"HSIC":9,"LNT":15,"TPL":33,"TRMB":15,"SNA":15,"VRSK":40,
    "PLTR":270,"MLM":40,"ZBRA":13,"WELL":75,"HLT":80,"MAR":98,"C":140,"ECL":75,"KEYS":58,"GWW":51,"RMD":33,
    "SBAC":23,"UHS":12,"EQIX":80,"DLTR":18,"BRO":18,"ALB":21,"A":33,"OTIS":33,"PNC":52,
}

# ─── Run ───────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    closes_map = {}
    with ThreadPoolExecutor(max_workers=25) as ex:
        for f in as_completed([ex.submit(fetch, s) for s in TICKERS]):
            sym, closes = f.result()
            if closes: closes_map[sym] = closes

    rows = []
    for sym, closes in closes_map.items():
        fvb = fvb_state(closes)
        bxt = bxt_state(closes)
        if not fvb or not bxt: continue
        fvb_g = fvb["today_bull"] and not fvb["yest_bull"]
        fvb_r = (not fvb["today_bull"]) and fvb["yest_bull"]
        bxt_g = bxt["today"] > 0 and bxt["yest"] <= 0
        bxt_r = bxt["today"] < 0 and bxt["yest"] >= 0
        if not (fvb_g or fvb_r or bxt_g or bxt_r): continue
        rows.append({
            "sym": sym, "name": NAMES.get(sym, sym), "mcap": live_mcap(sym, fvb["price"]),
            "price": round(fvb["price"], 2), "basis": round(fvb["basis"], 2),
            "bxt_today": round(bxt["today"], 2), "bxt_yest": round(bxt["yest"], 2),
            "fvb_g": fvb_g, "fvb_r": fvb_r, "bxt_g": bxt_g, "bxt_r": bxt_r,
        })

    # Fetch ATH only for flipped tickers
    ath_map = {}
    flipped = [r["sym"] for r in rows if (r["fvb_g"] and r["bxt_g"]) or (r["fvb_r"] and r["bxt_r"])]
    with ThreadPoolExecutor(max_workers=15) as ex:
        for f in as_completed([ex.submit(fetch_ath, s) for s in flipped]):
            sym, ath = f.result()
            ath_map[sym] = ath
    for r in rows:
        ath = ath_map.get(r["sym"])
        r["ath"] = round(ath, 2) if ath else None
        r["pct_to_ath"] = round((ath - r["price"]) / r["price"] * 100, 1) if ath else None

    both_g = sorted([r for r in rows if r["fvb_g"] and r["bxt_g"]], key=lambda x: -x["mcap"])
    both_r = sorted([r for r in rows if r["fvb_r"] and r["bxt_r"]], key=lambda x: -x["mcap"])

    # Diff vs previous run
    out_path = os.path.join(HERE, "docs", "results.json")
    prev_g_set, prev_r_set, prev_ts = set(), set(), None
    try:
        with open(out_path) as f:
            prev = json.load(f)
        prev_g_set = {r["sym"] for r in prev.get("both_green", [])}
        prev_r_set = {r["sym"] for r in prev.get("both_red", [])}
        prev_ts = prev.get("updated_at")
    except Exception:
        pass

    cur_g_set = {r["sym"] for r in both_g}
    cur_r_set = {r["sym"] for r in both_r}
    cur_lookup = {r["sym"]: r for r in both_g + both_r}
    prev_lookup = {}
    if prev_g_set or prev_r_set:
        try:
            for pr in prev.get("both_green", []) + prev.get("both_red", []):
                prev_lookup[pr["sym"]] = pr
        except Exception: pass

    def enrich(sym, action):
        # Use current data for ADDED, previous data for REMOVED
        src = cur_lookup.get(sym) if action == "added" else prev_lookup.get(sym, cur_lookup.get(sym))
        if not src:
            return {"sym": sym, "name": NAMES.get(sym, sym), "mcap": MCAPS.get(sym, 0)}
        return {
            "sym": src["sym"], "name": src.get("name", sym),
            "mcap": src.get("mcap", MCAPS.get(sym, 0)),
            "price": src.get("price"), "basis": src.get("basis"),
            "bxt_today": src.get("bxt_today"),
            "ath": src.get("ath"), "pct_to_ath": src.get("pct_to_ath"),
        }

    def annotate(syms, action):
        return [enrich(s, action) for s in sorted(syms, key=lambda x: -MCAPS.get(x, 0))]

    changes = {
        "green_added":   annotate(cur_g_set - prev_g_set, "added"),
        "green_removed": annotate(prev_g_set - cur_g_set, "removed"),
        "red_added":     annotate(cur_r_set - prev_r_set, "added"),
        "red_removed":   annotate(prev_r_set - cur_r_set, "removed"),
        "compared_to":   prev_ts,
    }

    out = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "scan_seconds": round(time.time() - t0, 2),
        "scanned_count": len(closes_map),
        "both_green": both_g,
        "both_red": both_r,
        "changes": changes,
    }
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    n_changes = sum(len(v) for k, v in changes.items() if isinstance(v, list))
    print(f"[{out['updated_at']}] scanned {out['scanned_count']} in {out['scan_seconds']}s — {len(both_g)} both-green, {len(both_r)} both-red ({n_changes} changes vs prev)")

if __name__ == "__main__":
    main()
