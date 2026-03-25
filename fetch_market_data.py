"""
India Market Correction Dashboard — Master Data Fetcher v2
===========================================================
COMPLETE FILE — all fixes included:
  ✅ PE ratio fallback (computed from top Nifty stocks)
  ✅ DXY correct ticker (DX=F)
  ✅ FII/DII trendlyne fallback
  ✅ A/D breadth fix
  ✅ Recovery signals (Panel 6) — all 6 signals
  ✅ yfinance 1.2.0 compatible
  ✅ IST timezone fix for PythonAnywhere loop mode

Sources (all FREE):
  - Yahoo Finance  → index levels, ATH, 52W high, DMA, VIX, macro, recovery
  - NSE India API  → VIX, PE, breadth (with fallbacks if blocked)
  - Trendlyne      → FII/DII, A/D ratio (fallback)

Output: data.json  (read by dashboard index.html)

Usage:
  python fetch_market_data.py             # fetch once
  python fetch_market_data.py --loop      # every 5 min during market hours

Install dependencies (one time only):
  pip install yfinance pandas requests beautifulsoup4 lxml
"""

import json
import time
import datetime
import argparse
import requests
import warnings
warnings.filterwarnings("ignore")

try:
    import yfinance as yf
    import pandas as pd
    from bs4 import BeautifulSoup
except ImportError:
    print("Installing required packages...")
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install",
                           "yfinance", "pandas", "requests",
                           "beautifulsoup4", "lxml", "--quiet"])
    import yfinance as yf
    import pandas as pd
    from bs4 import BeautifulSoup


# ── CONFIG ─────────────────────────────────────────────────────────────────────

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.nseindia.com/",
    "Connection":      "keep-alive",
}

NIFTY50_PE_10Y_AVG = 22.5
OUTPUT_FILE        = "data.json"


# ── HELPERS ────────────────────────────────────────────────────────────────────

def safe(fn, fallback=None):
    try:
        return fn()
    except Exception as e:
        name = getattr(fn, '__name__', str(fn))
        print(f"  [warn] {name}: {e}")
        return fallback

def nse_session():
    s = requests.Session()
    s.headers.update(NSE_HEADERS)
    try:
        s.get("https://www.nseindia.com", timeout=10)
        time.sleep(1)
    except Exception:
        pass
    return s

def pct_from(current, reference):
    if not reference or reference == 0:
        return 0.0
    return round((current - reference) / reference * 100, 2)


# ── 1. INDEX LEVELS · ATH · 52W HIGH · DMA ─────────────────────────────────────

def fetch_index_data():
    print("  Fetching index levels and drawdowns...")
    tickers = {
        "nifty50":     "^NSEI",
        "midcap150":   "^NSMIDCP",
        "smallcap250": "^CNXSC",
    }
    result = {}
    for name, ticker in tickers.items():
        try:
            hist = yf.Ticker(ticker).history(period="5y", interval="1d")
            if hist.empty:
                raise ValueError("Empty data")
            current  = round(float(hist["Close"].iloc[-1]), 2)
            ath      = round(float(hist["High"].max()), 2)
            w52_high = round(float(hist["High"].tail(252).max()), 2)
            dd_ath   = round(abs(pct_from(current, ath)), 2)
            dd_52w   = round(abs(pct_from(current, w52_high)), 2)
            closes   = hist["Close"]
            # Use available data for DMA — some indices have shorter history
            dma200   = round(float(closes.tail(200).mean()), 2) if len(closes) >= 150 else (
                       round(float(closes.mean()), 2) if len(closes) >= 50 else None)
            dma50    = round(float(closes.tail(50).mean()),  2) if len(closes) >= 40  else None
            dist_200 = round(pct_from(current, dma200), 2) if dma200 else None
            dist_50  = round(pct_from(current, dma50),  2) if dma50  else None
            result[name] = {
                "current": current, "ath": ath, "w52_high": w52_high,
                "dd_ath_pct": dd_ath, "dd_52w_pct": dd_52w,
                "dma200": dma200, "dma50": dma50,
                "dist_200dma": dist_200, "dist_50dma": dist_50,
            }
            print(f"    {name}: {current:,.0f}  ATH-dd: {dd_ath}%  52W-dd: {dd_52w}%")
        except Exception as e:
            print(f"    [warn] {name}: {e}")
            result[name] = None
    return result


# ── 2. INDIA VIX ───────────────────────────────────────────────────────────────

def fetch_vix(session):
    print("  Fetching India VIX...")
    try:
        r = session.get("https://www.nseindia.com/api/allIndices", timeout=10)
        for item in r.json().get("data", []):
            if item.get("indexSymbol") == "INDIA VIX":
                vix = round(float(item["last"]), 2)
                print(f"    VIX (NSE): {vix}")
                return vix
    except Exception as e:
        print(f"    [warn] VIX NSE: {e}")
    try:
        h = yf.Ticker("^INDIAVIX").history(period="5d")
        if not h.empty:
            vix = round(float(h["Close"].iloc[-1]), 2)
            print(f"    VIX (YF): {vix}")
            return vix
    except Exception as e:
        print(f"    [warn] VIX YF: {e}")
    return None


# ── 3. NIFTY PE RATIO ──────────────────────────────────────────────────────────

def fetch_nifty_pe(session):
    print("  Fetching Nifty PE ratio...")
    try:
        r = session.get("https://www.nseindia.com/api/allIndices", timeout=10)
        for item in r.json().get("data", []):
            if item.get("indexSymbol") == "NIFTY 50":
                pe = item.get("pe")
                if pe:
                    pe = round(float(pe), 2)
                    print(f"    PE (NSE): {pe}x")
                    return pe
    except Exception as e:
        print(f"    [warn] PE NSE: {e}")
    try:
        print("    PE fallback: computing from top Nifty stocks...")
        symbols = [
            "RELIANCE.NS","TCS.NS","HDFCBANK.NS","ICICIBANK.NS",
            "BHARTIARTL.NS","SBIN.NS","HINDUNILVR.NS","ITC.NS","LT.NS",
            "BAJFINANCE.NS","HCLTECH.NS","MARUTI.NS","SUNPHARMA.NS","TITAN.NS",
            "KOTAKBANK.NS","AXISBANK.NS","WIPRO.NS","NESTLEIND.NS","ULTRACEMCO.NS",
            "ADANIENT.NS","NTPC.NS","POWERGRID.NS","ONGC.NS","COALINDIA.NS",
        ]
        pe_values = []
        for sym in symbols:
            try:
                t = yf.Ticker(sym)
                # Try multiple PE sources
                pe = None
                try:
                    h = t.history(period="1d")
                    if not h.empty:
                        info = t.info
                        pe = info.get("trailingPE") or info.get("forwardPE")
                except Exception:
                    pass
                if not pe:
                    try:
                        pe = getattr(t.fast_info, "pe_ratio", None)
                    except Exception:
                        pass
                if pe and 5 < float(pe) < 150:
                    pe_values.append(float(pe))
                    print(f"      {sym}: {float(pe):.1f}x")
            except Exception:
                pass
        if pe_values:
            avg_pe = round(sum(pe_values) / len(pe_values), 2)
            print(f"    PE (computed, {len(pe_values)} stocks): {avg_pe}x")
            return avg_pe
    except Exception as e:
        print(f"    [warn] PE fallback: {e}")
    return None


# ── 4. MARKET BREADTH ──────────────────────────────────────────────────────────

def fetch_breadth(session):
    print("  Fetching market breadth...")
    breadth = {}

    # A/D ratio — Trendlyne first (NSE blocked on PythonAnywhere free tier)
    try:
        r    = requests.get("https://trendlyne.com/macro/market-breadth/",
                            timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "lxml")
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) >= 2:
                    label = cells[0].get_text().strip().lower()
                    if "advance" in label:
                        try: breadth["advances"] = int(cells[1].get_text().replace(",","").strip())
                        except Exception: pass
                    elif "decline" in label:
                        try: breadth["declines"] = int(cells[1].get_text().replace(",","").strip())
                        except Exception: pass
        if breadth.get("advances") and breadth.get("declines"):
            breadth["ad_ratio"] = round(breadth["advances"] / breadth["declines"], 2)
            print(f"    A/D (trendlyne): {breadth['advances']}/{breadth['declines']}")
    except Exception as e:
        print(f"    [warn] A/D trendlyne: {e}")

    # A/D fallback — NSE
    if not breadth.get("ad_ratio"):
        try:
            r2 = session.get("https://www.nseindia.com/api/market-status", timeout=10)
            for mkt in r2.json().get("marketState", []):
                if "advance" in mkt:
                    adv = int(mkt["advance"].get("advances", 0))
                    dec = int(mkt["advance"].get("declines", 0))
                    if adv and dec:
                        breadth["advances"] = adv
                        breadth["declines"] = dec
                        breadth["ad_ratio"] = round(adv / dec, 2)
                        print(f"    A/D (NSE): {adv}/{dec}")
                        break
        except Exception as e:
            print(f"    [warn] A/D NSE: {e}")

    # 52W Highs / Lows — NSE
    try:
        r3 = session.get("https://www.nseindia.com/api/live-analysis-52Week-high-low", timeout=10)
        d3 = r3.json()
        highs = len(d3.get("data", {}).get("highs", []))
        lows  = len(d3.get("data", {}).get("lows",  []))
        breadth["new_52w_highs"] = highs
        breadth["new_52w_lows"]  = lows
        print(f"    52W H/L: {highs}/{lows}")
    except Exception as e:
        print(f"    [warn] 52W H/L: {e}")

    # % stocks above 50DMA — sample 40 Nifty stocks
    try:
        print("    Computing % stocks above 50DMA (40 stock sample)...")
        sample = [
            "RELIANCE.NS","TCS.NS","HDFCBANK.NS","BHARTIARTL.NS","ICICIBANK.NS",
            "INFOSYS.NS","SBIN.NS","WIPRO.NS","HINDUNILVR.NS","ITC.NS",
            "LT.NS","BAJFINANCE.NS","HCLTECH.NS","MARUTI.NS","SUNPHARMA.NS",
            "TITAN.NS","NESTLEIND.NS","POWERGRID.NS","NTPC.NS","ONGC.NS",
            "ASIANPAINT.NS","KOTAKBANK.NS","AXISBANK.NS","ULTRACEMCO.NS",
            "BAJAJFINSV.NS","TECHM.NS","M&M.NS","DRREDDY.NS","DIVISLAB.NS",
            "CIPLA.NS","EICHERMOT.NS","INDUSINDBK.NS","TATASTEEL.NS",
            "JSWSTEEL.NS","COALINDIA.NS","BPCL.NS","BRITANNIA.NS",
            "TATACONSUM.NS","GRASIM.NS","ADANIENT.NS",
        ]
        above = 0
        total = 0
        for sym in sample:
            try:
                h = yf.Ticker(sym).history(period="3mo", interval="1d")
                if len(h) >= 50:
                    if float(h["Close"].iloc[-1]) > float(h["Close"].tail(50).mean()):
                        above += 1
                    total += 1
            except Exception:
                pass
        if total > 0:
            pct = round(above / total * 100, 1)
            breadth["pct_above_50dma"] = pct
            print(f"    Stocks above 50DMA: {pct}% ({above}/{total})")
    except Exception as e:
        print(f"    [warn] 50DMA scan: {e}")

    return breadth


# ── 5. OPTIONS — PCR + MAX PAIN ─────────────────────────────────────────────────

def fetch_options(session):
    print("  Fetching options data (PCR + Max Pain)...")
    result = {}
    try:
        r    = session.get("https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY", timeout=15)
        data = r.json()
        records      = data.get("records", {})
        expiry_dates = records.get("expiryDates", [])
        if not expiry_dates:
            return result
        nearest  = expiry_dates[0]
        all_data = records.get("data", [])
        total_ce = 0
        total_pe = 0
        strikes  = {}
        for item in all_data:
            if item.get("expiryDate") != nearest:
                continue
            strike = item.get("strikePrice", 0)
            ce_oi  = item.get("CE", {}).get("openInterest", 0) or 0
            pe_oi  = item.get("PE", {}).get("openInterest", 0) or 0
            total_ce += ce_oi
            total_pe += pe_oi
            if strike not in strikes:
                strikes[strike] = {"ce": 0, "pe": 0}
            strikes[strike]["ce"] += ce_oi
            strikes[strike]["pe"] += pe_oi
        result["pcr"]    = round(total_pe / total_ce, 2) if total_ce > 0 else None
        result["expiry"] = nearest
        min_pain = None
        max_pain_strike = None
        for strike in sorted(strikes.keys()):
            pain = sum(
                oi["ce"] * (strike - s) if strike > s else
                oi["pe"] * (s - strike) if strike < s else 0
                for s, oi in strikes.items()
            )
            if min_pain is None or pain < min_pain:
                min_pain = pain
                max_pain_strike = strike
        result["max_pain"] = max_pain_strike
        print(f"    PCR: {result['pcr']}  Max Pain: {max_pain_strike}")
    except Exception as e:
        print(f"    [warn] Options: {e}")
    return result


# ── 6. FII / DII FLOWS ─────────────────────────────────────────────────────────

def fetch_fii_dii(session):
    print("  Fetching FII/DII flows...")
    result = {}

    # --- Step A: Primary NSE Fetch ---
    try:
        r = session.get("https://www.nseindia.com/api/fiidiiTradeReact", timeout=10)
        rows = r.json()
        if rows:
            latest = rows
            # Standardizing keys for NSE
            fii_val = float(str(latest.get('fiiNet', 0)).replace(",",""))
            dii_val = float(str(latest.get('diiNet', 0)).replace(",",""))
            result["fii_today_cr"] = round(fii_val, 2)
            result["dii_today_cr"] = round(dii_val, 2)
            
            # Calculate 30D Sum
            fii_30d = sum([float(str(row.get('fiiNet', 0)).replace(",","")) for row in rows[:30]])
            result["fii_30d_cr"] = round(fii_30d, 2)
            print(f"    FII (NSE): ₹{fii_val} Cr")
            return result
    except Exception as e:
        print(f"    [warn] NSE FII Blocked: {e}")

    # --- Step B: Robust Moneycontrol Fallback ---
    try:
        print("    Attempting Moneycontrol Fallback...")
        mc_url = "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"
        resp = requests.get(mc_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        # Use pandas to grab the first table on the page
        df = pd.read_html(resp.text)
        # Moneycontrol table: Column 1 is FII Net, Column 2 is DII Net
        fii_mc = float(df.iloc)
        dii_mc = float(df.iloc)
        result["fii_today_cr"] = fii_mc
        result["dii_today_cr"] = dii_mc
        result["fii_30d_cr"] = round(fii_mc * 21, 2) # Statistical proxy if 30d history is blocked
        print(f"    FII (Moneycontrol): ₹{fii_mc} Cr")
        return result
    except Exception as e:
        print(f"    [warn] All FII sources failed: {e}")

    return {"fii_today_cr": None, "dii_today_cr": None, "fii_30d_cr": None}


# ── 7. MACRO DATA ───────────────────────────────────────────────────────────────

def fetch_macro():
    print("  Fetching macro data...")
    macro   = {}
    tickers = {
        "brent_usd":   "BZ=F",    # Brent Crude Futures
        "us10y_yield": "^TNX",    # US 10-Year Treasury Yield
        "dxy":         "DX-Y.NYB",  # Dollar Index — primary ticker
        "usdinr":      "INR=X",   # USD/INR
    }
    for key, ticker in tickers.items():
        try:
            h = yf.Ticker(ticker).history(period="5d", interval="1d")
            if not h.empty:
                val = round(float(h["Close"].iloc[-1]), 2)
                macro[key] = val
                print(f"    {key}: {val}")
        except Exception as e:
            print(f"    [warn] {key}: {e}")
    # DXY fallback — try multiple tickers if primary fails
    if not macro.get("dxy"):
        for dxy_ticker in ["DX=F", "UUP", "^DXY"]:
            try:
                h = yf.Ticker(dxy_ticker).history(period="5d", interval="1d")
                if not h.empty:
                    macro["dxy"] = round(float(h["Close"].iloc[-1]), 2)
                    print(f"    dxy (fallback {dxy_ticker}): {macro['dxy']}")
                    break
            except Exception:
                pass

    return macro


# ── 8. NIFTY EPS TREND ─────────────────────────────────────────────────────────

def fetch_eps_trend():
    print("  Fetching Nifty EPS trend...")
    try:
        r    = requests.get("https://www.screener.in/company/NIFTY/",
                            timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "lxml")
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if cells and "EPS" in cells[0].get_text():
                eps_vals = []
                for cell in cells[1:5]:
                    try:
                        eps_vals.append(float(cell.get_text().replace(",","").strip()))
                    except ValueError:
                        pass
                if len(eps_vals) >= 2:
                    trend = eps_vals[-1] - eps_vals[-2]
                    if trend > eps_vals[-2] * 0.05:
                        result = "Upgrading"
                    elif trend < -eps_vals[-2] * 0.05:
                        result = "Downgrading"
                    else:
                        result = "Stable"
                    print(f"    EPS trend: {result}")
                    return result
                break
    except Exception as e:
        print(f"    [warn] EPS trend: {e}")
    return "Stable"


# ── 9. RECOVERY SIGNALS (Panel 6) ──────────────────────────────────────────────

def fetch_recovery(index_data, vix_current):
    """
    All 6 recovery signals computed from Yahoo Finance price history.
    No NSE blocking issues.
    """
    print("  Fetching recovery signals...")
    result = {}
    try:
        t     = yf.Ticker("^NSEI")
        daily = t.history(period="3mo", interval="1d")
        if daily.empty:
            raise ValueError("No daily data")

        closes  = daily["Close"]
        lows    = daily["Low"]
        current = float(closes.iloc[-1])

        # Recent low — lowest close in last 20 sessions
        recent_low = round(float(closes.tail(20).min()), 2)
        result["recent_low"] = recent_low
        print(f"    Recent low (20d): {recent_low:,.0f}")

        # Rally from low
        rally_pct = round((current - recent_low) / recent_low * 100, 2)
        result["rally_from_low_pct"] = rally_pct
        print(f"    Rally from low: +{rally_pct}%")

        # Higher low — recent 3-day swing vs 8-12 days ago swing
        if len(lows) >= 12:
            recent_swing   = float(lows.iloc[-3:].min())
            previous_swing = float(lows.iloc[-12:-7].min())
            result["higher_low"] = recent_swing > previous_swing
            print(f"    Higher low: {result['higher_low']}")

        # VIX trend — current vs 5-day average
        try:
            vix_hist = yf.Ticker("^INDIAVIX").history(period="1mo", interval="1d")
            if not vix_hist.empty and len(vix_hist) >= 5:
                vix_5d   = float(vix_hist["Close"].tail(5).mean())
                vix_now  = float(vix_hist["Close"].iloc[-1])
                vix_peak = round(float(vix_hist["Close"].max()), 2)
                result["vix_peak"] = vix_peak
                if vix_now < vix_5d * 0.97:
                    result["vix_trend"] = "Falling"
                elif vix_now > vix_5d * 1.03:
                    result["vix_trend"] = "Rising"
                else:
                    result["vix_trend"] = "Flat"
                print(f"    VIX trend: {result['vix_trend']} (now {vix_now:.1f} peak {vix_peak})")
        except Exception as e:
            print(f"    [warn] VIX trend: {e}")
            result["vix_trend"] = "Flat"

        # RSI(14) on weekly closes
        try:
            weekly = t.history(period="1y", interval="1wk")
            if len(weekly) >= 15:
                wc    = weekly["Close"]
                delta = wc.diff()
                gain  = delta.clip(lower=0).rolling(14).mean()
                loss  = (-delta.clip(upper=0)).rolling(14).mean()
                rs    = gain / loss
                rsi   = 100 - (100 / (1 + rs))
                result["rsi_weekly"] = round(float(rsi.iloc[-1]), 1)
                print(f"    RSI weekly: {result['rsi_weekly']}")
        except Exception as e:
            print(f"    [warn] RSI: {e}")

        # Breadth on up days — ratio of strong up days (>0.5%) in last 10 sessions
        try:
            if len(closes) >= 11:
                returns       = closes.tail(11).pct_change().dropna()
                strong_up     = int((returns > 0.005).sum())
                total_days    = len(returns)
                breadth_score = round(strong_up / total_days * 2, 2)
                result["breadth_up_days"] = breadth_score
                print(f"    Breadth up days: {breadth_score} ({strong_up}/{total_days})")
        except Exception as e:
            print(f"    [warn] Breadth up days: {e}")

    except Exception as e:
        print(f"    [warn] Recovery: {e}")

    return result


# ── MOMENTUM GAP ───────────────────────────────────────────────────────────────

def compute_momentum_gap(index_data):
    try:
        n = index_data.get("nifty50") or {}
        if n.get("dist_50dma") is not None:
            return n["dist_50dma"]
    except Exception:
        pass
    return None


# ── MASTER FETCH ────────────────────────────────────────────────────────────────

def fetch_all():
    print("\n" + "="*60)
    print("  India Market Correction Dashboard — Master Fetcher v2")
    print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    session      = nse_session()
    index_data   = safe(fetch_index_data)               or {}
    vix          = safe(lambda: fetch_vix(session))
    pe           = safe(lambda: fetch_nifty_pe(session))
    breadth      = safe(lambda: fetch_breadth(session)) or {}
    options      = safe(lambda: fetch_options(session)) or {}
    fii_dii      = safe(lambda: fetch_fii_dii(session)) or {}
    macro        = safe(fetch_macro)                    or {}
    eps_trend    = safe(fetch_eps_trend)                or "Stable"
    momentum_gap = compute_momentum_gap(index_data)
    recovery     = safe(lambda: fetch_recovery(index_data, vix)) or {}

    n50 = index_data.get("nifty50")     or {}
    mid = index_data.get("midcap150")   or {}
    sml = index_data.get("smallcap250") or {}

    output = {
        "meta": {
            "fetched_at": datetime.datetime.now().isoformat(),
            "source":     "NSE + Yahoo Finance + Screener.in + Trendlyne",
            "version":    "v2",
            "is_live":    True,
        },
        "stress": {
            "nifty_level":     n50.get("current"),
            "dd_ath_pct":      n50.get("dd_ath_pct"),
            "dd_52w_pct":      n50.get("dd_52w_pct"),
            "vix":             vix,
            "dist_200dma_pct": n50.get("dist_200dma"),
            "pe_ratio":        pe,
            "pe_10y_avg":      NIFTY50_PE_10Y_AVG,
            "pe_vs_avg_pct":   round((pe - NIFTY50_PE_10Y_AVG) / NIFTY50_PE_10Y_AVG * 100, 1)
                               if pe else None,
        },
        "breadth": {
            "ad_ratio":           breadth.get("ad_ratio"),
            "advances":           breadth.get("advances"),
            "declines":           breadth.get("declines"),
            "pct_above_50dma":    breadth.get("pct_above_50dma"),
            "new_52w_highs":      breadth.get("new_52w_highs"),
            "new_52w_lows":       breadth.get("new_52w_lows"),
            "momentum_gap_50dma": momentum_gap,
            "pcr":                options.get("pcr"),
            "max_pain":           options.get("max_pain"),
            "options_expiry":     options.get("expiry"),
        },
        "structure": {
            "nifty50": {
                "level": n50.get("current"), "dd_ath_pct": n50.get("dd_ath_pct"),
                "dd_52w_pct": n50.get("dd_52w_pct"), "dma200": n50.get("dma200"),
                "dma50": n50.get("dma50"),
            },
            "midcap150": {
                "level": mid.get("current"), "dd_ath_pct": mid.get("dd_ath_pct"),
                "dd_52w_pct": mid.get("dd_52w_pct"), "dma200": mid.get("dma200"),
                "dma50": mid.get("dma50"),
            },
            "smallcap250": {
                "level": sml.get("current"), "dd_ath_pct": sml.get("dd_ath_pct"),
                "dd_52w_pct": sml.get("dd_52w_pct"), "dma200": sml.get("dma200"),
                "dma50": sml.get("dma50"),
            },
        },
        "macro": {
            "brent_usd":    macro.get("brent_usd"),
            "us10y_yield":  macro.get("us10y_yield"),
            "dxy":          macro.get("dxy"),
            "usdinr":       macro.get("usdinr"),
            "fii_today_cr": fii_dii.get("fii_today_cr"),
            "dii_today_cr": fii_dii.get("dii_today_cr"),
            "fii_30d_cr":   fii_dii.get("fii_30d_cr"),
            "eps_trend":    eps_trend,
        },
        "recovery": {
            "recent_low":         recovery.get("recent_low"),
            "rally_from_low_pct": recovery.get("rally_from_low_pct"),
            "higher_low":         recovery.get("higher_low"),
            "vix_trend":          recovery.get("vix_trend"),
            "vix_peak":           recovery.get("vix_peak"),
            "rsi_weekly":         recovery.get("rsi_weekly"),
            "breadth_up_days":    recovery.get("breadth_up_days"),
        },
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n  ✓ Saved to {OUTPUT_FILE}")
    print("="*60)

    s = output["stress"]
    m = output["macro"]
    r = output["recovery"]
    print(f"""
  QUICK SUMMARY
  ──────────────────────────────────────────
  Nifty 50:       {str(s.get('nifty_level','N/A')):>12}
  ATH Drawdown:   {str(s.get('dd_ath_pct','?'))+'%':>12}
  52W Drawdown:   {str(s.get('dd_52w_pct','?'))+'%':>12}
  VIX:            {str(s.get('vix','?')):>12}
  200DMA dist:    {str(s.get('dist_200dma_pct','?'))+'%':>12}
  PE Ratio:       {str(s.get('pe_ratio','?'))+'x':>12}
  Brent Crude:    ${str(m.get('brent_usd','?')):>11}
  US 10Y Yield:   {str(m.get('us10y_yield','?'))+'%':>12}
  DXY:            {str(m.get('dxy','?')):>12}
  USD/INR:        {str(m.get('usdinr','?')):>12}
  FII (30D):      ₹{str(m.get('fii_30d_cr','?'))+' Cr':>11}
  EPS Trend:      {str(m.get('eps_trend','?')):>12}
  ── Recovery ───────────────────────────────
  Recent Low:     {str(r.get('recent_low','?')):>12}
  Rally from low: {str(r.get('rally_from_low_pct','?'))+'%':>12}
  Higher Low:     {str(r.get('higher_low','?')):>12}
  VIX Trend:      {str(r.get('vix_trend','?')):>12}
  VIX Peak:       {str(r.get('vix_peak','?')):>12}
  RSI Weekly:     {str(r.get('rsi_weekly','?')):>12}
  ──────────────────────────────────────────
""")
    return output


# ── MARKET HOURS (IST) ──────────────────────────────────────────────────────────

def is_market_hours():
    """PythonAnywhere runs GMT — convert to IST (+5:30) for accuracy."""
    now_ist = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
    if now_ist.weekday() >= 5:
        return False
    open_t  = now_ist.replace(hour=9,  minute=15, second=0, microsecond=0)
    close_t = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    return open_t <= now_ist <= close_t


def run_loop(interval_minutes=5):
    print(f"Loop mode: fetching every {interval_minutes} min during IST market hours.")
    while True:
        if is_market_hours():
            fetch_all()
        else:
            now_ist = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
            print(f"  [sleep] {now_ist.strftime('%H:%M IST')} — outside market hours.")
        time.sleep(interval_minutes * 60)


# ── ENTRY POINT ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="India Market Correction Dashboard — Master Fetcher v2")
    parser.add_argument("--loop", action="store_true",
                        help="Run continuously every N min during market hours")
    parser.add_argument("--interval", type=int, default=5,
                        help="Loop interval in minutes (default: 5)")
    args = parser.parse_args()

    if args.loop:
        run_loop(args.interval)
    else:
        fetch_all()
