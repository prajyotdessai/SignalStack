"""
NSE PRO TRADER v5.1 — KITE CONNECT NATIVE (yfinance removed)
=============================================================
MIGRATION v5.0 → v5.1:
  ✅ yfinance REMOVED entirely — all data now via Kite Connect
  ✅ Historical OHLCV via kite.historical_data() (instruments API)
  ✅ Live LTP / quotes via kite.ltp() and kite.quote()
  ✅ Nifty 50 benchmark via INDICES instrument token
  ✅ News headlines via NewsAPI (optional; set NEWS_API_KEY in secrets)
  ✅ Instrument token cache built once per session from kite.instruments("NSE")
  ✅ All 250 stocks + 16 strategies retained
  ✅ Zerodha order placement / square-off retained
  ✅ Paper trading, MTF confluence, P&L tracker retained

SECRETS REQUIRED (.streamlit/secrets.toml):
  KITE_API_KEY       = "xxxxxx"
  KITE_API_SECRET    = "xxxxxx"
  ANTHROPIC_API_KEY  = "xxxxxx"          # optional — AI sentiment
  TELEGRAM_TOKEN     = "xxxxxx"          # optional — alerts
  TELEGRAM_CHAT_ID   = "xxxxxx"          # optional — alerts
  NEWS_API_KEY       = "xxxxxx"          # optional — news headlines

HOW TO AUTHENTICATE DAILY:
  1. Click "Login Zerodha" link in sidebar
  2. Complete login + 2FA
  3. Copy request_token from redirect URL
  4. Paste in sidebar → Connect
  App stores access token in session; re-auth required each day.
"""

import streamlit as st
import pandas as pd
import numpy as np
import ta
import requests
import time
import json
import concurrent.futures
import io
from datetime import datetime, date, timedelta
import anthropic

# ── Kite Connect ──────────────────────────────────────────────
try:
    from kiteconnect import KiteConnect
    KITE_AVAILABLE = True
except ImportError:
    KITE_AVAILABLE = False

st.set_page_config(
    layout="wide",
    page_title="NSE Pro Trader v5.1 | Kite Native",
    page_icon="📈",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.block-container { padding-top:1rem; }
.regime-bull { background:#0d2e1a; border:1px solid #00e676; border-radius:6px; padding:4px 12px; color:#00e676; }
.regime-bear { background:#2e0d0d; border:1px solid #ff1744; border-radius:6px; padding:4px 12px; color:#ff1744; }
.regime-side { background:#1a1a0d; border:1px solid #ffd600; border-radius:6px; padding:4px 12px; color:#ffd600; }
.sector-tag  { display:inline-block; background:var(--color-background-secondary);
               border:0.5px solid var(--color-border-secondary); border-radius:4px;
               padding:1px 8px; font-size:11px; color:var(--color-text-secondary); margin-right:4px; }
div[data-testid="stExpander"] { border:1px solid #21262d !important; }
.stButton > button { font-weight:600; }
.kite-badge { background:#387ED1; color:#fff; padding:2px 10px; border-radius:4px;
              font-size:11px; font-weight:700; letter-spacing:0.5px; }
</style>
""", unsafe_allow_html=True)

# ╔══════════════════════════════════════════════════════════════╗
# ║              NIFTY 100 UNIVERSE                             ║
# ╚══════════════════════════════════════════════════════════════╝
NIFTY100_SYM = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK",
    "HINDUNILVR","ITC","SBIN","BHARTIARTL","KOTAKBANK",
    "LT","AXISBANK","ASIANPAINT","MARUTI","TITAN",
    "SUNPHARMA","ULTRACEMCO","BAJFINANCE","WIPRO","HCLTECH",
    "TATAMOTORS","POWERGRID","NTPC","ONGC","JSWSTEEL",
    "TATASTEEL","ADANIPORTS","COALINDIA","BAJAJFINSV","DIVISLAB",
    "DRREDDY","CIPLA","TECHM","NESTLEIND","BRITANNIA",
    "GRASIM","HINDALCO","EICHERMOT","BPCL","INDUSINDBK",
    "TATACONSUM","APOLLOHOSP","HEROMOTOCO","BAJAJ-AUTO","SBILIFE",
    "HDFCLIFE","ADANIENT","VEDL","PIDILITIND","HAVELLS",
    "NAUKRI","MCDOWELL-N","GODREJCP","DMART","SIEMENS",
    "AMBUJACEM","DABUR","MARICO","COLPAL","BERGEPAINT",
    "TORNTPHARM","LUPIN","AUBANK","BANDHANBNK","FEDERALBNK",
    "IDFCFIRSTB","PNB","BANKBARODA","CANBK","UNIONBANK",
    "INDIGO","TRENT","ZOMATO","ADANIGREEN","TATAPOWER",
    "LICI","GAIL","IOC","HINDPETRO","RECLTD",
    "PFC","IRFC","NHPC","SJVN","TORNTPOWER",
    "AUROPHARMA","ALKEM","LAURUSLABS","MPHASIS","LTIM",
    "PERSISTENT","COFORGE","KPITTECH","ASHOKLEY","TVSMOTOR",
    "BALKRISIND","CHOLAFIN","LICHSGFIN","MANAPPURAM","ABCAPITAL",
]

MIDCAP150_SYM = [
    "MUTHOOTFIN","SUNDARMFIN","BAJAJHLDNG","IIFL","PNBHOUSING",
    "CANFINHOME","HOMEFIRST","AAVAS","CREDITACC","APTUS",
    "LTTS","HEXAWARE","MASTEK","BIRLASOFT","TATAELXSI",
    "INTELLECT","TANLA","RATEGAIN",
    "ABBOTINDIA","PFIZER","GLAXO","SANOFI","AJANTPHARM",
    "JBCHEPHARM","GRANULES","ERIS",
    "MOTHERSON","BOSCHLTD","BHARATFORG","EXIDEIND",
    "AMARAJABAT","MINDA","SUPRAJIT","GABRIEL",
    "EMAMILTD","JYOTHYLAB","VSTIND","RADICO","BIKAJI",
    "DEVYANI","WESTLIFE",
    "CUMMINSIND","ABB","THERMAX","BHEL","KEC",
    "NCC","HGINFRA","PNCINFRA",
    "AARTI","DEEPAKNITR","NAVINFLUOR","TATACHEM",
    "VINATI","FINEORG","ROSSARI",
    "NMDC","MOIL","RATNAMANI","WELCORP","JSPL",
    "JINDALSAW",
    "GODREJPROP","OBEROIRLTY","PRESTIGE","BRIGADE","SOBHA",
    "PHOENIXLTD","LODHA",
    "CESC","JSWENERGY","POWERMECH",
    "PAGEIND","MANYAVAR","SHOPERSTOP","RAYMOND","ARVIND",
    "WELSPUNIND","TRIDENT","VARDHMAN",
    "IRCTC","CONCOR","BLUEDART",
    "ZEEL","SUNTV","PVRINOX","NETWEB",
    "TATACOMM","HFCL","RAILTEL",
    "JKCEMENT","RAMCOCEM","HEIDELBERG","ORIENTCEM",
    "KAJARIA","GREENPLY","CENTURYPLY",
]

# Remove duplicates
MIDCAP150_SYM = [t for t in MIDCAP150_SYM if t not in NIFTY100_SYM]
ALL_SYMBOLS    = NIFTY100_SYM + MIDCAP150_SYM

# Sector mapping
SECTOR_MAP = {
    "RELIANCE":"Oil & Gas","TCS":"IT","HDFCBANK":"Banking",
    "INFY":"IT","ICICIBANK":"Banking","SBIN":"Banking",
    "BHARTIARTL":"Telecom","ITC":"FMCG","HINDUNILVR":"FMCG",
    "KOTAKBANK":"Banking","LT":"Infra","AXISBANK":"Banking",
    "ASIANPAINT":"Consumer","MARUTI":"Auto","TITAN":"Consumer",
    "SUNPHARMA":"Pharma","BAJFINANCE":"NBFC","WIPRO":"IT",
    "HCLTECH":"IT","TATAMOTORS":"Auto","NTPC":"Power",
    "ONGC":"Oil & Gas","JSWSTEEL":"Steel","TATASTEEL":"Steel",
    "DRREDDY":"Pharma","CIPLA":"Pharma","ZOMATO":"Consumer",
    "TRENT":"Retail","IRFC":"Finance","PFC":"Finance",
    "RECLTD":"Finance","ADANIGREEN":"Power","TATAPOWER":"Power",
    "NHPC":"Power","SJVN":"Power","LTIM":"IT",
    "PERSISTENT":"IT","COFORGE":"IT","KPITTECH":"IT",
    "MPHASIS":"IT","TATAELXSI":"IT","INTELLECT":"IT",
    "MUTHOOTFIN":"NBFC","SUNDARMFIN":"NBFC","BAJAJHLDNG":"Finance",
    "ABBOTINDIA":"Pharma","PFIZER":"Pharma","GLAXO":"Pharma",
    "BOSCHLTD":"Auto Anc","BHARATFORG":"Auto Anc",
    "CUMMINSIND":"Cap Goods","ABB":"Cap Goods","THERMAX":"Cap Goods",
    "GODREJPROP":"Real Estate","OBEROIRLTY":"Real Estate",
    "PRESTIGE":"Real Estate","IRCTC":"Travel","CONCOR":"Logistics",
    "PAGEIND":"Textile","NMDC":"Metals","MOIL":"Metals",
    "DEEPAKNITR":"Chemicals","NAVINFLUOR":"Chemicals",
    "JKCEMENT":"Cement","RAMCOCEM":"Cement","KAJARIA":"Building",
}
def get_sector(sym: str) -> str:
    return SECTOR_MAP.get(sym, "Midcap")

# ╔══════════════════════════════════════════════════════════════╗
# ║                   SESSION STATE                             ║
# ╚══════════════════════════════════════════════════════════════╝
def ss(k, v):
    if k not in st.session_state: st.session_state[k] = v

ss("scan_results", []); ss("scan_ts", None)
ss("kite", None);       ss("access_token", "")
ss("positions", []);    ss("trade_log", [])
ss("paper_trades", []); ss("orders_today", 0)
ss("paper_mode", True); ss("max_trades_day", 5)
ss("capital", 50000);   ss("risk_per_trade", 0.02)
ss("target_daily", 1000); ss("sentiment_weight", 0.0)
ss("instrument_tokens", {})   # sym -> token map (built once)
ss("nifty50_token", 256265)   # NSE:NIFTY 50 index token (standard)

# ╔══════════════════════════════════════════════════════════════╗
# ║            KITE CONNECT CORE HELPERS                        ║
# ╚══════════════════════════════════════════════════════════════╝
def kite_login_obj():
    if not KITE_AVAILABLE: return None
    try:
        return KiteConnect(api_key=st.secrets["KITE_API_KEY"])
    except Exception as e:
        st.error(f"Kite init: {e}"); return None

def kite_set_token(kite, req_token: str) -> bool:
    try:
        data = kite.generate_session(req_token, api_secret=st.secrets["KITE_API_SECRET"])
        st.session_state.access_token = data["access_token"]
        kite.set_access_token(data["access_token"])
        st.session_state.kite = kite
        return True
    except Exception as e:
        st.error(f"Token error: {e}"); return False

def is_connected() -> bool:
    return st.session_state.kite is not None and bool(st.session_state.access_token)

def get_kite() -> "KiteConnect | None":
    return st.session_state.kite if is_connected() else None

# ── Instrument token cache ────────────────────────────────────
@st.cache_data(ttl=86400)   # refresh once a day
def _fetch_instrument_tokens(_kite_key: str, _token: str) -> dict:
    """
    Download NSE instrument list and return {tradingsymbol: instrument_token}.
    Cached for 24 h. The dummy args force re-cache if api_key/token change.
    """
    kite = KiteConnect(api_key=_kite_key)
    kite.set_access_token(_token)
    instruments = kite.instruments("NSE")
    mapping = {}
    for inst in instruments:
        mapping[inst["tradingsymbol"]] = inst["instrument_token"]
    return mapping

def ensure_tokens():
    """Populate session instrument_tokens if not already done."""
    if st.session_state.instrument_tokens:
        return
    if not is_connected():
        return
    try:
        tok_map = _fetch_instrument_tokens(
            st.secrets["KITE_API_KEY"],
            st.session_state.access_token
        )
        st.session_state.instrument_tokens = tok_map
    except Exception as e:
        st.warning(f"Could not load instrument tokens: {e}")

def get_token(symbol: str) -> int | None:
    """Return instrument_token for a given NSE trading symbol."""
    return st.session_state.instrument_tokens.get(symbol)

# ── Historical data ───────────────────────────────────────────
# Interval map: our mode string → Kite interval string
KITE_INTERVAL = {
    "Swing (Daily)":    "day",
    "Intraday (15m)":   "15minute",
    "Intraday (5m)":    "5minute",
}
# How many calendar days to look back
KITE_LOOKBACK_DAYS = {
    "day":      730,   # 2 years
    "15minute": 60,
    "5minute":  30,
}

@st.cache_data(ttl=300)
def get_data_kite(symbol: str, interval: str, _kite_key: str, _access_token: str) -> pd.DataFrame | None:
    """
    Fetch OHLCV from Kite historical API.
    Returns cleaned DataFrame or None.
    """
    try:
        kite = KiteConnect(api_key=_kite_key)
        kite.set_access_token(_access_token)

        # Resolve token
        tok_map = _fetch_instrument_tokens(_kite_key, _access_token)
        token = tok_map.get(symbol)
        if token is None:
            return None

        days = KITE_LOOKBACK_DAYS.get(interval, 365)
        to_dt   = datetime.now()
        from_dt = to_dt - timedelta(days=days)

        records = kite.historical_data(
            instrument_token = token,
            from_date        = from_dt,
            to_date          = to_dt,
            interval         = interval,
            continuous       = False,
            oi               = False,
        )
        if not records:
            return None

        df = pd.DataFrame(records)
        df = df.rename(columns={
            "date":   "Date",
            "open":   "Open",
            "high":   "High",
            "low":    "Low",
            "close":  "Close",
            "volume": "Volume",
        })
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date").sort_index()
        df = df[~df.index.duplicated(keep="last")]
        return df if len(df) >= 50 else None
    except Exception:
        return None


def fetch_parallel_kite(symbols: list[str], interval: str, workers: int = 16) -> dict:
    """Fetch historical data for all symbols in parallel via Kite."""
    if not is_connected():
        return {}
    key   = st.secrets["KITE_API_KEY"]
    token = st.session_state.access_token

    out = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(get_data_kite, sym, interval, key, token): sym
                for sym in symbols}
        for f in concurrent.futures.as_completed(futs):
            out[futs[f]] = f.result()
    return out


# ── Nifty 50 benchmark returns ────────────────────────────────
@st.cache_data(ttl=3600)
def get_nifty50_returns_kite(_kite_key: str, _access_token: str) -> pd.Series:
    """
    Fetch Nifty 50 index daily prices via Kite and return pct_change series.
    Kite index token for NIFTY 50 on NSE = 256265.
    """
    try:
        kite = KiteConnect(api_key=_kite_key)
        kite.set_access_token(_access_token)
        to_dt   = datetime.now()
        from_dt = to_dt - timedelta(days=180)
        records = kite.historical_data(
            instrument_token = 256265,   # NIFTY 50 index
            from_date        = from_dt,
            to_date          = to_dt,
            interval         = "day",
            continuous       = False,
            oi               = False,
        )
        if not records:
            return pd.Series(dtype=float)
        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
        return df["close"].squeeze().pct_change().dropna()
    except Exception:
        return pd.Series(dtype=float)


# ── Live LTP via Kite quote ───────────────────────────────────
def get_ltp(symbol: str) -> float | None:
    """Fetch last traded price for a symbol."""
    kite = get_kite()
    if kite is None:
        return None
    try:
        key = f"NSE:{symbol}"
        q = kite.ltp([key])
        return float(q[key]["last_price"])
    except Exception:
        return None


# ── News headlines (NewsAPI, optional) ────────────────────────
@st.cache_data(ttl=3600)
def get_news(symbol: str) -> tuple:
    """Fetch top headlines for a stock symbol via NewsAPI."""
    try:
        key = st.secrets.get("NEWS_API_KEY", "")
        if not key:
            return ()
        url = (f"https://newsapi.org/v2/everything"
               f"?q={symbol}+NSE&sortBy=publishedAt&pageSize=5&apiKey={key}")
        r = requests.get(url, timeout=5).json()
        return tuple(a["title"] for a in r.get("articles", [])[:5] if a.get("title"))
    except Exception:
        return ()


# ╔══════════════════════════════════════════════════════════════╗
# ║                  ZERODHA ORDER HELPERS                      ║
# ╚══════════════════════════════════════════════════════════════╝
def place_order(symbol, action, qty, price, sl, target, paper_mode=True) -> dict:
    ts = datetime.now().strftime("%H:%M:%S")
    if paper_mode:
        trade = {"symbol":symbol,"action":action,"qty":qty,"entry":price,
                 "sl":sl,"target":target,"status":"Open","pnl":0.0,"time":ts,"mode":"Paper"}
        st.session_state.paper_trades.append(trade)
        st.session_state.trade_log.append(trade.copy())
        st.session_state.orders_today += 1
        _telegram(f"📝 PAPER {action} {symbol}\nQty:{qty} @ ₹{price} SL:₹{sl} Tgt:₹{target}")
        return {"status":"paper","id":f"P-{int(time.time())}"}
    kite = get_kite()
    if kite is None:
        return {"status":"error","error":"Not connected to Kite"}
    try:
        txn = kite.TRANSACTION_TYPE_BUY if action=="BUY" else kite.TRANSACTION_TYPE_SELL
        eid = kite.place_order(variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                               tradingsymbol=symbol, transaction_type=txn,
                               quantity=qty, product=kite.PRODUCT_MIS,
                               order_type=kite.ORDER_TYPE_MARKET)
        time.sleep(0.8)
        sl_txn = kite.TRANSACTION_TYPE_SELL if action=="BUY" else kite.TRANSACTION_TYPE_BUY
        sl_trig = round(sl*0.998,2) if action=="BUY" else round(sl*1.002,2)
        slid = kite.place_order(variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                                tradingsymbol=symbol, transaction_type=sl_txn,
                                quantity=qty, product=kite.PRODUCT_MIS,
                                order_type=kite.ORDER_TYPE_SL_M,
                                trigger_price=sl_trig, price=sl)
        tid = kite.place_order(variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                               tradingsymbol=symbol, transaction_type=sl_txn,
                               quantity=qty, product=kite.PRODUCT_MIS,
                               order_type=kite.ORDER_TYPE_LIMIT, price=target)
        trade = {"symbol":symbol,"action":action,"qty":qty,"entry":price,
                 "sl":sl,"target":target,"status":"Open","pnl":0.0,"time":ts,
                 "mode":"Live","sl_order":slid,"tgt_order":tid}
        st.session_state.trade_log.append(trade.copy())
        st.session_state.orders_today += 1
        _telegram(f"✅ LIVE {action} {symbol} Qty:{qty} Entry:{eid} SL:{slid} Tgt:{tid}")
        return {"status":"live","entry_id":eid,"sl_id":slid,"tgt_id":tid}
    except Exception as e:
        _telegram(f"❌ Order FAILED {symbol}: {e}")
        return {"status":"error","error":str(e)}


def square_off_all(paper_mode=True):
    if paper_mode:
        for t in st.session_state.paper_trades:
            if t["status"]=="Open": t["status"]="Squared Off"
        _telegram("📤 All paper positions squared off")
        return
    kite = get_kite()
    if not kite: return
    try:
        for o in kite.orders():
            if o["status"] in ("OPEN","TRIGGER PENDING"):
                try: kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=o["order_id"])
                except: pass
        time.sleep(0.5)
        for p in kite.positions()["day"]:
            if p["quantity"] != 0:
                txn = kite.TRANSACTION_TYPE_SELL if p["quantity"]>0 else kite.TRANSACTION_TYPE_BUY
                kite.place_order(variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                                  tradingsymbol=p["tradingsymbol"], transaction_type=txn,
                                  quantity=abs(p["quantity"]), product=kite.PRODUCT_MIS,
                                  order_type=kite.ORDER_TYPE_MARKET)
        _telegram("📤 All LIVE MIS positions squared off")
    except Exception as e:
        st.error(f"Square off error: {e}")


def fetch_live_positions():
    kite = get_kite()
    if not kite: return [], 0.0
    try:
        pos = kite.positions()["day"]
        return pos, round(sum(p.get("pnl",0) for p in pos), 2)
    except: return [], 0.0


def paper_pnl_mtm() -> float:
    """Update paper trade P&L using Kite LTP."""
    total = 0.0
    for t in st.session_state.paper_trades:
        if t["status"] != "Open":
            total += t.get("pnl", 0.0); continue
        try:
            ltp = get_ltp(t.get("symbol", "")) or t["entry"]
        except:
            ltp = t["entry"]
        if t["action"] == "BUY":
            if ltp >= t["target"]:
                t["status"]="Target Hit"; t["pnl"]=round((t["target"]-t["entry"])*t["qty"],2)
            elif ltp <= t["sl"]:
                t["status"]="SL Hit";     t["pnl"]=round((t["sl"]-t["entry"])*t["qty"],2)
            else:
                t["pnl"]=round((ltp-t["entry"])*t["qty"],2)
        else:
            if ltp <= t["target"]:
                t["status"]="Target Hit"; t["pnl"]=round((t["entry"]-t["target"])*t["qty"],2)
            elif ltp >= t["sl"]:
                t["status"]="SL Hit";     t["pnl"]=round((t["entry"]-t["sl"])*t["qty"],2)
            else:
                t["pnl"]=round((t["entry"]-ltp)*t["qty"],2)
        total += t["pnl"]
    return round(total, 2)


def _telegram(msg: str):
    try:
        tok = st.secrets.get("TELEGRAM_TOKEN","")
        cid = st.secrets.get("TELEGRAM_CHAT_ID","")
        if tok and cid:
            requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          data={"chat_id":cid,"text":msg}, timeout=5)
    except: pass


# ╔══════════════════════════════════════════════════════════════╗
# ║                  MARKET REGIME (Kite)                       ║
# ╚══════════════════════════════════════════════════════════════╝
@st.cache_data(ttl=3600)
def market_regime_kite(_kite_key: str, _access_token: str) -> str:
    try:
        n = get_nifty50_returns_kite(_kite_key, _access_token)
        if n.empty: return "Sideways"
        # Reconstruct cumulative price index from returns
        prices = (1 + n).cumprod()
        e20 = prices.ewm(span=20).mean()
        e50 = prices.ewm(span=50).mean()
        # Approximate ADX from returns volatility
        adx_proxy = n.rolling(14).std().iloc[-1] * 100 / prices.iloc[-1] * 1000
        if e20.iloc[-1] > e50.iloc[-1] and adx_proxy > 20: return "Bull"
        if e20.iloc[-1] < e50.iloc[-1] and adx_proxy > 20: return "Bear"
        return "Sideways"
    except:
        return "Sideways"


def regime_weights(regime: str) -> dict:
    if regime == "Bull":
        return {"ORB":0.07,"VWAP":0.07,"EMA":0.10,"MACD":0.10,"BB":0.07,"RSI":0.04,
                "ST":0.09,"Stoch":0.05,"W52":0.08,"Pivot":0.03,
                "HH_HL":0.10,"OBV_DIV":0.07,"FLAG":0.07,"RS":0.08,"IB":0.05,"TBR":0.04}
    if regime == "Bear":
        return {"ORB":0.05,"VWAP":0.08,"EMA":0.07,"MACD":0.08,"BB":0.06,"RSI":0.09,
                "ST":0.08,"Stoch":0.07,"W52":0.02,"Pivot":0.06,
                "HH_HL":0.06,"OBV_DIV":0.09,"FLAG":0.06,"RS":0.05,"IB":0.07,"TBR":0.08}
    return {"ORB":0.05,"VWAP":0.07,"EMA":0.06,"MACD":0.07,"BB":0.08,"RSI":0.10,
            "ST":0.06,"Stoch":0.08,"W52":0.04,"Pivot":0.09,
            "HH_HL":0.06,"OBV_DIV":0.07,"FLAG":0.07,"RS":0.05,"IB":0.09,"TBR":0.06}


# ╔══════════════════════════════════════════════════════════════╗
# ║              FEATURE ENGINEERING                            ║
# ╚══════════════════════════════════════════════════════════════╝
def add_features(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) < 50: return pd.DataFrame()
    df = df.copy()
    c = df["Close"].squeeze(); h = df["High"].squeeze()
    l = df["Low"].squeeze();   v = df["Volume"].squeeze()
    df["Close"]=c; df["High"]=h; df["Low"]=l; df["Volume"]=v

    df["ema9"]   = ta.trend.EMAIndicator(c,9).ema_indicator()
    df["ema21"]  = ta.trend.EMAIndicator(c,21).ema_indicator()
    df["ema50"]  = ta.trend.EMAIndicator(c,50).ema_indicator()
    df["ema200"] = ta.trend.EMAIndicator(c,200).ema_indicator()
    df["rsi"]    = ta.momentum.RSIIndicator(c,14).rsi()
    df["atr"]    = ta.volatility.AverageTrueRange(h,l,c,14).average_true_range()
    df["obv"]    = ta.volume.OnBalanceVolumeIndicator(c,v).on_balance_volume()

    if hasattr(df.index, 'date'):
        dates = pd.Series(df.index.date, index=df.index)
        df["vwap"] = ((c*v).groupby(dates).cumsum()
                      / v.replace(0,np.nan).groupby(dates).cumsum())
    else:
        df["vwap"] = (c*v).cumsum() / v.replace(0,np.nan).cumsum()

    mi = ta.trend.MACD(c)
    df["macd"]=mi.macd(); df["macd_s"]=mi.macd_signal(); df["macd_h"]=mi.macd_diff()

    bb = ta.volatility.BollingerBands(c,20,2)
    df["bb_u"]=bb.bollinger_hband(); df["bb_l"]=bb.bollinger_lband()
    df["bb_m"]=bb.bollinger_mavg();  df["bb_w"]=(df["bb_u"]-df["bb_l"])/df["bb_m"]

    adxi = ta.trend.ADXIndicator(h,l,c,14)
    df["adx"]=adxi.adx(); df["di_pos"]=adxi.adx_pos(); df["di_neg"]=adxi.adx_neg()

    st2 = ta.momentum.StochasticOscillator(h,l,c,14,3)
    df["stoch_k"]=st2.stoch(); df["stoch_d"]=st2.stoch_signal()

    df["vol_ratio"] = v / v.rolling(20).mean()
    df["body"]      = abs(c - df["Open"].squeeze())
    df["wick_u"]    = h - c.clip(lower=df["Open"].squeeze())
    df["wick_l"]    = c.clip(upper=df["Open"].squeeze()) - l
    df["range"]     = h - l
    df["nr7"]       = df["range"] == df["range"].rolling(7).min()

    return df.ffill().dropna()


# ╔══════════════════════════════════════════════════════════════╗
# ║           ALL 16 STRATEGIES (S1–S16)                        ║
# ╚══════════════════════════════════════════════════════════════╝
def _s(sig, conf, reason): return sig, int(min(95, max(0, conf))), reason

# S1 — ORB
def s_orb(df, mode="Swing (Daily)"):
    if "Daily" in mode or "Swing" in mode: return _s("HOLD",0,"N/A daily")
    if len(df) < 10: return _s("HOLD",0,"")
    oh=df["High"].iloc[:6].max(); ol=df["Low"].iloc[:6].min()
    p=df["Close"].iloc[-1]; vr=df["vol_ratio"].iloc[-1]; adx=df["adx"].iloc[-1]
    if p>oh and vr>1.2 and adx>18: return _s("BUY",65+vr*8,f"ORB breakout ₹{oh:.1f} {vr:.1f}x")
    if p<ol and vr>1.2 and adx>18: return _s("SELL",62+vr*8,f"ORB breakdown ₹{ol:.1f} {vr:.1f}x")
    return _s("HOLD",0,"")

# S2 — VWAP
def s_vwap(df):
    if len(df) < 20: return _s("HOLD",0,"")
    p=df["Close"].iloc[-1]; vw=df["vwap"].iloc[-1]; prev=df["Close"].iloc[-2]
    up=df["ema21"].iloc[-1]>df["ema50"].iloc[-1]; rsi=df["rsi"].iloc[-1]; d=abs(p-vw)/vw*100
    if up and d<0.6 and p>prev and 35<rsi<70: return _s("BUY",70+(0.6-d)*25,f"VWAP pullback dist={d:.2f}%")
    if not up and d<0.6 and p<prev and rsi>40: return _s("SELL",68+(0.6-d)*25,f"VWAP reject dist={d:.2f}%")
    return _s("HOLD",0,"")

# S3 — EMA
def s_ema(df):
    if len(df) < 25: return _s("HOLD",0,"")
    e9,e21=df["ema9"],df["ema21"]
    rsi,adx,vr=df["rsi"].iloc[-1],df["adx"].iloc[-1],df["vol_ratio"].iloc[-1]
    sep=(e9.iloc[-1]-e21.iloc[-1])/e21.iloc[-1]
    if sep>0.001 and 40<rsi<72 and adx>18: return _s("BUY",65+adx*0.4+vr*3,f"EMA9>21 RSI={rsi:.0f}")
    if sep<-0.001 and rsi>30 and adx>18:   return _s("SELL",62+adx*0.4+vr*3,f"EMA9<21 RSI={rsi:.0f}")
    return _s("HOLD",0,"")

# S4 — MACD
def s_macd(df):
    if len(df) < 30: return _s("HOLD",0,"")
    h,adx=df["macd_h"],df["adx"].iloc[-1]
    bull=(h.iloc[-1]>0 and h.iloc[-2]<=0 and adx>22) or (h.iloc[-1]>h.iloc[-2] and df["macd"].iloc[-1]>df["macd_s"].iloc[-1] and adx>22)
    bear=(h.iloc[-1]<0 and h.iloc[-2]>=0 and adx>22) or (h.iloc[-1]<h.iloc[-2] and df["macd"].iloc[-1]<df["macd_s"].iloc[-1] and adx>22)
    if bull: return _s("BUY",70+(adx-22)*0.5,f"MACD bullish ADX={adx:.0f}")
    if bear: return _s("SELL",68+(adx-22)*0.5,f"MACD bearish ADX={adx:.0f}")
    return _s("HOLD",0,"")

# S5 — Bollinger
def s_bb(df):
    if len(df) < 30: return _s("HOLD",0,"")
    bw,p,vr=df["bb_w"],df["Close"].iloc[-1],df["vol_ratio"].iloc[-1]
    rm=bw.rolling(min(50,len(bw))).mean(); sq=bw.iloc[-5:-1].mean()<rm.iloc[-1]*0.80
    if sq and p>df["bb_u"].iloc[-1] and vr>1.2: return _s("BUY",68+vr*5,f"BB squeeze break vol={vr:.1f}x")
    if sq and p<df["bb_l"].iloc[-1] and vr>1.2: return _s("SELL",66+vr*5,f"BB squeeze break vol={vr:.1f}x")
    return _s("HOLD",0,"")

# S6 — RSI
def s_rsi(df):
    if len(df) < 20: return _s("HOLD",0,"")
    rsi,p,prev=df["rsi"],df["Close"].iloc[-1],df["Close"].iloc[-2]
    body,atr=df["body"].iloc[-1],df["atr"].iloc[-1]
    if rsi.iloc[-2]<35 and rsi.iloc[-1]>rsi.iloc[-2] and p>prev and body>0.2*atr:
        return _s("BUY",60+(35-rsi.iloc[-2])*1.5,f"RSI OS reversal {rsi.iloc[-1]:.0f}")
    if rsi.iloc[-2]>65 and rsi.iloc[-1]<rsi.iloc[-2] and p<prev and body>0.2*atr:
        return _s("SELL",58+(rsi.iloc[-2]-65)*1.5,f"RSI OB reversal {rsi.iloc[-1]:.0f}")
    return _s("HOLD",0,"")

# S7 — SuperTrend
def s_st(df):
    if len(df) < 20: return _s("HOLD",0,"")
    atr,c,adx=df["atr"],df["Close"],df["adx"].iloc[-1]
    hl2=(df["High"]+df["Low"])/2; up=hl2+3*atr; dn=hl2-3*atr
    if not(c.iloc[-2]>dn.iloc[-2]) and c.iloc[-1]>dn.iloc[-1]:
        return _s("BUY",70+adx*0.4,f"SuperTrend flip bull SL₹{dn.iloc[-1]:.1f}")
    if not(c.iloc[-2]<up.iloc[-2]) and c.iloc[-1]<up.iloc[-1]:
        return _s("SELL",68+adx*0.4,f"SuperTrend flip bear SL₹{up.iloc[-1]:.1f}")
    return _s("HOLD",0,"")

# S8 — Stochastic
def s_stoch(df):
    if len(df) < 20: return _s("HOLD",0,"")
    sk,sd=df["stoch_k"],df["stoch_d"]
    up=df["ema21"].iloc[-1]>df["ema50"].iloc[-1]
    p,vw=df["Close"].iloc[-1],df["vwap"].iloc[-1]
    bx=sk.iloc[-1]>sd.iloc[-1] and sk.iloc[-2]<=sd.iloc[-2] and sk.iloc[-2]<30
    sx=sk.iloc[-1]<sd.iloc[-1] and sk.iloc[-2]>=sd.iloc[-2] and sk.iloc[-2]>70
    if bx and up and p>vw*0.995: return _s("BUY",65+(30-sk.iloc[-2])*0.6,f"Stoch X up {sk.iloc[-1]:.0f}")
    if sx and not up and p<vw*1.005: return _s("SELL",63+(sk.iloc[-2]-70)*0.6,f"Stoch X dn {sk.iloc[-1]:.0f}")
    return _s("HOLD",0,"")

# S9 — 52W
def s_w52(df):
    if len(df) < 252: return _s("HOLD",0,"")
    hi=df["High"].rolling(251).max().iloc[-2]
    p=df["Close"].iloc[-1]; vr=df["vol_ratio"].iloc[-1]; rsi=df["rsi"].iloc[-1]
    if p>hi*1.001 and vr>1.5 and 50<rsi<80: return _s("BUY",75+vr*5,f"52W HIGH break ₹{hi:.1f} {vr:.1f}x")
    return _s("HOLD",0,"")

# S10 — Pivot
def s_pivot(df):
    if len(df) < 5: return _s("HOLD",0,"")
    H=float(df["High"].iloc[-2]); L=float(df["Low"].iloc[-2]); C=float(df["Close"].iloc[-2])
    P=(H+L+C)/3; R1=2*P-L; S1=2*P-H; R2=P+(H-L); S2=P-(H-L)
    p=df["Close"].iloc[-1]; prev=df["Close"].iloc[-2]
    rsi=df["rsi"].iloc[-1]; atr=df["atr"].iloc[-1]
    near=lambda lv: abs(p-lv)<atr*0.5
    if near(S1) and p>prev and rsi<55: return _s("BUY",72,f"Pivot S1 bounce ₹{S1:.1f}")
    if near(S2) and p>prev and rsi<45: return _s("BUY",78,f"Pivot S2 bounce ₹{S2:.1f}")
    if near(R1) and p<prev and rsi>55: return _s("SELL",70,f"Pivot R1 reject ₹{R1:.1f}")
    if near(R2) and p<prev and rsi>60: return _s("SELL",76,f"Pivot R2 reject ₹{R2:.1f}")
    return _s("HOLD",0,"")

# S11 — HH/HL trend structure
def s_hh_hl(df) -> tuple:
    if len(df) < 30: return _s("HOLD",0,"")
    h=df["High"]; l=df["Low"]; c=df["Close"]
    local_highs = h.rolling(5, center=True).max() == h
    local_lows  = l.rolling(5, center=True).min() == l
    swing_highs = h[local_highs].iloc[-4:]
    swing_lows  = l[local_lows].iloc[-4:]
    if len(swing_highs)<2 or len(swing_lows)<2: return _s("HOLD",0,"")
    hh=float(swing_highs.iloc[-1])>float(swing_highs.iloc[-2])
    hl=float(swing_lows.iloc[-1])>float(swing_lows.iloc[-2])
    ll=float(swing_lows.iloc[-1])<float(swing_lows.iloc[-2])
    lh=float(swing_highs.iloc[-1])<float(swing_highs.iloc[-2])
    price=float(c.iloc[-1]); ema50=float(df["ema50"].iloc[-1])
    rsi=float(df["rsi"].iloc[-1]); adx=float(df["adx"].iloc[-1])
    vol_rise=float(df["vol_ratio"].iloc[-1])>1.0
    if hh and hl and price>ema50 and adx>20 and 40<rsi<75 and vol_rise:
        return _s("BUY",int(min(90,72+adx*0.4)),f"HH+HL trend structure | ADX={adx:.0f} | RSI={rsi:.0f}")
    if ll and lh and price<ema50 and adx>20 and rsi<60:
        return _s("SELL",int(min(88,70+adx*0.4)),f"LL+LH downtrend | ADX={adx:.0f} | RSI={rsi:.0f}")
    return _s("HOLD",0,"")

# S12 — OBV divergence
def s_obv_divergence(df) -> tuple:
    if len(df) < 20: return _s("HOLD",0,"")
    c=df["Close"]; obv=df["obv"]; rsi=float(df["rsi"].iloc[-1])
    lb=10
    price_low_now  = float(c.iloc[-1])<float(c.iloc[-lb])
    obv_high_now   = float(obv.iloc[-1])>float(obv.iloc[-lb])
    price_high_now = float(c.iloc[-1])>float(c.iloc[-lb])
    obv_low_now    = float(obv.iloc[-1])<float(obv.iloc[-lb])
    obv_slope=(float(obv.iloc[-1])-float(obv.iloc[-5]))/max(abs(float(obv.iloc[-5])),1)
    if price_low_now and obv_high_now and obv_slope>0 and rsi<55:
        return _s("BUY",int(min(86,68+abs(obv_slope)*500)),f"OBV bullish divergence | RSI={rsi:.0f}")
    if price_high_now and obv_low_now and obv_slope<0 and rsi>45:
        return _s("SELL",int(min(84,66+abs(obv_slope)*500)),f"OBV bearish divergence | RSI={rsi:.0f}")
    return _s("HOLD",0,"")

# S13 — Flag pattern
def s_flag_pattern(df) -> tuple:
    if len(df) < 20: return _s("HOLD",0,"")
    c=df["Close"]; h=df["High"]; l=df["Low"]
    v=df["vol_ratio"]; atr=float(df["atr"].iloc[-1]); rsi=float(df["rsi"].iloc[-1])
    pole_start=float(c.iloc[-10]); pole_end=float(c.iloc[-5])
    pole_pct=(pole_end-pole_start)/pole_start*100
    consol_range=float(h.iloc[-5:-1].max()-l.iloc[-5:-1].min())
    consol_tight=consol_range<atr*2.5
    consol_high=float(h.iloc[-5:-1].max()); consol_low=float(l.iloc[-5:-1].min())
    price=float(c.iloc[-1]); vol_ok=float(v.iloc[-1])>1.3
    if pole_pct>3.0 and consol_tight and price>consol_high and vol_ok and rsi<80:
        return _s("BUY",int(min(88,68+pole_pct*2)),f"Bull flag breakout | Pole={pole_pct:.1f}% | vol={v.iloc[-1]:.1f}x")
    if pole_pct<-3.0 and consol_tight and price<consol_low and vol_ok and rsi>20:
        return _s("SELL",int(min(86,66+abs(pole_pct)*2)),f"Bear flag breakdown | Pole={pole_pct:.1f}%")
    return _s("HOLD",0,"")

# S14 — Relative Strength vs Nifty
def s_relative_strength(df, nifty_returns: pd.Series) -> tuple:
    if len(df)<22 or nifty_returns.empty: return _s("HOLD",0,"")
    c=df["Close"].squeeze()
    lb=min(20,len(c)-1,len(nifty_returns)-1)
    if lb<5: return _s("HOLD",0,"")
    stock_ret=float(c.iloc[-1])/float(c.iloc[-lb])-1
    nifty_ret=float((1+nifty_returns.iloc[-lb:]).prod())-1
    rs_ratio=stock_ret-nifty_ret
    price=float(c.iloc[-1]); ema50=float(df["ema50"].iloc[-1])
    rsi=float(df["rsi"].iloc[-1]); adx=float(df["adx"].iloc[-1])
    if rs_ratio>0.05 and price>ema50 and 45<rsi<75 and adx>18:
        return _s("BUY",int(min(88,68+rs_ratio*200)),f"RS+{rs_ratio*100:.1f}% vs Nifty | Outperforming")
    if rs_ratio<-0.05 and price<ema50 and rsi<55:
        return _s("SELL",int(min(84,64+abs(rs_ratio)*200)),f"RS{rs_ratio*100:.1f}% vs Nifty | Underperforming")
    return _s("HOLD",0,"")

# S15 — Inside Bar / NR7
def s_inside_bar_nr7(df) -> tuple:
    if len(df) < 10: return _s("HOLD",0,"")
    h=df["High"]; l=df["Low"]; c=df["Close"]
    atr=float(df["atr"].iloc[-1]); rsi=float(df["rsi"].iloc[-1])
    vr=float(df["vol_ratio"].iloc[-1]); adx=float(df["adx"].iloc[-1])
    is_inside=(float(h.iloc[-1])<float(h.iloc[-2]) and float(l.iloc[-1])>float(l.iloc[-2]))
    is_nr7=bool(df["nr7"].iloc[-1]) if "nr7" in df.columns else False
    prev_high=float(h.iloc[-2]); prev_low=float(l.iloc[-2])
    price=float(c.iloc[-1]); trend_up=float(df["ema21"].iloc[-1])>float(df["ema50"].iloc[-1])
    if (is_inside or is_nr7) and price>prev_high and vr>1.3 and trend_up and rsi<75:
        tag="Inside bar" if is_inside else "NR7"
        return _s("BUY",int(min(85,65+vr*5+adx*0.3)),f"{tag} breakout above ₹{prev_high:.1f} | vol={vr:.1f}x")
    if (is_inside or is_nr7) and price<prev_low and vr>1.3 and not trend_up and rsi>25:
        tag="Inside bar" if is_inside else "NR7"
        return _s("SELL",int(min(83,63+vr*5+adx*0.3)),f"{tag} breakdown below ₹{prev_low:.1f} | vol={vr:.1f}x")
    return _s("HOLD",0,"")

# S16 — Three-bar reversal
def s_three_bar_reversal(df) -> tuple:
    if len(df) < 8: return _s("HOLD",0,"")
    c=df["Close"]; o=df["Open"].squeeze() if "Open" in df.columns else df["Close"]
    rsi=df["rsi"]; vr=float(df["vol_ratio"].iloc[-1])
    three_red=(float(c.iloc[-4])>float(c.iloc[-3])>float(c.iloc[-2]))
    bar4_green=float(c.iloc[-1])>float(c.iloc[-2])
    bar4_midpoint=(float(c.iloc[-4])+float(c.iloc[-3]))/2
    bar4_strong=float(c.iloc[-1])>bar4_midpoint
    rsi_turning=float(rsi.iloc[-2])<42 and float(rsi.iloc[-1])>float(rsi.iloc[-2])
    if three_red and bar4_green and bar4_strong and rsi_turning and vr>1.4:
        conf=int(min(88,68+vr*5+(42-float(rsi.iloc[-2]))*0.5))
        return _s("BUY",conf,f"3-bar reversal | RSI={rsi.iloc[-1]:.0f} turning | {vr:.1f}x vol")
    three_green=(float(c.iloc[-4])<float(c.iloc[-3])<float(c.iloc[-2]))
    bar4_red=float(c.iloc[-1])<float(c.iloc[-2])
    bar4_mid_dn=(float(c.iloc[-4])+float(c.iloc[-3]))/2
    bar4_strong_dn=float(c.iloc[-1])<bar4_mid_dn
    rsi_turning_dn=float(rsi.iloc[-2])>58 and float(rsi.iloc[-1])<float(rsi.iloc[-2])
    if three_green and bar4_red and bar4_strong_dn and rsi_turning_dn and vr>1.4:
        conf=int(min(86,66+vr*5+(float(rsi.iloc[-2])-58)*0.5))
        return _s("SELL",conf,f"3-bar top reversal | RSI={rsi.iloc[-1]:.0f} turning | {vr:.1f}x vol")
    return _s("HOLD",0,"")


# ╔══════════════════════════════════════════════════════════════╗
# ║                 STRATEGY REGISTRY                           ║
# ╚══════════════════════════════════════════════════════════════╝
STRAT_FNS = {
    "ORB":s_orb,"VWAP":s_vwap,"EMA":s_ema,"MACD":s_macd,
    "BB":s_bb,"RSI":s_rsi,"ST":s_st,"Stoch":s_stoch,"W52":s_w52,"Pivot":s_pivot,
    "HH_HL":s_hh_hl,"OBV_DIV":s_obv_divergence,"FLAG":s_flag_pattern,
    "RS":s_relative_strength,"IB":s_inside_bar_nr7,"TBR":s_three_bar_reversal,
}
STRAT_LABELS = {
    "ORB":"ORB Breakout","VWAP":"VWAP Pullback","EMA":"EMA Momentum",
    "MACD":"MACD+ADX","BB":"BB Squeeze","RSI":"RSI Reversal",
    "ST":"SuperTrend","Stoch":"Stoch+EMA","W52":"52W Breakout","Pivot":"Pivot Bounce",
    "HH_HL":"HH+HL Trend","OBV_DIV":"OBV Divergence","FLAG":"Flag Pattern",
    "RS":"Relative Strength","IB":"Inside Bar/NR7","TBR":"3-Bar Reversal",
}
NEEDS_NIFTY = {"RS"}
NEEDS_MODE  = {"ORB"}

def run_strategies(df, enabled_keys, mode, rw, nifty_ret=None) -> dict:
    bw=sw=tw=0.0; results={}; triggers=[]
    for k in enabled_keys:
        fn=STRAT_FNS[k]; w=rw.get(k,0.06)
        try:
            if k in NEEDS_NIFTY:
                sig,conf,reason = fn(df, nifty_ret if nifty_ret is not None else pd.Series(dtype=float))
            elif k in NEEDS_MODE:
                sig,conf,reason = fn(df, mode)
            else:
                sig,conf,reason = fn(df)
            results[k]={"signal":sig,"confidence":conf,"reason":reason,"label":STRAT_LABELS[k]}
            if sig=="BUY":  bw+=w*(conf/100); triggers.append(f"✅ {STRAT_LABELS[k]} BUY ({conf}%)")
            elif sig=="SELL": sw+=w*(conf/100); triggers.append(f"🔴 {STRAT_LABELS[k]} SELL ({conf}%)")
            tw+=w
        except Exception: pass
    score=(bw-sw)/tw if tw else 0
    sig="BUY" if score>0.20 else ("SELL" if score<-0.20 else "HOLD")
    return {"signal":sig,"score":round(score,3),"strategies":results,"triggers":triggers,
            "n_buy":sum(1 for v in results.values() if v["signal"]=="BUY"),
            "n_sell":sum(1 for v in results.values() if v["signal"]=="SELL")}


def mtf_check_kite(symbol: str, primary_sig: str, enabled_keys: list,
                   rw: dict, nifty_ret: pd.Series) -> bool:
    """Multi-timeframe confirmation using Kite 15m data."""
    if not is_connected(): return True
    try:
        df15 = get_data_kite(symbol, "15minute",
                             st.secrets["KITE_API_KEY"], st.session_state.access_token)
        if df15 is None: return True
        df15 = add_features(df15)
        if df15.empty: return True
        r = run_strategies(df15, enabled_keys, "Intraday (15m)", rw, nifty_ret)
        return r["signal"] == primary_sig or r["signal"] == "HOLD"
    except:
        return True


def candle_patterns(df) -> list:
    o=float(df["Open"].iloc[-1]); h=float(df["High"].iloc[-1])
    l=float(df["Low"].iloc[-1]); c=float(df["Close"].iloc[-1])
    c2=float(df["Close"].iloc[-2]); o2=float(df["Open"].iloc[-2])
    body=abs(c-o); rng=h-l; wu=h-max(o,c); wl=min(o,c)-l
    pats=[]
    if rng>0 and body/rng<0.10: pats.append("Doji")
    if c2<o2 and c>o and c>o2 and o<c2: pats.append("Bull Engulf")
    if c2>o2 and c<o and c<o2 and o>c2: pats.append("Bear Engulf")
    if rng>0 and wl>2*body and wu<body*0.5: pats.append("Hammer")
    if rng>0 and wu>2*body and wl<body*0.5: pats.append("Shoot Star")
    if rng>0 and body/rng>0.85: pats.append("Marubozu" if c>o else "Bear Marubozu")
    return pats


def week52(df) -> dict:
    n=min(252,len(df))
    hi=df["High"].rolling(n).max().iloc[-1]; lo=df["Low"].rolling(n).min().iloc[-1]
    p=df["Close"].iloc[-1]
    return {"hi52":round(hi,2),"lo52":round(lo,2),
            "pct_hi":round((p-hi)/hi*100,2),"pct_lo":round((p-lo)/lo*100,2),
            "near_hi":(p-hi)/hi*100>-5}


# ╔══════════════════════════════════════════════════════════════╗
# ║              BATCH AI SENTIMENT                             ║
# ╚══════════════════════════════════════════════════════════════╝
@st.cache_data(ttl=1800)
def ai_sentiment_batch(payload_json: str) -> dict:
    try:
        key = st.secrets.get("ANTHROPIC_API_KEY","")
        if not key: return {}
        client = anthropic.Anthropic(api_key=key)
        items = json.loads(payload_json)
        lines = [f'{it["ticker"]}: Rs{it["price"]:.1f} ({it["pct"]:+.1f}%) | {"; ".join(it["headlines"][:3]) or "No news"}'
                 for it in items]
        prompt = ("Senior NSE analyst. For each stock return JSON array only, no markdown:\n\n"
                  + "\n".join(lines)
                  + '\n\n[{"ticker":"X","score":<-1 to 1>,"label":"Bullish/Neutral/Bearish","confidence":<0-100>,"summary":"1 line"}]')
        r = client.messages.create(model="claude-sonnet-4-20250514",max_tokens=600,
                                    messages=[{"role":"user","content":prompt}])
        arr = json.loads(r.content[0].text.strip().replace("```json","").replace("```",""))
        return {a["ticker"]:{"score":max(-1,min(1,float(a.get("score",0)))),
                              "label":a.get("label","Neutral"),
                              "confidence":max(0,min(100,int(a.get("confidence",0)))),
                              "summary":a.get("summary","—")} for a in arr}
    except:
        return {}


# ╔══════════════════════════════════════════════════════════════╗
# ║              POSITION SIZING                                ║
# ╚══════════════════════════════════════════════════════════════╝
def pos_size(price, atr, capital, risk_pct, direction="BUY") -> dict:
    risk = capital * risk_pct
    sl     = round(price - atr, 2) if direction=="BUY" else round(price + atr, 2)
    tgt    = round(price + 2*atr, 2) if direction=="BUY" else round(price - 2*atr, 2)
    qty    = max(1, int(risk / max(atr, 0.01)))
    qty    = min(qty, int(capital * 0.25 / price))
    inv    = round(qty*price, 2); gain=round(qty*2*atr,2); loss=round(qty*atr,2)
    brok   = round(inv*0.0005*2, 2)
    return {"qty":qty,"invest":inv,"sl":sl,"target":tgt,
            "pot_gain":gain,"pot_loss":loss,"brokerage":brok,
            "net_gain":round(gain-brok,2),"rr":"1:2"}


# ╔══════════════════════════════════════════════════════════════╗
# ║              SCAN ONE STOCK                                 ║
# ╚══════════════════════════════════════════════════════════════╝
def scan_one(symbol, df_raw, mode, enabled_keys, rw,
             use_mtf, sent_cache, capital, risk_pct, nifty_ret) -> dict | None:
    try:
        df = add_features(df_raw)
        if df.empty or len(df)<50: return None
        tech = run_strategies(df, enabled_keys, mode, rw, nifty_ret)
        p    = float(df["Close"].iloc[-1]); prev=float(df["Close"].iloc[-2])
        pct  = (p-prev)/prev*100; atr=float(df["atr"].iloc[-1])
        mtf_ok = True
        if use_mtf and tech["signal"] in ("BUY","SELL") and "Daily" in mode:
            mtf_ok = mtf_check_kite(symbol, tech["signal"], enabled_keys, rw, nifty_ret)
        cpats = candle_patterns(df); w52s=week52(df)
        sent  = sent_cache.get(symbol, {"score":0,"label":"Neutral","confidence":0,"summary":"—"})
        sw    = st.session_state.sentiment_weight
        blended = (1-sw)*tech["score"] + sw*sent["score"]
        if not mtf_ok: blended *= 0.7
        final = "BUY" if blended>0.20 else ("SELL" if blended<-0.20 else "HOLD")
        position = pos_size(p,atr,capital,risk_pct,final) if final in ("BUY","SELL") and atr>0 else {}
        sector   = get_sector(symbol)
        cap_type = "Large Cap" if symbol in NIFTY100_SYM else "Mid Cap"
        return {
            "ticker":symbol,"price":round(p,2),"change_pct":round(pct,2),"atr":round(atr,2),
            "tech_score":tech["score"],"tech_signal":tech["signal"],
            "strategies":tech["strategies"],"triggers":tech["triggers"],
            "n_buy":tech["n_buy"],"n_sell":tech["n_sell"],
            "sent_score":sent["score"],"sent_label":sent["label"],
            "sent_conf":sent["confidence"],"sent_summary":sent["summary"],
            "final_score":round(blended,3),"final_signal":final,
            "position":position,"mtf_ok":mtf_ok,"w52":w52s,"candles":cpats,
            "sector":sector,"cap_type":cap_type,
        }
    except:
        return None


# ╔══════════════════════════════════════════════════════════════╗
# ║                       SIDEBAR                               ║
# ╚══════════════════════════════════════════════════════════════╝
with st.sidebar:
    st.markdown('<span class="kite-badge">⚡ Kite Native v5.1</span>', unsafe_allow_html=True)
    st.header("⚙️ Settings")

    # ── Zerodha Auth ──────────────────────────────────────────
    st.subheader("🔗 Zerodha Kite Connect")
    with st.expander("❓ How to authenticate", expanded=False):
        st.markdown("""
**Daily auth flow:**
1. Secrets in `.streamlit/secrets.toml`:
   ```
   KITE_API_KEY = "xxxxx"
   KITE_API_SECRET = "xxxxx"
   ```
2. Click **Login Zerodha** link below
3. Complete login + 2FA on Zerodha
4. Copy `request_token=XXXXX` from redirect URL
5. Paste in Step 2 box → Connect

⚠️ Token is one-time-use, expires in ~2 min.
Re-authenticate every trading day.
        """)

    if not KITE_AVAILABLE:
        st.error("Run: `pip install kiteconnect`")
    else:
        st.session_state.paper_mode = st.toggle(
            "📝 Paper Trade Mode", value=st.session_state.paper_mode)
        if not st.session_state.paper_mode:
            st.warning("⚠️ LIVE MODE — Real orders will be placed!")

        if is_connected():
            try: name = st.session_state.kite.profile().get("user_name","")
            except: name = ""
            st.success(f"✅ Connected via Kite{' — '+name if name else ''}")
            tok_count = len(st.session_state.instrument_tokens)
            if tok_count:
                st.caption(f"🗄️ {tok_count:,} instrument tokens loaded")
            else:
                if st.button("📥 Load Instrument Tokens"):
                    ensure_tokens()
                    st.rerun()
            if st.button("🔌 Disconnect"):
                st.session_state.kite = None
                st.session_state.access_token = ""
                st.session_state.instrument_tokens = {}
                st.rerun()
        else:
            if "KITE_API_KEY" in st.secrets:
                kite_obj = kite_login_obj()
                if kite_obj:
                    st.markdown(f"**Step 1:** [Login Zerodha ↗]({kite_obj.login_url()})")
                    req_token = st.text_input("Step 2: Paste request_token",
                                              placeholder="Paste from redirect URL...")
                    if st.button("🔑 Connect", type="primary") and req_token.strip():
                        with st.spinner("Connecting & loading tokens..."):
                            if kite_set_token(kite_obj, req_token.strip()):
                                ensure_tokens()
                                st.success("✅ Connected!")
                                st.rerun()
            else:
                st.info("Add `KITE_API_KEY` + `KITE_API_SECRET` to `.streamlit/secrets.toml`")

    st.divider()

    # ── Universe ──────────────────────────────────────────────
    st.subheader("📊 Universe")
    universe_choice = st.radio("Scan scope",
                               ["All (Nifty 100 + Midcap 150)",
                                "Large Cap only (Nifty 100)",
                                "Mid Cap only (Midcap 150)"],
                               index=0)
    if "Large Cap" in universe_choice:   UNIVERSE = NIFTY100_SYM
    elif "Mid Cap" in universe_choice:   UNIVERSE = MIDCAP150_SYM
    else:                                UNIVERSE = ALL_SYMBOLS
    st.caption(f"Scanning {len(UNIVERSE)} stocks")

    mode = st.selectbox("Timeframe",
                        ["Swing (Daily)","Intraday (15m)","Intraday (5m)"])
    kite_interval = KITE_INTERVAL[mode]
    st.divider()

    # ── Strategies ────────────────────────────────────────────
    st.subheader("Strategies")
    st.caption("Original 10:")
    enabled_keys = []
    orig = ["ORB","VWAP","EMA","MACD","BB","RSI","ST","Stoch","W52","Pivot"]
    new6 = ["HH_HL","OBV_DIV","FLAG","RS","IB","TBR"]
    cols = st.columns(2)
    for i,k in enumerate(orig):
        with cols[i%2]:
            if st.checkbox(STRAT_LABELS[k], value=True, key=f"s_{k}"):
                enabled_keys.append(k)
    st.caption("New high-accuracy strategies:")
    cols2 = st.columns(2)
    for i,k in enumerate(new6):
        with cols2[i%2]:
            if st.checkbox(STRAT_LABELS[k], value=True, key=f"s_{k}"):
                enabled_keys.append(k)

    st.divider()
    use_mtf       = st.toggle("📊 MTF Confluence", value=True)
    use_sentiment = st.toggle("🤖 AI Sentiment",   value=False)
    sent_w        = st.slider("Sentiment Weight",0.0,0.5,0.25,0.05, disabled=not use_sentiment)
    st.session_state.sentiment_weight = sent_w if use_sentiment else 0.0

    st.divider()
    st.session_state.capital        = st.number_input("Capital (₹)",10000,500000,st.session_state.capital,5000)
    st.session_state.risk_per_trade = st.slider("Risk per Trade %",0.5,5.0,2.0,0.5)/100
    st.session_state.target_daily   = st.number_input("Daily Target (₹)",500,10000,st.session_state.target_daily,500)
    st.session_state.max_trades_day = int(st.number_input("Max Trades/Day",1,20,st.session_state.max_trades_day,1))

    st.divider()
    min_strats = st.slider("Min Strategies Agreeing",1,10,3,1,
                           help="3+ recommended — filters noise heavily")
    min_score  = st.slider("Min Score Threshold",0.20,0.70,0.35,0.05,
                           help="0.35+ recommended for high conviction")
    only_52hi  = st.checkbox("Only 52W Breakouts",False)
    only_mtf   = st.checkbox("Only MTF Confirmed",False)
    cap_filter = st.multiselect("Cap Type Filter",["Large Cap","Mid Cap"],
                                default=["Large Cap","Mid Cap"])
    auto_ref   = st.checkbox("⏱️ Auto Refresh (5 min)")

    CAPITAL=st.session_state.capital; RISK_PER_TRADE=st.session_state.risk_per_trade
    TARGET_DAILY=st.session_state.target_daily
    gain_pt=int(CAPITAL*RISK_PER_TRADE*2)
    st.caption(f"Risk/trade: ₹{int(CAPITAL*RISK_PER_TRADE):,} | 1:2 gain: ₹{gain_pt:,}")
    st.caption(f"Need {max(1,int(TARGET_DAILY/gain_pt))} wins for ₹{TARGET_DAILY:,}")

    if st.button("📤 Square Off All", type="secondary", use_container_width=True):
        square_off_all(st.session_state.paper_mode)
        st.success("All positions squared off!")


# ╔══════════════════════════════════════════════════════════════╗
# ║                        MAIN UI                              ║
# ╚══════════════════════════════════════════════════════════════╝
st.title("📈 NSE Pro Trader v5.1 — Kite Connect Native")

# Guard: must be authenticated to scan
if not is_connected():
    st.warning("⚠️ **Not connected to Kite Connect.** Please authenticate in the sidebar to scan markets.")
    st.info("All market data (historical OHLCV, live LTP, Nifty benchmark) is now sourced exclusively from Zerodha Kite Connect. yfinance has been removed.")
    st.stop()

# Ensure tokens loaded
ensure_tokens()

regime = market_regime_kite(st.secrets["KITE_API_KEY"], st.session_state.access_token)
rw     = regime_weights(regime)
rcss   = {"Bull":"regime-bull","Bear":"regime-bear","Sideways":"regime-side"}.get(regime,"regime-side")
pm     = st.session_state.paper_mode
color_tag = "#ffd600" if pm else "#ff1744"

col_r, col_m = st.columns([3,1])
with col_r:
    st.markdown(
        f"<span class='{rcss}'>🌐 Regime: {regime}</span> &nbsp; "
        f"<span style='color:{color_tag};font-weight:700;font-size:14px;'>"
        f"{'📝 PAPER MODE' if pm else '🔴 LIVE TRADING'}</span> &nbsp; "
        f"<span style='color:var(--color-text-secondary);font-size:13px;'>"
        f"Universe: {len(UNIVERSE)} stocks | <span class='kite-badge'>Kite</span></span>",
        unsafe_allow_html=True)
with col_m:
    st.markdown(f"Orders: **{st.session_state.orders_today}** / {st.session_state.max_trades_day}")

st.divider()

# ── P&L Header ───────────────────────────────────────────────
def pnl_header():
    total_pnl = paper_pnl_mtm() if pm else fetch_live_positions()[1]
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("💰 Session P&L",  f"₹{total_pnl:+,.2f}")
    c2.metric("🎯 Daily Target", f"₹{TARGET_DAILY:,}")
    pct_done = min(100, int(abs(total_pnl)/TARGET_DAILY*100)) if TARGET_DAILY>0 else 0
    c3.metric("📊 Progress", f"{pct_done}%")
    open_t = sum(1 for t in st.session_state.paper_trades if t["status"]=="Open")
    c4.metric("📂 Open", open_t)
    c5.metric("📋 Trades Today", st.session_state.orders_today)
    st.progress(min(1.0,pct_done/100), text=f"₹{total_pnl:+.0f} / ₹{TARGET_DAILY:,}")
    return total_pnl

pnl_header()
st.divider()

col_a, col_b = st.columns([3,1])
with col_a:
    do_scan = st.button("🔍 Scan Universe", type="primary", use_container_width=True)
with col_b:
    reuse = st.button("🔄 Re-filter Cache", use_container_width=True,
                      disabled=not st.session_state.scan_results)

if do_scan:
    if not enabled_keys:
        st.warning("Select at least one strategy."); st.stop()
    if not is_connected():
        st.error("Connect to Kite first."); st.stop()

    bar = st.progress(0, f"⚡ Fetching {len(UNIVERSE)} stocks via Kite...")

    # Parallel historical fetch via Kite
    data_cache = fetch_parallel_kite(UNIVERSE, kite_interval, workers=16)
    bar.progress(0.35, "✅ Data ready. Fetching Nifty 50 benchmark...")

    # Nifty 50 for RS strategy
    nifty_ret = get_nifty50_returns_kite(
        st.secrets["KITE_API_KEY"], st.session_state.access_token)
    bar.progress(0.40, "🧠 Running strategies...")

    # Optional AI sentiment
    sent_cache = {}
    if use_sentiment:
        bar.progress(0.42, "🤖 Batch AI sentiment...")
        valid = [s for s in UNIVERSE if data_cache.get(s) is not None]
        payload = []
        for sym in valid:
            df0 = data_cache[sym]
            if df0 is not None and len(df0) > 2:
                p  = float(df0["Close"].iloc[-1])
                pv = float(df0["Close"].iloc[-2])
                payload.append({"ticker":sym,"price":p,"pct":(p-pv)/pv*100,
                                 "headlines":list(get_news(sym))})
        for i in range(0, len(payload), 5):
            sent_cache.update(ai_sentiment_batch(json.dumps(payload[i:i+5])))

    results = []
    for i, symbol in enumerate(UNIVERSE):
        bar.progress(0.45 + (i+1)/len(UNIVERSE)*0.55,
                     f"Analysing {symbol} ({i+1}/{len(UNIVERSE)})...")
        df_raw = data_cache.get(symbol)
        if df_raw is None: continue
        res = scan_one(symbol, df_raw, mode, enabled_keys, rw,
                       use_mtf, sent_cache, CAPITAL, RISK_PER_TRADE, nifty_ret)
        if not res or res["final_signal"]=="HOLD": continue

        n     = res["n_buy"] if res["final_signal"]=="BUY" else res["n_sell"]
        score = abs(res["final_score"])

        if n < min_strats:              continue
        if score < min_score:           continue
        if only_52hi and not res["w52"]["near_hi"]: continue
        if only_mtf  and not res["mtf_ok"]:         continue
        if res["cap_type"] not in cap_filter:        continue

        results.append(res)

        # Telegram high-conviction alert
        if score > 0.50:
            pos = res["position"]
            _telegram(
                f"{'🟢' if res['final_signal']=='BUY' else '🔴'} "
                f"{res['final_signal']}: {res['ticker']} [{res['cap_type']} | {res['sector']}]\n"
                f"₹{res['price']} Score:{res['final_score']:.2f} MTF:{'✅' if res['mtf_ok'] else '⚠️'}\n"
                f"SL:₹{pos.get('sl','—')} Tgt:₹{pos.get('target','—')} Qty:{pos.get('qty','—')}\n"
                f"Strategies: {', '.join([t.split(':')[0].strip('✅🔴 ') for t in res['triggers'][:3]])}\n"
                f"Candles: {', '.join(res['candles']) or 'None'} | Source: Kite"
            )

    bar.empty()
    st.session_state.scan_results = results
    st.session_state.scan_ts = datetime.now().strftime("%H:%M:%S")
    st.rerun()

results = st.session_state.scan_results

if results or reuse:
    buys  = sorted([r for r in results if r["final_signal"]=="BUY"],  key=lambda x:-x["final_score"])
    sells = sorted([r for r in results if r["final_signal"]=="SELL"], key=lambda x:x["final_score"])

    c1,c2,c3,c4,c5,c6,c7 = st.columns(7)
    c1.metric("🟢 BUY",  len(buys))
    c2.metric("🔴 SELL", len(sells))
    c3.metric("📊 Total", len(results))
    c4.metric("🌐 Regime", regime)
    lc = sum(1 for r in results if r["cap_type"]=="Large Cap")
    mc = sum(1 for r in results if r["cap_type"]=="Mid Cap")
    c5.metric("🏦 Large Cap", lc)
    c6.metric("📈 Mid Cap", mc)
    c7.metric("🕐 Scanned", st.session_state.scan_ts or "—")

    if not results:
        st.warning("No signals at current filters. Try: Min Strategies=2, Min Score=0.25")

    st.divider()
    tab1,tab2,tab3,tab4,tab5 = st.tabs([
        "🟢 BUY","🔴 SELL","📋 Table","💰 Live P&L","📈 Analytics"
    ])

    def order_btn(r):
        pos = r["position"]
        if not pos: return
        max_t = st.session_state.max_trades_day
        if st.session_state.orders_today >= max_t:
            st.warning(f"Max {max_t} trades/day reached."); return
        lbl = (f"{'📝 Paper' if pm else '🚀 LIVE'} {r['final_signal']} "
               f"{r['ticker']} Qty:{pos['qty']} @ ₹{r['price']} "
               f"→ SL:₹{pos['sl']} Tgt:₹{pos['target']}")
        if st.button(lbl, key=f"ord_{r['ticker']}_{r['final_signal']}",
                     type="secondary" if pm else "primary",
                     use_container_width=True):
            res = place_order(r["ticker"], r["final_signal"], pos["qty"],
                              r["price"], pos["sl"], pos["target"], pm)
            if res.get("status") in ("paper","live"):
                st.success(f"✅ Order placed! ID:{res.get('id') or res.get('entry_id')}")
                st.rerun()
            else:
                st.error(f"❌ {res.get('error')}")

    def render_cards(sig_list):
        if not sig_list: st.info("No signals for current filters."); return
        for r in sig_list:
            pos = r.get("position",{})
            n   = r["n_buy"] if r["final_signal"]=="BUY" else r["n_sell"]
            with st.expander(
                f"{'🟢' if r['final_signal']=='BUY' else '🔴'} **{r['ticker']}** "
                f"[{r['cap_type']}] "
                f"₹{r['price']} ({r['change_pct']:+.2f}%) | "
                f"Score:{r['final_score']:.3f} | {n}/{len(enabled_keys)} strats | "
                f"{'✅ MTF' if r.get('mtf_ok') else '⚠️ MTF'}"
            ):
                st.markdown(
                    f"<span class='sector-tag'>{r.get('sector','—')}</span>"
                    f"<span class='sector-tag'>{r.get('cap_type','—')}</span>"
                    f"<span class='kite-badge' style='font-size:10px;'>Kite</span>",
                    unsafe_allow_html=True
                )
                c1,c2,c3,c4 = st.columns(4)
                c1.metric("Entry",  f"₹{r['price']}")
                c2.metric("Target", f"₹{pos.get('target','—')}", f"+₹{pos.get('pot_gain','—')}")
                c3.metric("SL",     f"₹{pos.get('sl','—')}",    f"-₹{pos.get('pot_loss','—')}")
                c4.metric("Qty",    pos.get("qty","—"),          f"₹{pos.get('invest','—')}")
                c5,c6,c7,c8 = st.columns(4)
                c5.metric("Net Gain", f"₹{pos.get('net_gain','—')}")
                c6.metric("ATR",      f"₹{r['atr']}")
                c7.metric("52W Hi%",  f"{r['w52']['pct_hi']:.1f}%" if r.get("w52") else "—")
                c8.metric("Sent",     r["sent_label"])
                if r.get("candles"):
                    st.caption("📊 " + " | ".join(r["candles"]))
                st.markdown("**Strategies triggered:**")
                for trig in r["triggers"]: st.caption(trig)
                if r.get("sent_summary","—") not in ("—",""):
                    st.info(f"🤖 {r['sent_label']} ({r['sent_conf']}%): {r['sent_summary']}")
                st.divider()
                order_btn(r)

    with tab1:
        st.subheader(f"🟢 {len(buys)} BUY Signals")
        render_cards(buys)
    with tab2:
        st.subheader(f"🔴 {len(sells)} SELL Signals")
        render_cards(sells)

    with tab3:
        st.subheader("All Signals")
        if results:
            rows = []
            for r in sorted(results, key=lambda x:-abs(x["final_score"])):
                pos = r.get("position",{})
                n   = r["n_buy"] if r["final_signal"]=="BUY" else r["n_sell"]
                rows.append({
                    "Stock":r["ticker"],"Type":r["cap_type"],"Sector":r["sector"],
                    "Price":r["price"],"Chg%":r["change_pct"],
                    "Signal":r["final_signal"],"Score":r["final_score"],
                    "MTF":"✅" if r.get("mtf_ok") else "⚠️",
                    "Strats":f"{n}/{len(enabled_keys)}","Sent":r["sent_label"],
                    "Target":pos.get("target","—"),"SL":pos.get("sl","—"),
                    "Qty":pos.get("qty","—"),"Net(₹)":pos.get("net_gain","—"),
                    "52W Hi%":r["w52"]["pct_hi"] if r.get("w52") else "—",
                    "Candles":", ".join(r.get("candles",[])[:2]),
                    "Source":"Kite",
                })
            df_t = pd.DataFrame(rows)
            def csig(v):
                if v=="BUY":  return "background-color:#1a4731;color:#00e676;font-weight:bold"
                if v=="SELL": return "background-color:#4a1010;color:#ff5252;font-weight:bold"
                return ""
            st.dataframe(df_t.style.map(csig, subset=["Signal"])
                                   .format({"Chg%":"{:+.2f}%","Score":"{:.3f}"}),
                         use_container_width=True, height=500)
            csv = io.BytesIO(); df_t.to_csv(csv, index=False)
            st.download_button("⬇️ Export CSV", csv.getvalue(),
                               f"signals_kite_{date.today()}.csv","text/csv")

    with tab4:
        st.subheader("💰 Live P&L Dashboard")
        if is_connected() and not pm:
            pos_list, live_pnl = fetch_live_positions()
            if pos_list:
                color = "#00e676" if live_pnl>=0 else "#ff1744"
                st.markdown(f"**Live P&L (Kite): <span style='color:{color}'>₹{live_pnl:+,.2f}</span>**",
                            unsafe_allow_html=True)
                pos_df = pd.DataFrame([{
                    "Symbol":p["tradingsymbol"],"Qty":p["quantity"],
                    "Avg":p.get("average_price",0),"LTP":p.get("last_price",0),
                    "P&L":p.get("pnl",0),"Value":p.get("value",0)
                } for p in pos_list if p.get("quantity",0)!=0])
                if not pos_df.empty:
                    st.dataframe(pos_df.style.format({
                        "Avg":"₹{:.2f}","LTP":"₹{:.2f}","P&L":"₹{:+.2f}","Value":"₹{:.2f}"}),
                        use_container_width=True)
                if st.button("🔄 Refresh"): st.rerun()
            else:
                st.info("No open positions in Kite.")
        else:
            pnl_now = paper_pnl_mtm()
            color   = "#00e676" if pnl_now>=0 else "#ff1744"
            st.markdown(f"**Paper P&L (LTP via Kite): <span style='color:{color}'>₹{pnl_now:+,.2f}</span>**",
                        unsafe_allow_html=True)
            t_pct = min(100, int(abs(pnl_now)/TARGET_DAILY*100)) if TARGET_DAILY>0 else 0
            st.progress(max(0.0,min(1.0,t_pct/100)), text=f"{t_pct}% of ₹{TARGET_DAILY:,}")
            if st.session_state.paper_trades:
                rows = [{"Symbol":t.get("symbol",t.get("ticker","")),"Action":t.get("action",""),
                         "Entry":t.get("entry",0),"SL":t.get("sl",0),"Target":t.get("target",0),
                         "Qty":t.get("qty",0),"Status":t.get("status",""),"P&L (₹)":t.get("pnl",0.0),
                         "Time":t.get("time","")} for t in st.session_state.paper_trades]
                pt_df = pd.DataFrame(rows)
                def pnl_c(v):
                    if v>0: return "color:#00e676;font-weight:600"
                    if v<0: return "color:#ff1744;font-weight:600"
                    return ""
                st.dataframe(pt_df.style.map(pnl_c, subset=["P&L (₹)"])
                             .format({"Entry":"₹{:.2f}","SL":"₹{:.2f}",
                                      "Target":"₹{:.2f}","P&L (₹)":"₹{:+.2f}"}),
                             use_container_width=True, height=360)
                col_r1,col_r2 = st.columns(2)
                with col_r1:
                    if st.button("🔄 Refresh P&L"): st.rerun()
                with col_r2:
                    if st.button("🗑️ Clear Trades"):
                        st.session_state.paper_trades=[]; st.rerun()
                tl = io.BytesIO(); pt_df.to_csv(tl, index=False)
                st.download_button("⬇️ Export Log", tl.getvalue(),
                                   f"trades_{date.today()}.csv","text/csv")
            else:
                st.info("No paper trades yet. Place orders from BUY/SELL tabs.")

    with tab5:
        st.subheader("📈 Analytics")
        if results:
            ca,cb = st.columns(2)
            with ca:
                st.markdown("**Score Distribution**")
                st.bar_chart(pd.DataFrame({
                    "Ticker":[r["ticker"] for r in results],
                    "Score":[r["final_score"] for r in results]
                }).set_index("Ticker"))
            with cb:
                st.markdown("**Strategy Hit Count (all 16)**")
                sc = {}
                for r in results:
                    for k,v in r["strategies"].items():
                        if v["signal"] in ("BUY","SELL"):
                            sc[STRAT_LABELS[k]] = sc.get(STRAT_LABELS[k],0)+1
                if sc:
                    sc_sorted = dict(sorted(sc.items(),key=lambda x:-x[1]))
                    st.bar_chart(pd.DataFrame.from_dict(sc_sorted,orient="index",columns=["Hits"]))
            cc,cd = st.columns(2)
            with cc:
                st.markdown("**Sector Distribution**")
                sec_cnt = {}
                for r in results: sec_cnt[r["sector"]] = sec_cnt.get(r["sector"],0)+1
                if sec_cnt:
                    st.bar_chart(pd.DataFrame.from_dict(sec_cnt,orient="index",columns=["Count"]))
            with cd:
                st.markdown("**Large Cap vs Mid Cap**")
                cap_cnt = {
                    "Large Cap": sum(1 for r in results if r["cap_type"]=="Large Cap"),
                    "Mid Cap":   sum(1 for r in results if r["cap_type"]=="Mid Cap"),
                }
                st.bar_chart(pd.DataFrame.from_dict(cap_cnt,orient="index",columns=["Count"]))

# ── Auto square-off at 3:20 PM IST ───────────────────────────
now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
if now_ist.hour==15 and now_ist.minute>=20 and st.session_state.orders_today>0:
    square_off_all(pm)
    _telegram(f"📤 Auto square-off 3:20 PM | P&L: ₹{paper_pnl_mtm():+.2f} | Source: Kite")

if auto_ref:
    st.toast("Refreshing in 5 min..."); time.sleep(300); st.rerun()
