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

try:
    from kiteconnect import KiteConnect
    KITE_AVAILABLE = True
except ImportError:
    KITE_AVAILABLE = False

st.set_page_config(
    layout="wide",
    page_title="NSE Pro Trader v5.2 | Kite Native",
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
# ║              UNIVERSES                                      ║
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
MIDCAP150_SYM = [t for t in MIDCAP150_SYM if t not in NIFTY100_SYM]
ALL_SYMBOLS    = NIFTY100_SYM + MIDCAP150_SYM

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
def get_sector(sym): return SECTOR_MAP.get(sym, "Midcap")

# ╔══════════════════════════════════════════════════════════════╗
# ║                   SESSION STATE                             ║
# ╚══════════════════════════════════════════════════════════════╝
def ss(k, v):
    if k not in st.session_state: st.session_state[k] = v

ss("scan_results", []);   ss("scan_ts", None);     ss("scan_ran", False)
ss("kite", None);         ss("access_token", "")
ss("positions", []);      ss("trade_log", [])
ss("paper_trades", []);   ss("orders_today", 0)
ss("paper_mode", True);   ss("max_trades_day", 5)
ss("capital", 50000);     ss("risk_per_trade", 0.02)
ss("target_daily", 1000); ss("sentiment_weight", 0.0)
ss("tok_map", {})          # FIX BUG 1/2/3: instrument token map in session_state
ss("tok_loaded", False)    # flag so we load only once per session
ss("scan_errors", [])      # FIX BUG 6: collect per-symbol errors for debug
ss("enabled_keys_store", [])  # FIX BUG 8: persist enabled_keys

# ╔══════════════════════════════════════════════════════════════╗
# ║            KITE CONNECT CORE                                ║
# ╚══════════════════════════════════════════════════════════════╝
def kite_login_obj():
    if not KITE_AVAILABLE: return None
    try: return KiteConnect(api_key=st.secrets["KITE_API_KEY"])
    except Exception as e: st.error(f"Kite init: {e}"); return None

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

def get_kite():
    return st.session_state.kite if is_connected() else None

# ── FIX BUG 1/2/3: Token map loaded once into session_state ──
def load_instrument_tokens() -> bool:
    """
    Download NSE instrument list once per session.
    Stores result in session_state.tok_map (plain dict — thread-safe to read).
    Returns True on success.
    """
    if st.session_state.tok_loaded:
        return True
    if not is_connected():
        return False
    try:
        kite = st.session_state.kite
        instruments = kite.instruments("NSE")
        tok_map = {inst["tradingsymbol"]: inst["instrument_token"] for inst in instruments}
        st.session_state.tok_map    = tok_map
        st.session_state.tok_loaded = True
        return True
    except Exception as e:
        st.warning(f"Could not load instrument tokens: {e}")
        return False

# ── FIX BUG 4/5: get_data_kite — no st.cache_data, tok_map passed in ──
KITE_INTERVAL = {
    "Swing (Daily)":  "day",
    "Intraday (15m)": "15minute",
    "Intraday (5m)":  "5minute",
}
# FIX BUG 5: Reduced lookback to stay within Kite API limits
KITE_LOOKBACK_DAYS = {
    "day":      400,   # Kite allows ~2000 candles; 400 calendar days ≈ 270 trading days
    "15minute": 60,
    "5minute":  30,
}

def get_data_kite(symbol: str, interval: str, kite_key: str,
                  access_token: str, tok_map: dict) -> pd.DataFrame | None:
    """
    Fetch OHLCV from Kite historical API.
    tok_map is passed explicitly (no session_state access inside threads).
    No @st.cache_data — caching handled by caller via session_state dict.
    """
    try:
        token = tok_map.get(symbol)
        if token is None:
            return None                        # symbol not in NSE instrument list

        kite = KiteConnect(api_key=kite_key)
        kite.set_access_token(access_token)

        days    = KITE_LOOKBACK_DAYS.get(interval, 365)
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
        # Kite returns dict with keys: date, open, high, low, close, volume
        df = df.rename(columns={
            "date": "Date", "open": "Open", "high": "High",
            "low":  "Low",  "close": "Close", "volume": "Volume",
        })
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date").sort_index()
        df = df[~df.index.duplicated(keep="last")]
        # Must have all required columns
        for col in ["Open","High","Low","Close","Volume"]:
            if col not in df.columns:
                return None
        return df if len(df) >= 50 else None
    except Exception as e:
        return None   # silently skip; errors collected at caller level


def fetch_parallel_kite(symbols: list, interval: str, workers: int = 16) -> dict:
    """
    FIX BUG 3: tok_map snapshot passed into every worker thread.
    No session_state access inside threads.
    """
    if not is_connected():
        return {}
    key      = st.secrets["KITE_API_KEY"]
    token    = st.session_state.access_token
    tok_map  = st.session_state.tok_map      # snapshot for thread safety

    out = {}
    errors = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(get_data_kite, sym, interval, key, token, tok_map): sym
            for sym in symbols
        }
        for f in concurrent.futures.as_completed(futs):
            sym = futs[f]
            try:
                result = f.result()
                out[sym] = result
                if result is None:
                    errors.append(f"{sym}: no data")
            except Exception as e:
                out[sym] = None
                errors.append(f"{sym}: {e}")
    st.session_state.scan_errors = errors
    return out


# ── Nifty 50 benchmark ────────────────────────────────────────
def get_nifty50_returns_kite() -> pd.Series:
    """
    Fetch Nifty 50 index daily prices via Kite.
    Token 256265 = NIFTY 50 index on NSE (standard Kite token).
    """
    if not is_connected():
        return pd.Series(dtype=float)
    try:
        kite    = st.session_state.kite
        to_dt   = datetime.now()
        from_dt = to_dt - timedelta(days=180)
        records = kite.historical_data(
            instrument_token = 256265,
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


# ── Live LTP ──────────────────────────────────────────────────
def get_ltp(symbol: str) -> float | None:
    kite = get_kite()
    if kite is None: return None
    try:
        q = kite.ltp([f"NSE:{symbol}"])
        return float(q[f"NSE:{symbol}"]["last_price"])
    except Exception:
        return None


# ── News (optional) ───────────────────────────────────────────
@st.cache_data(ttl=3600)
def get_news(symbol: str) -> tuple:
    try:
        key = st.secrets.get("NEWS_API_KEY","")
        if not key: return ()
        url = (f"https://newsapi.org/v2/everything"
               f"?q={symbol}+NSE&sortBy=publishedAt&pageSize=5&apiKey={key}")
        r = requests.get(url, timeout=5).json()
        return tuple(a["title"] for a in r.get("articles",[])[:5] if a.get("title"))
    except Exception:
        return ()


# ── Market regime ─────────────────────────────────────────────
def market_regime_kite() -> str:
    try:
        n = get_nifty50_returns_kite()
        if n.empty or len(n) < 50: return "Sideways"
        prices = (1 + n).cumprod()
        e20    = prices.ewm(span=20).mean()
        e50    = prices.ewm(span=50).mean()
        adx_p  = n.rolling(14).std().iloc[-1] * 10000
        if e20.iloc[-1] > e50.iloc[-1] and adx_p > 20: return "Bull"
        if e20.iloc[-1] < e50.iloc[-1] and adx_p > 20: return "Bear"
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
# ║           ALL 16 STRATEGIES                                 ║
# ╚══════════════════════════════════════════════════════════════╝
def _s(sig, conf, reason): return sig, int(min(95, max(0, conf))), reason

def s_orb(df, mode="Swing (Daily)"):
    if "Daily" in mode or "Swing" in mode: return _s("HOLD",0,"N/A daily")
    if len(df)<10: return _s("HOLD",0,"")
    oh=df["High"].iloc[:6].max(); ol=df["Low"].iloc[:6].min()
    p=df["Close"].iloc[-1]; vr=df["vol_ratio"].iloc[-1]; adx=df["adx"].iloc[-1]
    if p>oh and vr>1.2 and adx>18: return _s("BUY",65+vr*8,f"ORB breakout ₹{oh:.1f}")
    if p<ol and vr>1.2 and adx>18: return _s("SELL",62+vr*8,f"ORB breakdown ₹{ol:.1f}")
    return _s("HOLD",0,"")

def s_vwap(df):
    if len(df)<20: return _s("HOLD",0,"")
    p=df["Close"].iloc[-1]; vw=df["vwap"].iloc[-1]; prev=df["Close"].iloc[-2]
    up=df["ema21"].iloc[-1]>df["ema50"].iloc[-1]; rsi=df["rsi"].iloc[-1]; d=abs(p-vw)/vw*100
    if up and d<0.6 and p>prev and 35<rsi<70: return _s("BUY",70+(0.6-d)*25,f"VWAP pullback d={d:.2f}%")
    if not up and d<0.6 and p<prev and rsi>40: return _s("SELL",68+(0.6-d)*25,f"VWAP reject d={d:.2f}%")
    return _s("HOLD",0,"")

def s_ema(df):
    if len(df)<25: return _s("HOLD",0,"")
    e9,e21=df["ema9"],df["ema21"]
    rsi,adx,vr=df["rsi"].iloc[-1],df["adx"].iloc[-1],df["vol_ratio"].iloc[-1]
    sep=(e9.iloc[-1]-e21.iloc[-1])/e21.iloc[-1]
    if sep>0.001 and 40<rsi<72 and adx>18: return _s("BUY",65+adx*0.4+vr*3,f"EMA9>21 RSI={rsi:.0f}")
    if sep<-0.001 and rsi>30 and adx>18:   return _s("SELL",62+adx*0.4+vr*3,f"EMA9<21 RSI={rsi:.0f}")
    return _s("HOLD",0,"")

def s_macd(df):
    if len(df)<30: return _s("HOLD",0,"")
    h,adx=df["macd_h"],df["adx"].iloc[-1]
    bull=(h.iloc[-1]>0 and h.iloc[-2]<=0 and adx>22) or (h.iloc[-1]>h.iloc[-2] and df["macd"].iloc[-1]>df["macd_s"].iloc[-1] and adx>22)
    bear=(h.iloc[-1]<0 and h.iloc[-2]>=0 and adx>22) or (h.iloc[-1]<h.iloc[-2] and df["macd"].iloc[-1]<df["macd_s"].iloc[-1] and adx>22)
    if bull: return _s("BUY",70+(adx-22)*0.5,f"MACD bull ADX={adx:.0f}")
    if bear: return _s("SELL",68+(adx-22)*0.5,f"MACD bear ADX={adx:.0f}")
    return _s("HOLD",0,"")

def s_bb(df):
    if len(df)<30: return _s("HOLD",0,"")
    bw,p,vr=df["bb_w"],df["Close"].iloc[-1],df["vol_ratio"].iloc[-1]
    rm=bw.rolling(min(50,len(bw))).mean(); sq=bw.iloc[-5:-1].mean()<rm.iloc[-1]*0.80
    if sq and p>df["bb_u"].iloc[-1] and vr>1.2: return _s("BUY",68+vr*5,f"BB squeeze vol={vr:.1f}x")
    if sq and p<df["bb_l"].iloc[-1] and vr>1.2: return _s("SELL",66+vr*5,f"BB squeeze vol={vr:.1f}x")
    return _s("HOLD",0,"")

def s_rsi(df):
    if len(df)<20: return _s("HOLD",0,"")
    rsi,p,prev=df["rsi"],df["Close"].iloc[-1],df["Close"].iloc[-2]
    body,atr=df["body"].iloc[-1],df["atr"].iloc[-1]
    if rsi.iloc[-2]<35 and rsi.iloc[-1]>rsi.iloc[-2] and p>prev and body>0.2*atr:
        return _s("BUY",60+(35-rsi.iloc[-2])*1.5,f"RSI OS {rsi.iloc[-1]:.0f}")
    if rsi.iloc[-2]>65 and rsi.iloc[-1]<rsi.iloc[-2] and p<prev and body>0.2*atr:
        return _s("SELL",58+(rsi.iloc[-2]-65)*1.5,f"RSI OB {rsi.iloc[-1]:.0f}")
    return _s("HOLD",0,"")

def s_st(df):
    if len(df)<20: return _s("HOLD",0,"")
    atr,c,adx=df["atr"],df["Close"],df["adx"].iloc[-1]
    hl2=(df["High"]+df["Low"])/2; up=hl2+3*atr; dn=hl2-3*atr
    if not(c.iloc[-2]>dn.iloc[-2]) and c.iloc[-1]>dn.iloc[-1]:
        return _s("BUY",70+adx*0.4,f"SuperTrend bull SL₹{dn.iloc[-1]:.1f}")
    if not(c.iloc[-2]<up.iloc[-2]) and c.iloc[-1]<up.iloc[-1]:
        return _s("SELL",68+adx*0.4,f"SuperTrend bear SL₹{up.iloc[-1]:.1f}")
    return _s("HOLD",0,"")

def s_stoch(df):
    if len(df)<20: return _s("HOLD",0,"")
    sk,sd=df["stoch_k"],df["stoch_d"]
    up=df["ema21"].iloc[-1]>df["ema50"].iloc[-1]
    p,vw=df["Close"].iloc[-1],df["vwap"].iloc[-1]
    bx=sk.iloc[-1]>sd.iloc[-1] and sk.iloc[-2]<=sd.iloc[-2] and sk.iloc[-2]<30
    sx=sk.iloc[-1]<sd.iloc[-1] and sk.iloc[-2]>=sd.iloc[-2] and sk.iloc[-2]>70
    if bx and up and p>vw*0.995: return _s("BUY",65+(30-sk.iloc[-2])*0.6,f"Stoch X up {sk.iloc[-1]:.0f}")
    if sx and not up and p<vw*1.005: return _s("SELL",63+(sk.iloc[-2]-70)*0.6,f"Stoch X dn {sk.iloc[-1]:.0f}")
    return _s("HOLD",0,"")

def s_w52(df):
    if len(df)<252: return _s("HOLD",0,"")
    hi=df["High"].rolling(251).max().iloc[-2]
    p=df["Close"].iloc[-1]; vr=df["vol_ratio"].iloc[-1]; rsi=df["rsi"].iloc[-1]
    if p>hi*1.001 and vr>1.5 and 50<rsi<80: return _s("BUY",75+vr*5,f"52W HIGH ₹{hi:.1f} {vr:.1f}x")
    return _s("HOLD",0,"")

def s_pivot(df):
    if len(df)<5: return _s("HOLD",0,"")
    H=float(df["High"].iloc[-2]); L=float(df["Low"].iloc[-2]); C=float(df["Close"].iloc[-2])
    P=(H+L+C)/3; R1=2*P-L; S1=2*P-H; R2=P+(H-L); S2=P-(H-L)
    p=df["Close"].iloc[-1]; prev=df["Close"].iloc[-2]
    rsi=df["rsi"].iloc[-1]; atr=df["atr"].iloc[-1]
    near=lambda lv: abs(p-lv)<atr*0.5
    if near(S1) and p>prev and rsi<55: return _s("BUY",72,f"Pivot S1 ₹{S1:.1f}")
    if near(S2) and p>prev and rsi<45: return _s("BUY",78,f"Pivot S2 ₹{S2:.1f}")
    if near(R1) and p<prev and rsi>55: return _s("SELL",70,f"Pivot R1 ₹{R1:.1f}")
    if near(R2) and p<prev and rsi>60: return _s("SELL",76,f"Pivot R2 ₹{R2:.1f}")
    return _s("HOLD",0,"")

def s_hh_hl(df):
    if len(df)<30: return _s("HOLD",0,"")
    h=df["High"]; l=df["Low"]; c=df["Close"]
    local_highs=h.rolling(5,center=True).max()==h
    local_lows=l.rolling(5,center=True).min()==l
    sh=h[local_highs].iloc[-4:]; sl=l[local_lows].iloc[-4:]
    if len(sh)<2 or len(sl)<2: return _s("HOLD",0,"")
    hh=float(sh.iloc[-1])>float(sh.iloc[-2]); hl=float(sl.iloc[-1])>float(sl.iloc[-2])
    ll=float(sl.iloc[-1])<float(sl.iloc[-2]); lh=float(sh.iloc[-1])<float(sh.iloc[-2])
    price=float(c.iloc[-1]); ema50=float(df["ema50"].iloc[-1])
    rsi=float(df["rsi"].iloc[-1]); adx=float(df["adx"].iloc[-1])
    vr=float(df["vol_ratio"].iloc[-1])>1.0
    if hh and hl and price>ema50 and adx>20 and 40<rsi<75 and vr:
        return _s("BUY",int(min(90,72+adx*0.4)),f"HH+HL ADX={adx:.0f} RSI={rsi:.0f}")
    if ll and lh and price<ema50 and adx>20 and rsi<60:
        return _s("SELL",int(min(88,70+adx*0.4)),f"LL+LH ADX={adx:.0f} RSI={rsi:.0f}")
    return _s("HOLD",0,"")

def s_obv_divergence(df):
    if len(df)<20: return _s("HOLD",0,"")
    c=df["Close"]; obv=df["obv"]; rsi=float(df["rsi"].iloc[-1])
    lb=10
    pln=float(c.iloc[-1])<float(c.iloc[-lb])
    obn=float(obv.iloc[-1])>float(obv.iloc[-lb])
    phn=float(c.iloc[-1])>float(c.iloc[-lb])
    obl=float(obv.iloc[-1])<float(obv.iloc[-lb])
    slope=(float(obv.iloc[-1])-float(obv.iloc[-5]))/max(abs(float(obv.iloc[-5])),1)
    if pln and obn and slope>0 and rsi<55:
        return _s("BUY",int(min(86,68+abs(slope)*500)),f"OBV bull div RSI={rsi:.0f}")
    if phn and obl and slope<0 and rsi>45:
        return _s("SELL",int(min(84,66+abs(slope)*500)),f"OBV bear div RSI={rsi:.0f}")
    return _s("HOLD",0,"")

def s_flag_pattern(df):
    if len(df)<20: return _s("HOLD",0,"")
    c=df["Close"]; h=df["High"]; l=df["Low"]
    v=df["vol_ratio"]; atr=float(df["atr"].iloc[-1]); rsi=float(df["rsi"].iloc[-1])
    pole_pct=(float(c.iloc[-5])-float(c.iloc[-10]))/float(c.iloc[-10])*100
    consol_range=float(h.iloc[-5:-1].max()-l.iloc[-5:-1].min())
    consol_tight=consol_range<atr*2.5
    ch=float(h.iloc[-5:-1].max()); cl=float(l.iloc[-5:-1].min())
    price=float(c.iloc[-1]); vol_ok=float(v.iloc[-1])>1.3
    if pole_pct>3.0 and consol_tight and price>ch and vol_ok and rsi<80:
        return _s("BUY",int(min(88,68+pole_pct*2)),f"Bull flag Pole={pole_pct:.1f}% vol={v.iloc[-1]:.1f}x")
    if pole_pct<-3.0 and consol_tight and price<cl and vol_ok and rsi>20:
        return _s("SELL",int(min(86,66+abs(pole_pct)*2)),f"Bear flag Pole={pole_pct:.1f}%")
    return _s("HOLD",0,"")

def s_relative_strength(df, nifty_returns: pd.Series):
    if len(df)<22 or nifty_returns.empty: return _s("HOLD",0,"")
    c=df["Close"].squeeze()
    lb=min(20,len(c)-1,len(nifty_returns)-1)
    if lb<5: return _s("HOLD",0,"")
    stock_ret=float(c.iloc[-1])/float(c.iloc[-lb])-1
    nifty_ret=float((1+nifty_returns.iloc[-lb:]).prod())-1
    rs=stock_ret-nifty_ret
    price=float(c.iloc[-1]); ema50=float(df["ema50"].iloc[-1])
    rsi=float(df["rsi"].iloc[-1]); adx=float(df["adx"].iloc[-1])
    if rs>0.05 and price>ema50 and 45<rsi<75 and adx>18:
        return _s("BUY",int(min(88,68+rs*200)),f"RS+{rs*100:.1f}% vs Nifty")
    if rs<-0.05 and price<ema50 and rsi<55:
        return _s("SELL",int(min(84,64+abs(rs)*200)),f"RS{rs*100:.1f}% vs Nifty")
    return _s("HOLD",0,"")

def s_inside_bar_nr7(df):
    if len(df)<10: return _s("HOLD",0,"")
    h=df["High"]; l=df["Low"]; c=df["Close"]
    rsi=float(df["rsi"].iloc[-1]); vr=float(df["vol_ratio"].iloc[-1])
    adx=float(df["adx"].iloc[-1])
    is_inside=(float(h.iloc[-1])<float(h.iloc[-2]) and float(l.iloc[-1])>float(l.iloc[-2]))
    is_nr7=bool(df["nr7"].iloc[-1]) if "nr7" in df.columns else False
    ph=float(h.iloc[-2]); pl=float(l.iloc[-2])
    price=float(c.iloc[-1]); up=float(df["ema21"].iloc[-1])>float(df["ema50"].iloc[-1])
    if (is_inside or is_nr7) and price>ph and vr>1.3 and up and rsi<75:
        tag="Inside bar" if is_inside else "NR7"
        return _s("BUY",int(min(85,65+vr*5+adx*0.3)),f"{tag} breakout ₹{ph:.1f} vol={vr:.1f}x")
    if (is_inside or is_nr7) and price<pl and vr>1.3 and not up and rsi>25:
        tag="Inside bar" if is_inside else "NR7"
        return _s("SELL",int(min(83,63+vr*5+adx*0.3)),f"{tag} breakdown ₹{pl:.1f}")
    return _s("HOLD",0,"")

def s_three_bar_reversal(df):
    if len(df)<8: return _s("HOLD",0,"")
    c=df["Close"]; rsi=df["rsi"]; vr=float(df["vol_ratio"].iloc[-1])
    three_red=(float(c.iloc[-4])>float(c.iloc[-3])>float(c.iloc[-2]))
    bar4_green=float(c.iloc[-1])>float(c.iloc[-2])
    bar4_mid=(float(c.iloc[-4])+float(c.iloc[-3]))/2
    bar4_strong=float(c.iloc[-1])>bar4_mid
    rsi_turn=float(rsi.iloc[-2])<42 and float(rsi.iloc[-1])>float(rsi.iloc[-2])
    if three_red and bar4_green and bar4_strong and rsi_turn and vr>1.4:
        return _s("BUY",int(min(88,68+vr*5+(42-float(rsi.iloc[-2]))*0.5)),
                  f"3-bar reversal RSI={rsi.iloc[-1]:.0f} {vr:.1f}x")
    three_green=(float(c.iloc[-4])<float(c.iloc[-3])<float(c.iloc[-2]))
    bar4_red=float(c.iloc[-1])<float(c.iloc[-2])
    bar4_mid_dn=(float(c.iloc[-4])+float(c.iloc[-3]))/2
    bar4_strong_dn=float(c.iloc[-1])<bar4_mid_dn
    rsi_turn_dn=float(rsi.iloc[-2])>58 and float(rsi.iloc[-1])<float(rsi.iloc[-2])
    if three_green and bar4_red and bar4_strong_dn and rsi_turn_dn and vr>1.4:
        return _s("SELL",int(min(86,66+vr*5+(float(rsi.iloc[-2])-58)*0.5)),
                  f"3-bar top RSI={rsi.iloc[-1]:.0f} {vr:.1f}x")
    return _s("HOLD",0,"")


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
                sig,conf,reason=fn(df,nifty_ret if nifty_ret is not None else pd.Series(dtype=float))
            elif k in NEEDS_MODE:
                sig,conf,reason=fn(df,mode)
            else:
                sig,conf,reason=fn(df)
            results[k]={"signal":sig,"confidence":conf,"reason":reason,"label":STRAT_LABELS[k]}
            if sig=="BUY":   bw+=w*(conf/100); triggers.append(f"✅ {STRAT_LABELS[k]} BUY ({conf}%)")
            elif sig=="SELL": sw+=w*(conf/100); triggers.append(f"🔴 {STRAT_LABELS[k]} SELL ({conf}%)")
            tw+=w
        except Exception:
            pass
    score=(bw-sw)/tw if tw else 0
    sig="BUY" if score>0.20 else ("SELL" if score<-0.20 else "HOLD")
    return {"signal":sig,"score":round(score,3),"strategies":results,"triggers":triggers,
            "n_buy":sum(1 for v in results.values() if v["signal"]=="BUY"),
            "n_sell":sum(1 for v in results.values() if v["signal"]=="SELL")}


def mtf_check_kite(symbol, primary_sig, enabled_keys, rw, nifty_ret, key, token, tok_map):
    try:
        df15=get_data_kite(symbol,"15minute",key,token,tok_map)
        if df15 is None: return True
        df15=add_features(df15)
        if df15.empty: return True
        r=run_strategies(df15,enabled_keys,"Intraday (15m)",rw,nifty_ret)
        return r["signal"]==primary_sig or r["signal"]=="HOLD"
    except:
        return True


def candle_patterns(df) -> list:
    o=float(df["Open"].iloc[-1]); h=float(df["High"].iloc[-1])
    l=float(df["Low"].iloc[-1]);  c=float(df["Close"].iloc[-1])
    c2=float(df["Close"].iloc[-2]); o2=float(df["Open"].iloc[-2])
    body=abs(c-o); rng=h-l; wu=h-max(o,c); wl=min(o,c)-l
    pats=[]
    if rng>0 and body/rng<0.10:               pats.append("Doji")
    if c2<o2 and c>o and c>o2 and o<c2:       pats.append("Bull Engulf")
    if c2>o2 and c<o and c<o2 and o>c2:       pats.append("Bear Engulf")
    if rng>0 and wl>2*body and wu<body*0.5:   pats.append("Hammer")
    if rng>0 and wu>2*body and wl<body*0.5:   pats.append("Shoot Star")
    if rng>0 and body/rng>0.85:               pats.append("Marubozu" if c>o else "Bear Marubozu")
    return pats


def week52(df) -> dict:
    n=min(252,len(df))
    hi=df["High"].rolling(n).max().iloc[-1]; lo=df["Low"].rolling(n).min().iloc[-1]
    p=df["Close"].iloc[-1]
    return {"hi52":round(hi,2),"lo52":round(lo,2),
            "pct_hi":round((p-hi)/hi*100,2),"pct_lo":round((p-lo)/lo*100,2),
            "near_hi":(p-hi)/hi*100>-5}


# ── AI Sentiment ──────────────────────────────────────────────
@st.cache_data(ttl=1800)
def ai_sentiment_batch(payload_json: str) -> dict:
    try:
        key=st.secrets.get("ANTHROPIC_API_KEY","")
        if not key: return {}
        client=anthropic.Anthropic(api_key=key)
        items=json.loads(payload_json)
        lines=[f'{it["ticker"]}: Rs{it["price"]:.1f} ({it["pct"]:+.1f}%) | {"; ".join(it["headlines"][:3]) or "No news"}'
               for it in items]
        prompt=("Senior NSE analyst. For each stock return JSON array only, no markdown:\n\n"
                +"\n".join(lines)
                +'\n\n[{"ticker":"X","score":<-1 to 1>,"label":"Bullish/Neutral/Bearish","confidence":<0-100>,"summary":"1 line"}]')
        r=client.messages.create(model="claude-sonnet-4-20250514",max_tokens=600,
                                  messages=[{"role":"user","content":prompt}])
        arr=json.loads(r.content[0].text.strip().replace("```json","").replace("```",""))
        return {a["ticker"]:{"score":max(-1,min(1,float(a.get("score",0)))),
                              "label":a.get("label","Neutral"),
                              "confidence":max(0,min(100,int(a.get("confidence",0)))),
                              "summary":a.get("summary","—")} for a in arr}
    except:
        return {}


# ── Position sizing ───────────────────────────────────────────
def pos_size(price, atr, capital, risk_pct, direction="BUY") -> dict:
    risk=capital*risk_pct
    sl   =round(price-atr,2) if direction=="BUY" else round(price+atr,2)
    tgt  =round(price+2*atr,2) if direction=="BUY" else round(price-2*atr,2)
    qty  =max(1,int(risk/max(atr,0.01))); qty=min(qty,int(capital*0.25/price))
    inv  =round(qty*price,2); gain=round(qty*2*atr,2); loss=round(qty*atr,2)
    brok =round(inv*0.0005*2,2)
    return {"qty":qty,"invest":inv,"sl":sl,"target":tgt,
            "pot_gain":gain,"pot_loss":loss,"brokerage":brok,
            "net_gain":round(gain-brok,2),"rr":"1:2"}


# ── Scan one stock ────────────────────────────────────────────
def scan_one(symbol, df_raw, mode, enabled_keys, rw,
             use_mtf, sent_cache, capital, risk_pct, nifty_ret,
             kite_key, access_token, tok_map) -> dict | None:
    # FIX BUG 6: explicit exception capture, not silent bare except
    error_detail = None
    try:
        df = add_features(df_raw)
        if df.empty or len(df)<50:
            return None
        tech = run_strategies(df, enabled_keys, mode, rw, nifty_ret)
        p    = float(df["Close"].iloc[-1])
        prev = float(df["Close"].iloc[-2])
        pct  = (p-prev)/prev*100
        atr  = float(df["atr"].iloc[-1])

        mtf_ok = True
        if use_mtf and tech["signal"] in ("BUY","SELL") and "Daily" in mode:
            mtf_ok = mtf_check_kite(symbol, tech["signal"], enabled_keys, rw,
                                    nifty_ret, kite_key, access_token, tok_map)

        cpats = candle_patterns(df)
        w52s  = week52(df)
        sent  = sent_cache.get(symbol, {"score":0,"label":"Neutral","confidence":0,"summary":"—"})
        sw    = st.session_state.sentiment_weight
        blended = (1-sw)*tech["score"] + sw*sent["score"]
        if not mtf_ok: blended *= 0.7
        final = "BUY" if blended>0.20 else ("SELL" if blended<-0.20 else "HOLD")
        position = pos_size(p,atr,capital,risk_pct,final) if final in ("BUY","SELL") and atr>0 else {}
        return {
            "ticker":symbol,"price":round(p,2),"change_pct":round(pct,2),"atr":round(atr,2),
            "tech_score":tech["score"],"tech_signal":tech["signal"],
            "strategies":tech["strategies"],"triggers":tech["triggers"],
            "n_buy":tech["n_buy"],"n_sell":tech["n_sell"],
            "sent_score":sent["score"],"sent_label":sent["label"],
            "sent_conf":sent["confidence"],"sent_summary":sent["summary"],
            "final_score":round(blended,3),"final_signal":final,
            "position":position,"mtf_ok":mtf_ok,"w52":w52s,"candles":cpats,
            "sector":get_sector(symbol),
            "cap_type":"Large Cap" if symbol in NIFTY100_SYM else "Mid Cap",
        }
    except Exception as e:
        st.session_state.scan_errors.append(f"{symbol}: scan_one exception — {e}")
        return None


# ── Order helpers ─────────────────────────────────────────────
def place_order(symbol, action, qty, price, sl, target, paper_mode=True) -> dict:
    ts=datetime.now().strftime("%H:%M:%S")
    if paper_mode:
        trade={"symbol":symbol,"action":action,"qty":qty,"entry":price,
               "sl":sl,"target":target,"status":"Open","pnl":0.0,"time":ts,"mode":"Paper"}
        st.session_state.paper_trades.append(trade)
        st.session_state.trade_log.append(trade.copy())
        st.session_state.orders_today+=1
        _telegram(f"📝 PAPER {action} {symbol}\nQty:{qty} @ ₹{price} SL:₹{sl} Tgt:₹{target}")
        return {"status":"paper","id":f"P-{int(time.time())}"}
    kite=get_kite()
    if not kite: return {"status":"error","error":"Not connected to Kite"}
    try:
        txn=kite.TRANSACTION_TYPE_BUY if action=="BUY" else kite.TRANSACTION_TYPE_SELL
        eid=kite.place_order(variety=kite.VARIETY_REGULAR,exchange=kite.EXCHANGE_NSE,
                             tradingsymbol=symbol,transaction_type=txn,quantity=qty,
                             product=kite.PRODUCT_MIS,order_type=kite.ORDER_TYPE_MARKET)
        time.sleep(0.8)
        sl_txn=kite.TRANSACTION_TYPE_SELL if action=="BUY" else kite.TRANSACTION_TYPE_BUY
        sl_trig=round(sl*0.998,2) if action=="BUY" else round(sl*1.002,2)
        slid=kite.place_order(variety=kite.VARIETY_REGULAR,exchange=kite.EXCHANGE_NSE,
                              tradingsymbol=symbol,transaction_type=sl_txn,quantity=qty,
                              product=kite.PRODUCT_MIS,order_type=kite.ORDER_TYPE_SL_M,
                              trigger_price=sl_trig,price=sl)
        tid=kite.place_order(variety=kite.VARIETY_REGULAR,exchange=kite.EXCHANGE_NSE,
                             tradingsymbol=symbol,transaction_type=sl_txn,quantity=qty,
                             product=kite.PRODUCT_MIS,order_type=kite.ORDER_TYPE_LIMIT,price=target)
        trade={"symbol":symbol,"action":action,"qty":qty,"entry":price,"sl":sl,"target":target,
               "status":"Open","pnl":0.0,"time":ts,"mode":"Live","sl_order":slid,"tgt_order":tid}
        st.session_state.trade_log.append(trade.copy())
        st.session_state.orders_today+=1
        _telegram(f"✅ LIVE {action} {symbol} Qty:{qty}")
        return {"status":"live","entry_id":eid,"sl_id":slid,"tgt_id":tid}
    except Exception as e:
        _telegram(f"❌ Order FAILED {symbol}: {e}")
        return {"status":"error","error":str(e)}


def square_off_all(paper_mode=True):
    if paper_mode:
        for t in st.session_state.paper_trades:
            if t["status"]=="Open": t["status"]="Squared Off"
        _telegram("📤 All paper positions squared off"); return
    kite=get_kite()
    if not kite: return
    try:
        for o in kite.orders():
            if o["status"] in ("OPEN","TRIGGER PENDING"):
                try: kite.cancel_order(variety=kite.VARIETY_REGULAR,order_id=o["order_id"])
                except: pass
        time.sleep(0.5)
        for p in kite.positions()["day"]:
            if p["quantity"]!=0:
                txn=kite.TRANSACTION_TYPE_SELL if p["quantity"]>0 else kite.TRANSACTION_TYPE_BUY
                kite.place_order(variety=kite.VARIETY_REGULAR,exchange=kite.EXCHANGE_NSE,
                                  tradingsymbol=p["tradingsymbol"],transaction_type=txn,
                                  quantity=abs(p["quantity"]),product=kite.PRODUCT_MIS,
                                  order_type=kite.ORDER_TYPE_MARKET)
    except Exception as e:
        st.error(f"Square off error: {e}")


def fetch_live_positions():
    kite=get_kite()
    if not kite: return [],0.0
    try:
        pos=kite.positions()["day"]
        return pos,round(sum(p.get("pnl",0) for p in pos),2)
    except: return [],0.0


def paper_pnl_mtm() -> float:
    total=0.0
    for t in st.session_state.paper_trades:
        if t["status"]!="Open": total+=t.get("pnl",0.0); continue
        ltp=get_ltp(t.get("symbol","")) or t["entry"]
        if t["action"]=="BUY":
            if ltp>=t["target"]: t["status"]="Target Hit"; t["pnl"]=round((t["target"]-t["entry"])*t["qty"],2)
            elif ltp<=t["sl"]:   t["status"]="SL Hit";     t["pnl"]=round((t["sl"]-t["entry"])*t["qty"],2)
            else:                                            t["pnl"]=round((ltp-t["entry"])*t["qty"],2)
        else:
            if ltp<=t["target"]: t["status"]="Target Hit"; t["pnl"]=round((t["entry"]-t["target"])*t["qty"],2)
            elif ltp>=t["sl"]:   t["status"]="SL Hit";     t["pnl"]=round((t["entry"]-t["sl"])*t["qty"],2)
            else:                                            t["pnl"]=round((t["entry"]-ltp)*t["qty"],2)
        total+=t["pnl"]
    return round(total,2)


def _telegram(msg: str):
    try:
        tok=st.secrets.get("TELEGRAM_TOKEN",""); cid=st.secrets.get("TELEGRAM_CHAT_ID","")
        if tok and cid:
            requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          data={"chat_id":cid,"text":msg},timeout=5)
    except: pass


# ╔══════════════════════════════════════════════════════════════╗
# ║                       SIDEBAR                               ║
# ╚══════════════════════════════════════════════════════════════╝
with st.sidebar:
    st.markdown('<span class="kite-badge">⚡ Kite Native v5.2</span>', unsafe_allow_html=True)
    st.header("⚙️ Settings")

    st.subheader("🔗 Zerodha Kite Connect")
    with st.expander("❓ How to authenticate",expanded=False):
        st.markdown("""
**Daily auth flow:**
1. `KITE_API_KEY` + `KITE_API_SECRET` in `.streamlit/secrets.toml`
2. Click **Login Zerodha** link below
3. Complete login + 2FA
4. Copy `request_token=XXXXX` from redirect URL
5. Paste in Step 2 box → Connect
⚠️ Token is one-time, re-auth each day.
        """)

    if not KITE_AVAILABLE:
        st.error("Run: `pip install kiteconnect`")
    else:
        st.session_state.paper_mode=st.toggle("📝 Paper Trade Mode",value=st.session_state.paper_mode)
        if not st.session_state.paper_mode:
            st.warning("⚠️ LIVE MODE — Real orders will be placed!")

        if is_connected():
            try: name=st.session_state.kite.profile().get("user_name","")
            except: name=""
            st.success(f"✅ Connected via Kite{' — '+name if name else ''}")

            # Show token status
            if st.session_state.tok_loaded:
                st.caption(f"🗄️ {len(st.session_state.tok_map):,} instrument tokens ready")
            else:
                if st.button("📥 Load Instrument Tokens"):
                    with st.spinner("Loading NSE instruments..."):
                        if load_instrument_tokens():
                            st.success(f"Loaded {len(st.session_state.tok_map):,} tokens")
                            st.rerun()

            if st.button("🔌 Disconnect"):
                st.session_state.kite=None; st.session_state.access_token=""
                st.session_state.tok_map={}; st.session_state.tok_loaded=False
                st.rerun()
        else:
            if "KITE_API_KEY" in st.secrets:
                kite_obj=kite_login_obj()
                if kite_obj:
                    st.markdown(f"**Step 1:** [Login Zerodha ↗]({kite_obj.login_url()})")
                    req_token=st.text_input("Step 2: Paste request_token",
                                            placeholder="Paste from redirect URL...")
                    if st.button("🔑 Connect",type="primary") and req_token.strip():
                        with st.spinner("Connecting..."):
                            if kite_set_token(kite_obj,req_token.strip()):
                                with st.spinner("Loading instrument tokens..."):
                                    load_instrument_tokens()
                                st.success(f"✅ Connected! {len(st.session_state.tok_map):,} tokens loaded")
                                st.rerun()
            else:
                st.info("Add `KITE_API_KEY` + `KITE_API_SECRET` to `.streamlit/secrets.toml`")

    st.divider()

    st.subheader("📊 Universe")
    universe_choice=st.radio("Scan scope",
                             ["All (Nifty 100 + Midcap 150)",
                              "Large Cap only (Nifty 100)",
                              "Mid Cap only (Midcap 150)"],index=0)
    if "Large Cap" in universe_choice:   UNIVERSE=NIFTY100_SYM
    elif "Mid Cap" in universe_choice:   UNIVERSE=MIDCAP150_SYM
    else:                                UNIVERSE=ALL_SYMBOLS
    st.caption(f"Scanning {len(UNIVERSE)} stocks")

    mode=st.selectbox("Timeframe",["Swing (Daily)","Intraday (15m)","Intraday (5m)"])
    kite_interval=KITE_INTERVAL[mode]
    st.divider()

    st.subheader("Strategies")
    st.caption("Original 10:")
    # FIX BUG 8: build enabled_keys and persist to session_state immediately
    enabled_keys=[]
    orig=["ORB","VWAP","EMA","MACD","BB","RSI","ST","Stoch","W52","Pivot"]
    new6=["HH_HL","OBV_DIV","FLAG","RS","IB","TBR"]
    cols=st.columns(2)
    for i,k in enumerate(orig):
        with cols[i%2]:
            if st.checkbox(STRAT_LABELS[k],value=True,key=f"s_{k}"):
                enabled_keys.append(k)
    st.caption("New high-accuracy strategies:")
    cols2=st.columns(2)
    for i,k in enumerate(new6):
        with cols2[i%2]:
            if st.checkbox(STRAT_LABELS[k],value=True,key=f"s_{k}"):
                enabled_keys.append(k)
    st.session_state.enabled_keys_store=enabled_keys   # persist for scan

    st.divider()
    use_mtf=st.toggle("📊 MTF Confluence",value=True)
    use_sentiment=st.toggle("🤖 AI Sentiment",value=False)
    sent_w=st.slider("Sentiment Weight",0.0,0.5,0.25,0.05,disabled=not use_sentiment)
    st.session_state.sentiment_weight=sent_w if use_sentiment else 0.0

    st.divider()
    st.session_state.capital=st.number_input("Capital (₹)",10000,500000,st.session_state.capital,5000)
    st.session_state.risk_per_trade=st.slider("Risk per Trade %",0.5,5.0,2.0,0.5)/100
    st.session_state.target_daily=st.number_input("Daily Target (₹)",500,10000,st.session_state.target_daily,500)
    st.session_state.max_trades_day=int(st.number_input("Max Trades/Day",1,20,st.session_state.max_trades_day,1))

    st.divider()
    min_strats=st.slider("Min Strategies Agreeing",1,10,3,1)
    min_score=st.slider("Min Score Threshold",0.20,0.70,0.35,0.05)
    only_52hi=st.checkbox("Only 52W Breakouts",False)
    only_mtf=st.checkbox("Only MTF Confirmed",False)
    cap_filter=st.multiselect("Cap Type Filter",["Large Cap","Mid Cap"],default=["Large Cap","Mid Cap"])
    auto_ref=st.checkbox("⏱️ Auto Refresh (5 min)")

    CAPITAL=st.session_state.capital; RISK_PER_TRADE=st.session_state.risk_per_trade
    TARGET_DAILY=st.session_state.target_daily
    gain_pt=int(CAPITAL*RISK_PER_TRADE*2)
    st.caption(f"Risk/trade: ₹{int(CAPITAL*RISK_PER_TRADE):,} | 1:2 gain: ₹{gain_pt:,}")
    st.caption(f"Need {max(1,int(TARGET_DAILY/gain_pt))} wins for ₹{TARGET_DAILY:,}")

    if st.button("📤 Square Off All",type="secondary",use_container_width=True):
        square_off_all(st.session_state.paper_mode); st.success("All positions squared off!")


# ╔══════════════════════════════════════════════════════════════╗
# ║                        MAIN UI                              ║
# ╚══════════════════════════════════════════════════════════════╝
st.title("📈 NSE Pro Trader v5.2 — Kite Connect Native")

if not is_connected():
    st.warning("⚠️ **Not connected to Kite Connect.** Authenticate in the sidebar.")
    st.info("All market data is sourced from Zerodha Kite Connect (yfinance removed).")
    st.stop()

if not st.session_state.tok_loaded:
    with st.spinner("Loading NSE instrument tokens..."):
        load_instrument_tokens()
    if not st.session_state.tok_loaded:
        st.error("Failed to load instrument tokens. Check your Kite connection.")
        st.stop()

regime=market_regime_kite()
rw=regime_weights(regime)
rcss={"Bull":"regime-bull","Bear":"regime-bear","Sideways":"regime-side"}.get(regime,"regime-side")
pm=st.session_state.paper_mode
color_tag="#ffd600" if pm else "#ff1744"

col_r,col_m=st.columns([3,1])
with col_r:
    st.markdown(
        f"<span class='{rcss}'>🌐 Regime: {regime}</span> &nbsp; "
        f"<span style='color:{color_tag};font-weight:700;font-size:14px;'>"
        f"{'📝 PAPER MODE' if pm else '🔴 LIVE TRADING'}</span> &nbsp; "
        f"<span style='color:var(--color-text-secondary);font-size:13px;'>"
        f"Universe: {len(UNIVERSE)} stocks | Tokens: {len(st.session_state.tok_map):,}</span>",
        unsafe_allow_html=True)
with col_m:
    st.markdown(f"Orders: **{st.session_state.orders_today}** / {st.session_state.max_trades_day}")

st.divider()

# P&L header
def pnl_header():
    total_pnl=paper_pnl_mtm() if pm else fetch_live_positions()[1]
    c1,c2,c3,c4,c5=st.columns(5)
    c1.metric("💰 Session P&L",f"₹{total_pnl:+,.2f}")
    c2.metric("🎯 Daily Target",f"₹{TARGET_DAILY:,}")
    pct_done=min(100,int(abs(total_pnl)/TARGET_DAILY*100)) if TARGET_DAILY>0 else 0
    c3.metric("📊 Progress",f"{pct_done}%")
    open_t=sum(1 for t in st.session_state.paper_trades if t["status"]=="Open")
    c4.metric("📂 Open",open_t)
    c5.metric("📋 Trades Today",st.session_state.orders_today)
    st.progress(min(1.0,pct_done/100),text=f"₹{total_pnl:+.0f} / ₹{TARGET_DAILY:,}")

pnl_header()
st.divider()

col_a,col_b=st.columns([3,1])
with col_a:
    do_scan=st.button("🔍 Scan Universe",type="primary",use_container_width=True)
with col_b:
    # FIX BUG 7: use scan_ran flag, not results truthiness
    reuse=st.button("🔄 Re-filter Cache",use_container_width=True,
                    disabled=not st.session_state.scan_ran)

if do_scan:
    # FIX BUG 8: read from session_state, not local variable
    enabled_keys_run=st.session_state.enabled_keys_store
    if not enabled_keys_run:
        st.warning("Select at least one strategy."); st.stop()

    st.session_state.scan_errors=[]
    bar=st.progress(0,f"⚡ Fetching {len(UNIVERSE)} stocks via Kite...")

    data_cache=fetch_parallel_kite(UNIVERSE,kite_interval,workers=12)

    n_fetched=sum(1 for v in data_cache.values() if v is not None)
    bar.progress(0.35,f"✅ Got data for {n_fetched}/{len(UNIVERSE)} stocks. Fetching Nifty 50...")

    nifty_ret=get_nifty50_returns_kite()
    bar.progress(0.40,"🧠 Running strategies...")

    sent_cache={}
    if use_sentiment:
        bar.progress(0.42,"🤖 Batch AI sentiment...")
        valid=[s for s in UNIVERSE if data_cache.get(s) is not None]
        payload=[]
        for sym in valid:
            df0=data_cache[sym]
            if df0 is not None and len(df0)>2:
                p=float(df0["Close"].iloc[-1]); pv=float(df0["Close"].iloc[-2])
                payload.append({"ticker":sym,"price":p,"pct":(p-pv)/pv*100,
                                 "headlines":list(get_news(sym))})
        for i in range(0,len(payload),5):
            sent_cache.update(ai_sentiment_batch(json.dumps(payload[i:i+5])))

    kite_key   =st.secrets["KITE_API_KEY"]
    access_token=st.session_state.access_token
    tok_map    =st.session_state.tok_map

    results=[]
    for i,symbol in enumerate(UNIVERSE):
        bar.progress(0.45+(i+1)/len(UNIVERSE)*0.55,
                     f"Analysing {symbol} ({i+1}/{len(UNIVERSE)})...")
        df_raw=data_cache.get(symbol)
        if df_raw is None: continue
        res=scan_one(symbol,df_raw,mode,enabled_keys_run,rw,
                     use_mtf,sent_cache,CAPITAL,RISK_PER_TRADE,nifty_ret,
                     kite_key,access_token,tok_map)
        if not res or res["final_signal"]=="HOLD": continue

        n=res["n_buy"] if res["final_signal"]=="BUY" else res["n_sell"]
        score=abs(res["final_score"])
        if n<min_strats: continue
        if score<min_score: continue
        if only_52hi and not res["w52"]["near_hi"]: continue
        if only_mtf and not res["mtf_ok"]: continue
        if res["cap_type"] not in cap_filter: continue

        results.append(res)
        if score>0.50:
            pos=res["position"]
            _telegram(
                f"{'🟢' if res['final_signal']=='BUY' else '🔴'} "
                f"{res['final_signal']}: {res['ticker']} [{res['sector']}]\n"
                f"₹{res['price']} Score:{res['final_score']:.2f} MTF:{'✅' if res['mtf_ok'] else '⚠️'}\n"
                f"SL:₹{pos.get('sl','—')} Tgt:₹{pos.get('target','—')} Qty:{pos.get('qty','—')}"
            )

    bar.empty()
    st.session_state.scan_results=results
    st.session_state.scan_ts=datetime.now().strftime("%H:%M:%S")
    st.session_state.scan_ran=True   # FIX BUG 7

    # Show data fetch summary
    n_null=len(UNIVERSE)-n_fetched
    if n_null>0:
        st.caption(f"ℹ️ {n_null} symbols had no data (not in NSE instrument list or no history).")
    if st.session_state.scan_errors:
        with st.expander(f"⚠️ {len(st.session_state.scan_errors)} fetch/scan warnings"):
            for e in st.session_state.scan_errors[:30]:
                st.caption(e)
    st.rerun()

# FIX BUG 7: use scan_ran to gate display, not `results or reuse`
results=st.session_state.scan_results
if st.session_state.scan_ran or reuse:
    buys =sorted([r for r in results if r["final_signal"]=="BUY"],  key=lambda x:-x["final_score"])
    sells=sorted([r for r in results if r["final_signal"]=="SELL"], key=lambda x:x["final_score"])

    c1,c2,c3,c4,c5,c6,c7=st.columns(7)
    c1.metric("🟢 BUY",len(buys))
    c2.metric("🔴 SELL",len(sells))
    c3.metric("📊 Total",len(results))
    c4.metric("🌐 Regime",regime)
    lc=sum(1 for r in results if r["cap_type"]=="Large Cap")
    mc=sum(1 for r in results if r["cap_type"]=="Mid Cap")
    c5.metric("🏦 Large Cap",lc)
    c6.metric("📈 Mid Cap",mc)
    c7.metric("🕐 Scanned",st.session_state.scan_ts or "—")

    if not results:
        st.warning("No signals passed the current filters. Try: Min Strategies ≤ 2, Min Score ≤ 0.25")
        st.info("Tip: Check the warnings expander above for data fetch issues.")

    st.divider()
    tab1,tab2,tab3,tab4,tab5=st.tabs(["🟢 BUY","🔴 SELL","📋 Table","💰 Live P&L","📈 Analytics"])

    def order_btn(r):
        pos=r["position"]
        if not pos: return
        if st.session_state.orders_today>=st.session_state.max_trades_day:
            st.warning(f"Max {st.session_state.max_trades_day} trades/day reached."); return
        lbl=(f"{'📝 Paper' if pm else '🚀 LIVE'} {r['final_signal']} "
             f"{r['ticker']} Qty:{pos['qty']} @ ₹{r['price']} "
             f"→ SL:₹{pos['sl']} Tgt:₹{pos['target']}")
        if st.button(lbl,key=f"ord_{r['ticker']}_{r['final_signal']}",
                     type="secondary" if pm else "primary",use_container_width=True):
            res=place_order(r["ticker"],r["final_signal"],pos["qty"],r["price"],
                            pos["sl"],pos["target"],pm)
            if res.get("status") in ("paper","live"):
                st.success(f"✅ Order placed! ID:{res.get('id') or res.get('entry_id')}"); st.rerun()
            else:
                st.error(f"❌ {res.get('error')}")

    def render_cards(sig_list):
        if not sig_list: st.info("No signals for current filters."); return
        for r in sig_list:
            pos=r.get("position",{})
            n=r["n_buy"] if r["final_signal"]=="BUY" else r["n_sell"]
            ek=st.session_state.enabled_keys_store
            with st.expander(
                f"{'🟢' if r['final_signal']=='BUY' else '🔴'} **{r['ticker']}** "
                f"[{r['cap_type']}] ₹{r['price']} ({r['change_pct']:+.2f}%) | "
                f"Score:{r['final_score']:.3f} | {n}/{len(ek)} strats | "
                f"{'✅ MTF' if r.get('mtf_ok') else '⚠️ MTF'}"
            ):
                st.markdown(
                    f"<span class='sector-tag'>{r.get('sector','—')}</span>"
                    f"<span class='sector-tag'>{r.get('cap_type','—')}</span>"
                    f"<span class='kite-badge' style='font-size:10px;'>Kite</span>",
                    unsafe_allow_html=True)
                c1,c2,c3,c4=st.columns(4)
                c1.metric("Entry",f"₹{r['price']}")
                c2.metric("Target",f"₹{pos.get('target','—')}",f"+₹{pos.get('pot_gain','—')}")
                c3.metric("SL",f"₹{pos.get('sl','—')}",f"-₹{pos.get('pot_loss','—')}")
                c4.metric("Qty",pos.get("qty","—"),f"₹{pos.get('invest','—')}")
                c5,c6,c7,c8=st.columns(4)
                c5.metric("Net Gain",f"₹{pos.get('net_gain','—')}")
                c6.metric("ATR",f"₹{r['atr']}")
                c7.metric("52W Hi%",f"{r['w52']['pct_hi']:.1f}%" if r.get("w52") else "—")
                c8.metric("Sent",r["sent_label"])
                if r.get("candles"): st.caption("📊 "+" | ".join(r["candles"]))
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
            rows=[]
            ek=st.session_state.enabled_keys_store
            for r in sorted(results,key=lambda x:-abs(x["final_score"])):
                pos=r.get("position",{})
                n=r["n_buy"] if r["final_signal"]=="BUY" else r["n_sell"]
                rows.append({
                    "Stock":r["ticker"],"Type":r["cap_type"],"Sector":r["sector"],
                    "Price":r["price"],"Chg%":r["change_pct"],
                    "Signal":r["final_signal"],"Score":r["final_score"],
                    "MTF":"✅" if r.get("mtf_ok") else "⚠️",
                    "Strats":f"{n}/{len(ek)}","Sent":r["sent_label"],
                    "Target":pos.get("target","—"),"SL":pos.get("sl","—"),
                    "Qty":pos.get("qty","—"),"Net(₹)":pos.get("net_gain","—"),
                    "52W Hi%":r["w52"]["pct_hi"] if r.get("w52") else "—",
                    "Candles":", ".join(r.get("candles",[])[:2]),
                })
            df_t=pd.DataFrame(rows)
            def csig(v):
                if v=="BUY":  return "background-color:#1a4731;color:#00e676;font-weight:bold"
                if v=="SELL": return "background-color:#4a1010;color:#ff5252;font-weight:bold"
                return ""
            st.dataframe(df_t.style.map(csig,subset=["Signal"])
                                   .format({"Chg%":"{:+.2f}%","Score":"{:.3f}"}),
                         use_container_width=True,height=500)
            csv=io.BytesIO(); df_t.to_csv(csv,index=False)
            st.download_button("⬇️ Export CSV",csv.getvalue(),
                               f"signals_kite_{date.today()}.csv","text/csv")

    with tab4:
        st.subheader("💰 Live P&L Dashboard")
        if is_connected() and not pm:
            pos_list,live_pnl=fetch_live_positions()
            if pos_list:
                color="#00e676" if live_pnl>=0 else "#ff1744"
                st.markdown(f"**Live P&L: <span style='color:{color}'>₹{live_pnl:+,.2f}</span>**",
                            unsafe_allow_html=True)
                pos_df=pd.DataFrame([{"Symbol":p["tradingsymbol"],"Qty":p["quantity"],
                    "Avg":p.get("average_price",0),"LTP":p.get("last_price",0),
                    "P&L":p.get("pnl",0),"Value":p.get("value",0)}
                    for p in pos_list if p.get("quantity",0)!=0])
                if not pos_df.empty:
                    st.dataframe(pos_df.style.format({"Avg":"₹{:.2f}","LTP":"₹{:.2f}",
                                                       "P&L":"₹{:+.2f}","Value":"₹{:.2f}"}),
                                 use_container_width=True)
                if st.button("🔄 Refresh"): st.rerun()
            else:
                st.info("No open positions.")
        else:
            pnl_now=paper_pnl_mtm()
            color="#00e676" if pnl_now>=0 else "#ff1744"
            st.markdown(f"**Paper P&L: <span style='color:{color}'>₹{pnl_now:+,.2f}</span>**",
                        unsafe_allow_html=True)
            t_pct=min(100,int(abs(pnl_now)/TARGET_DAILY*100)) if TARGET_DAILY>0 else 0
            st.progress(max(0.0,min(1.0,t_pct/100)),text=f"{t_pct}% of ₹{TARGET_DAILY:,}")
            if st.session_state.paper_trades:
                rows=[{"Symbol":t.get("symbol",""),"Action":t.get("action",""),
                       "Entry":t.get("entry",0),"SL":t.get("sl",0),"Target":t.get("target",0),
                       "Qty":t.get("qty",0),"Status":t.get("status",""),
                       "P&L (₹)":t.get("pnl",0.0),"Time":t.get("time","")}
                      for t in st.session_state.paper_trades]
                pt_df=pd.DataFrame(rows)
                def pnl_c(v):
                    if v>0: return "color:#00e676;font-weight:600"
                    if v<0: return "color:#ff1744;font-weight:600"
                    return ""
                st.dataframe(pt_df.style.map(pnl_c,subset=["P&L (₹)"])
                             .format({"Entry":"₹{:.2f}","SL":"₹{:.2f}",
                                      "Target":"₹{:.2f}","P&L (₹)":"₹{:+.2f}"}),
                             use_container_width=True,height=360)
                c1,c2=st.columns(2)
                with c1:
                    if st.button("🔄 Refresh P&L"): st.rerun()
                with c2:
                    if st.button("🗑️ Clear Trades"):
                        st.session_state.paper_trades=[]; st.rerun()
                tl=io.BytesIO(); pt_df.to_csv(tl,index=False)
                st.download_button("⬇️ Export Log",tl.getvalue(),
                                   f"trades_{date.today()}.csv","text/csv")
            else:
                st.info("No paper trades yet.")

    with tab5:
        st.subheader("📈 Analytics")
        if results:
            ca,cb=st.columns(2)
            with ca:
                st.markdown("**Score Distribution**")
                st.bar_chart(pd.DataFrame({
                    "Ticker":[r["ticker"] for r in results],
                    "Score":[r["final_score"] for r in results]
                }).set_index("Ticker"))
            with cb:
                st.markdown("**Strategy Hit Count**")
                sc={}
                for r in results:
                    for k,v in r["strategies"].items():
                        if v["signal"] in ("BUY","SELL"):
                            sc[STRAT_LABELS[k]]=sc.get(STRAT_LABELS[k],0)+1
                if sc:
                    st.bar_chart(pd.DataFrame.from_dict(
                        dict(sorted(sc.items(),key=lambda x:-x[1])),orient="index",columns=["Hits"]))
            cc,cd=st.columns(2)
            with cc:
                st.markdown("**Sector Distribution**")
                sec_cnt={}
                for r in results: sec_cnt[r["sector"]]=sec_cnt.get(r["sector"],0)+1
                if sec_cnt:
                    st.bar_chart(pd.DataFrame.from_dict(sec_cnt,orient="index",columns=["Count"]))
            with cd:
                st.markdown("**Large Cap vs Mid Cap**")
                cap_cnt={"Large Cap":sum(1 for r in results if r["cap_type"]=="Large Cap"),
                         "Mid Cap":sum(1 for r in results if r["cap_type"]=="Mid Cap")}
                st.bar_chart(pd.DataFrame.from_dict(cap_cnt,orient="index",columns=["Count"]))

# Auto square-off 3:20 PM IST
now_ist=datetime.utcnow()+timedelta(hours=5,minutes=30)
if now_ist.hour==15 and now_ist.minute>=20 and st.session_state.orders_today>0:
    square_off_all(pm)
    _telegram(f"📤 Auto square-off 3:20 PM | P&L: ₹{paper_pnl_mtm():+.2f}")

if auto_ref:
    st.toast("Refreshing in 5 min..."); time.sleep(300); st.rerun()
