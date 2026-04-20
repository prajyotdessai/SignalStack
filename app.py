"""
NSE PRO TRADER v4.1 — ZERODHA INTEGRATED (FIXED)
=================================================
FIXES over v4.0:
  BUG 1 FIXED: KeyError on paper trades table — 't["ticker"]' vs 't["symbol"]'
               The place_order() stores key as "symbol" but the table read "ticker".
               Unified to "symbol" everywhere.
  BUG 2 FIXED: Regime showing "Unknown" — ^NSEI download was failing because
               MultiIndex columns weren't handled + added ^NSENIFTY50 fallback.
  BUG 3 FIXED: paper_mode variable referenced before assignment outside sidebar
               scope — moved to session_state with a safe default.
  BUG 4 FIXED: max_trades_day referenced in render_cards before defined in sidebar
               — wrapped in st.session_state.

HOW TO GET THE REQUEST TOKEN (Zerodha):
  1. Add KITE_API_KEY and KITE_API_SECRET to your .streamlit/secrets.toml
  2. Click "Click to Login Zerodha" link in the sidebar — it opens Zerodha login
  3. After you log in with your Zerodha ID+password+2FA, Zerodha redirects you to
     your registered redirect URL (e.g. https://127.0.0.1:5000/?request_token=XXXXX&...)
  4. Copy the value after "request_token=" from the redirect URL
  5. Paste that token in the sidebar "Step 2" box and click Connect
  NOTE: Request token is one-time use and expires in minutes. Get a fresh one each day.
"""

import streamlit as st
import yfinance as yf
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
    page_title="NSE Pro Trader v4.1",
    page_icon="📈",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.block-container { padding-top:1rem; }
.buy-badge  { background:#0d2e1a; border:1px solid #00e676;
              color:#00e676; border-radius:5px; padding:2px 10px; font-weight:700; }
.sell-badge { background:#2e0d0d; border:1px solid #ff1744;
              color:#ff1744; border-radius:5px; padding:2px 10px; font-weight:700; }
.regime-bull { background:#0d2e1a; border:1px solid #00e676;
               border-radius:6px; padding:4px 12px; color:#00e676; }
.regime-bear { background:#2e0d0d; border:1px solid #ff1744;
               border-radius:6px; padding:4px 12px; color:#ff1744; }
.regime-side { background:#1a1a0d; border:1px solid #ffd600;
               border-radius:6px; padding:4px 12px; color:#ffd600; }
div[data-testid="stExpander"] { border:1px solid #21262d !important; }
.stButton > button { font-weight:600; }
</style>
""", unsafe_allow_html=True)

# ╔══════════════════════════════════════════════════════════════╗
# ║                   NIFTY 100 UNIVERSE                        ║
# ╚══════════════════════════════════════════════════════════════╝
NIFTY100 = [
    "RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS",
    "HINDUNILVR.NS","ITC.NS","SBIN.NS","BHARTIARTL.NS","KOTAKBANK.NS",
    "LT.NS","AXISBANK.NS","ASIANPAINT.NS","MARUTI.NS","TITAN.NS",
    "SUNPHARMA.NS","ULTRACEMCO.NS","BAJFINANCE.NS","WIPRO.NS","HCLTECH.NS",
    "TATAMOTORS.NS","POWERGRID.NS","NTPC.NS","ONGC.NS","JSWSTEEL.NS",
    "TATASTEEL.NS","ADANIPORTS.NS","COALINDIA.NS","BAJAJFINSV.NS","DIVISLAB.NS",
    "DRREDDY.NS","CIPLA.NS","TECHM.NS","NESTLEIND.NS","BRITANNIA.NS",
    "GRASIM.NS","HINDALCO.NS","EICHERMOT.NS","BPCL.NS","INDUSINDBK.NS",
    "TATACONSUM.NS","APOLLOHOSP.NS","HEROMOTOCO.NS","BAJAJ-AUTO.NS","SBILIFE.NS",
    "HDFCLIFE.NS","ADANIENT.NS","VEDL.NS","PIDILITIND.NS","HAVELLS.NS",
    "NAUKRI.NS","MCDOWELL-N.NS","GODREJCP.NS","DMART.NS","SIEMENS.NS",
    "AMBUJACEM.NS","DABUR.NS","MARICO.NS","COLPAL.NS","BERGEPAINT.NS",
    "TORNTPHARM.NS","LUPIN.NS","AUBANK.NS","BANDHANBNK.NS","FEDERALBNK.NS",
    "IDFCFIRSTB.NS","PNB.NS","BANKBARODA.NS","CANBK.NS","UNIONBANK.NS",
    "INDIGO.NS","TRENT.NS","ZOMATO.NS","ADANIGREEN.NS","TATAPOWER.NS",
    "LICI.NS","GAIL.NS","IOC.NS","HINDPETRO.NS","RECLTD.NS",
    "PFC.NS","IRFC.NS","NHPC.NS","SJVN.NS","TORNTPOWER.NS",
    "AUROPHARMA.NS","ALKEM.NS","LAURUSLABS.NS","MPHASIS.NS","LTIM.NS",
    "PERSISTENT.NS","COFORGE.NS","KPITTECH.NS","ASHOKLEY.NS","TVSMOTOR.NS",
    "BALKRISIND.NS","CHOLAFIN.NS","LICHSGFIN.NS","MANAPPURAM.NS","ABCAPITAL.NS",
]

# ╔══════════════════════════════════════════════════════════════╗
# ║                   SESSION STATE INIT                        ║
# ╚══════════════════════════════════════════════════════════════╝
def ss(key, default):
    if key not in st.session_state:
        st.session_state[key] = default

ss("scan_results",   [])
ss("scan_ts",        None)
ss("kite",           None)
ss("access_token",   "")
ss("positions",      [])
ss("trade_log",      [])
ss("session_pnl",    0.0)
ss("paper_trades",   [])
ss("regime",         "Unknown")
ss("orders_today",   0)
ss("paper_mode",     True)        # FIX 3: safe default in session_state
ss("max_trades_day", 5)           # FIX 4: safe default in session_state
ss("capital",        50000)
ss("risk_per_trade", 0.02)
ss("target_daily",   1000)
ss("sentiment_weight", 0.0)

# ╔══════════════════════════════════════════════════════════════╗
# ║                  ZERODHA KITE HELPERS                       ║
# ╚══════════════════════════════════════════════════════════════╝
def kite_login():
    if not KITE_AVAILABLE:
        st.error("kiteconnect not installed. Run: pip install kiteconnect")
        return None
    try:
        api_key = st.secrets["KITE_API_KEY"]
        kite    = KiteConnect(api_key=api_key)
        return kite
    except Exception as e:
        st.error(f"Kite init error: {e}")
        return None

def kite_set_token(kite, request_token: str) -> bool:
    try:
        api_secret = st.secrets["KITE_API_SECRET"]
        data       = kite.generate_session(request_token, api_secret=api_secret)
        st.session_state.access_token = data["access_token"]
        kite.set_access_token(data["access_token"])
        st.session_state.kite = kite
        return True
    except Exception as e:
        st.error(f"Token error: {e}")
        return False

def is_connected() -> bool:
    return (st.session_state.kite is not None
            and st.session_state.access_token != "")

def yf_to_kite(ticker: str) -> str:
    sym = ticker.replace(".NS","")
    remap = {"BAJAJ-AUTO":"BAJAJ-AUTO","MCDOWELL-N":"MCDOWELL-N"}
    return remap.get(sym, sym)
import requests
ip = requests.get("https://api.ipify.org").text
st.sidebar.info(f"App IP: {ip}")
# ╔══════════════════════════════════════════════════════════════╗
# ║               ORDER PLACEMENT                               ║
# FIX 1: Unified trade dict key to "symbol" everywhere.        ║
# ╚══════════════════════════════════════════════════════════════╝
def place_order(symbol: str, action: str, qty: int,
                price: float, sl: float, target: float,
                paper_mode: bool = True) -> dict:
    kite_sym = yf_to_kite(symbol)
    ts       = datetime.now().strftime("%H:%M:%S")

    # FIX 1: all keys consistent — use "symbol" (not "ticker")
    if paper_mode:
        trade = {
            "symbol":   symbol,    # FIX: was sometimes "ticker"
            "action":   action,
            "qty":      qty,
            "entry":    price,
            "sl":       sl,
            "target":   target,
            "status":   "Open",
            "pnl":      0.0,
            "time":     ts,
            "mode":     "Paper",
        }
        st.session_state.paper_trades.append(trade)
        st.session_state.trade_log.append(trade.copy())
        st.session_state.orders_today += 1
        _telegram(f"📝 PAPER {action} {symbol}\nQty:{qty} @ ₹{price} | SL:₹{sl} | Tgt:₹{target}")
        return {"status":"paper", "id": f"PAPER-{int(time.time())}"}

    # LIVE ORDER
    kite = st.session_state.kite
    try:
        txn = (kite.TRANSACTION_TYPE_BUY if action=="BUY"
               else kite.TRANSACTION_TYPE_SELL)
        entry_id = kite.place_order(
            variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
            tradingsymbol=kite_sym, transaction_type=txn,
            quantity=qty, product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET,
        )
        time.sleep(0.8)

        sl_txn = (kite.TRANSACTION_TYPE_SELL if action=="BUY"
                  else kite.TRANSACTION_TYPE_BUY)
        sl_trigger = round(sl*0.998,2) if action=="BUY" else round(sl*1.002,2)

        sl_id = kite.place_order(
            variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
            tradingsymbol=kite_sym, transaction_type=sl_txn,
            quantity=qty, product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_SL_M,
            trigger_price=sl_trigger, price=sl,
        )
        tgt_id = kite.place_order(
            variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
            tradingsymbol=kite_sym, transaction_type=sl_txn,
            quantity=qty, product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_LIMIT, price=target,
        )
        trade = {
            "symbol":   symbol,    # FIX: consistent key
            "action":   action,
            "qty":      qty,
            "entry":    price,
            "sl":       sl,
            "target":   target,
            "status":   "Open",
            "pnl":      0.0,
            "time":     ts,
            "mode":     "Live",
            "sl_order": sl_id,
            "tgt_order":tgt_id,
        }
        st.session_state.trade_log.append(trade.copy())
        st.session_state.orders_today += 1
        _telegram(f"✅ LIVE {action} {symbol}\nEntry:{entry_id} SL:{sl_id} Tgt:{tgt_id}")
        return {"status":"live","entry_id":entry_id,"sl_id":sl_id,"tgt_id":tgt_id}
    except Exception as e:
        _telegram(f"❌ Order FAILED {symbol}: {e}")
        return {"status":"error","error":str(e)}

def square_off_all(paper_mode: bool = True):
    if paper_mode:
        for t in st.session_state.paper_trades:
            if t["status"] == "Open":
                t["status"] = "Squared Off"
        _telegram("📤 All paper positions squared off")
        return
    kite = st.session_state.kite
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
    kite = st.session_state.kite
    if not kite: return [], 0.0
    try:
        pos   = kite.positions()["day"]
        total = sum(p.get("pnl",0) for p in pos)
        return pos, round(total,2)
    except Exception:
        return [], 0.0

def paper_pnl_mtm() -> float:
    """Mark open paper trades to market using yfinance LTP."""
    total = 0.0
    for t in st.session_state.paper_trades:
        if t["status"] != "Open":
            total += t.get("pnl", 0.0)
            continue
        try:
            # FIX 1: use "symbol" key (was "ticker" in old buggy dict)
            sym = t.get("symbol", t.get("ticker",""))
            info = yf.Ticker(sym + ".NS").fast_info
            ltp  = float(getattr(info, "last_price", t["entry"]) or t["entry"])
        except Exception:
            ltp = t["entry"]

        if t["action"] == "BUY":
            if ltp >= t["target"]:
                t["status"] = "Target Hit"
                t["pnl"]    = round((t["target"] - t["entry"]) * t["qty"], 2)
            elif ltp <= t["sl"]:
                t["status"] = "SL Hit"
                t["pnl"]    = round((t["sl"] - t["entry"]) * t["qty"], 2)
            else:
                t["pnl"]    = round((ltp - t["entry"]) * t["qty"], 2)
        else:
            if ltp <= t["target"]:
                t["status"] = "Target Hit"
                t["pnl"]    = round((t["entry"] - t["target"]) * t["qty"], 2)
            elif ltp >= t["sl"]:
                t["status"] = "SL Hit"
                t["pnl"]    = round((t["entry"] - t["sl"]) * t["qty"], 2)
            else:
                t["pnl"]    = round((t["entry"] - ltp) * t["qty"], 2)
        total += t["pnl"]
    return round(total, 2)

# ╔══════════════════════════════════════════════════════════════╗
# ║                    TELEGRAM                                 ║
# ╚══════════════════════════════════════════════════════════════╝
def _telegram(msg: str):
    try:
        token   = st.secrets.get("TELEGRAM_TOKEN","")
        chat_id = st.secrets.get("TELEGRAM_CHAT_ID","")
        if token and chat_id:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          data={"chat_id":chat_id,"text":msg}, timeout=5)
    except Exception:
        pass

# ╔══════════════════════════════════════════════════════════════╗
# ║                    DATA FETCH                               ║
# ╚══════════════════════════════════════════════════════════════╝
@st.cache_data(ttl=300)
def get_data(ticker: str, interval: str, period: str):
    try:
        raw = yf.download(ticker, period=period, interval=interval,
                          auto_adjust=True, progress=False)
        if raw is None or raw.empty: return None
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        raw = raw.loc[:, ~raw.columns.duplicated()]
        df  = raw[[c for c in ["Open","High","Low","Close","Volume"] if c in raw.columns]].copy()
        df  = df[~df.index.duplicated(keep="last")].sort_index()
        return df if len(df) >= 50 else None
    except Exception:
        return None

def fetch_parallel(tickers, interval, period, workers=12):
    out = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(get_data, t, interval, period): t for t in tickers}
        for f in concurrent.futures.as_completed(futs):
            out[futs[f]] = f.result()
    return out

@st.cache_data(ttl=3600)
def get_news(ticker_clean: str) -> tuple:
    try:
        news = yf.Ticker(ticker_clean+".NS").news or []
        return tuple(n.get("title","") for n in news[:6] if n.get("title"))
    except Exception:
        return ()

# ╔══════════════════════════════════════════════════════════════╗
# ║            MARKET REGIME — FIX 2                            ║
# Try ^NSEI first, fallback to ^NSENIFTY50, then NIFTYBEES.NS  ║
# Also fixed MultiIndex handling that was causing silent None.  ║
# ╚══════════════════════════════════════════════════════════════╝
@st.cache_data(ttl=3600)
def market_regime() -> str:
    for symbol in ["^NSEI", "^NSENIFTY50", "NIFTYBEES.NS"]:
        try:
            n = yf.download(symbol, period="6mo", interval="1d",
                             auto_adjust=True, progress=False)
            if n is None or n.empty:
                continue
            # FIX 2: always flatten MultiIndex
            if isinstance(n.columns, pd.MultiIndex):
                n.columns = n.columns.get_level_values(0)
            n = n.loc[:, ~n.columns.duplicated()]
            if "Close" not in n.columns or len(n) < 50:
                continue

            c   = n["Close"].squeeze()
            h   = n["High"].squeeze()
            l   = n["Low"].squeeze()
            e20 = c.ewm(span=20).mean()
            e50 = c.ewm(span=50).mean()
            adx = ta.trend.ADXIndicator(h, l, c, window=14).adx().iloc[-1]

            if e20.iloc[-1] > e50.iloc[-1] and adx > 20:
                return "Bull"
            if e20.iloc[-1] < e50.iloc[-1] and adx > 20:
                return "Bear"
            return "Sideways"
        except Exception:
            continue
    return "Sideways"   # FIX 2: default to Sideways instead of "Unknown"

def regime_weights(regime: str) -> dict:
    if regime == "Bull":
        return {"ORB":0.12,"VWAP":0.10,"EMA":0.15,"MACD":0.16,"BB":0.12,
                "RSI":0.05,"ST":0.14,"Stoch":0.06,"W52":0.07,"Pivot":0.03}
    if regime == "Bear":
        return {"ORB":0.08,"VWAP":0.14,"EMA":0.10,"MACD":0.14,"BB":0.08,
                "RSI":0.14,"ST":0.12,"Stoch":0.10,"W52":0.03,"Pivot":0.07}
    return {"ORB":0.08,"VWAP":0.12,"EMA":0.08,"MACD":0.08,"BB":0.10,
            "RSI":0.16,"ST":0.08,"Stoch":0.12,"W52":0.06,"Pivot":0.12}

# ╔══════════════════════════════════════════════════════════════╗
# ║              FEATURE ENGINEERING                            ║
# ╚══════════════════════════════════════════════════════════════╝
def add_features(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) < 50: return pd.DataFrame()
    df = df.copy()
    c = df["Close"].squeeze(); h = df["High"].squeeze()
    l = df["Low"].squeeze();   v = df["Volume"].squeeze()
    df["Close"]=c; df["High"]=h; df["Low"]=l; df["Volume"]=v

    df["ema9"]  = ta.trend.EMAIndicator(c,9).ema_indicator()
    df["ema21"] = ta.trend.EMAIndicator(c,21).ema_indicator()
    df["ema50"] = ta.trend.EMAIndicator(c,50).ema_indicator()
    df["ema200"]= ta.trend.EMAIndicator(c,200).ema_indicator()
    df["rsi"]   = ta.momentum.RSIIndicator(c,14).rsi()
    df["atr"]   = ta.volatility.AverageTrueRange(h,l,c,14).average_true_range()

    if hasattr(df.index,'date'):
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
    df["body"]  = abs(c - df["Open"].squeeze())
    df["wick_u"]= h - c.clip(lower=df["Open"].squeeze())
    df["wick_l"]= c.clip(upper=df["Open"].squeeze()) - l
    return df.ffill().dropna()

# ╔══════════════════════════════════════════════════════════════╗
# ║                  10 STRATEGIES                              ║
# ╚══════════════════════════════════════════════════════════════╝
def _s(sig,conf,reason): return sig,int(min(95,max(0,conf))),reason

def s_orb(df,mode="Swing (Daily)"):
    if "Daily" in mode or "Swing" in mode: return _s("HOLD",0,"N/A daily")
    if len(df)<10: return _s("HOLD",0,"")
    oh=df["High"].iloc[:6].max(); ol=df["Low"].iloc[:6].min()
    p=df["Close"].iloc[-1]; vr=df["vol_ratio"].iloc[-1]; adx=df["adx"].iloc[-1]
    if p>oh and vr>1.2 and adx>18: return _s("BUY",65+vr*8,f"ORB break ₹{oh:.1f} {vr:.1f}x")
    if p<ol and vr>1.2 and adx>18: return _s("SELL",62+vr*8,f"ORB break ₹{ol:.1f} {vr:.1f}x")
    return _s("HOLD",0,"")

def s_vwap(df):
    if len(df)<20: return _s("HOLD",0,"")
    p=df["Close"].iloc[-1]; vw=df["vwap"].iloc[-1]; prev=df["Close"].iloc[-2]
    up=df["ema21"].iloc[-1]>df["ema50"].iloc[-1]; rsi=df["rsi"].iloc[-1]; d=abs(p-vw)/vw*100
    if up and d<0.6 and p>prev and 35<rsi<70: return _s("BUY",70+(0.6-d)*25,f"VWAP pb d={d:.2f}%")
    if not up and d<0.6 and p<prev and rsi>40: return _s("SELL",68+(0.6-d)*25,f"VWAP rej d={d:.2f}%")
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
    bull_flip = h.iloc[-1]>0 and h.iloc[-2]<=0 and adx>22
    bear_flip = h.iloc[-1]<0 and h.iloc[-2]>=0 and adx>22
    bull_cont = h.iloc[-1]>h.iloc[-2] and df["macd"].iloc[-1]>df["macd_s"].iloc[-1] and adx>22
    bear_cont = h.iloc[-1]<h.iloc[-2] and df["macd"].iloc[-1]<df["macd_s"].iloc[-1] and adx>22
    if bull_flip or bull_cont: return _s("BUY",70+(adx-22)*0.5,f"MACD bull ADX={adx:.0f}")
    if bear_flip or bear_cont: return _s("SELL",68+(adx-22)*0.5,f"MACD bear ADX={adx:.0f}")
    return _s("HOLD",0,"")

def s_bb(df):
    if len(df)<30: return _s("HOLD",0,"")
    bw,p,vr=df["bb_w"],df["Close"].iloc[-1],df["vol_ratio"].iloc[-1]
    rm=bw.rolling(min(50,len(bw))).mean()
    sq=bw.iloc[-5:-1].mean()<rm.iloc[-1]*0.80
    if sq and p>df["bb_u"].iloc[-1] and vr>1.2: return _s("BUY",68+vr*5,f"BB squeeze break vol={vr:.1f}x")
    if sq and p<df["bb_l"].iloc[-1] and vr>1.2: return _s("SELL",66+vr*5,f"BB squeeze break vol={vr:.1f}x")
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
    if not (c.iloc[-2]>dn.iloc[-2]) and (c.iloc[-1]>dn.iloc[-1]):
        return _s("BUY",70+adx*0.4,f"ST flip bull SL₹{dn.iloc[-1]:.1f}")
    if not (c.iloc[-2]<up.iloc[-2]) and (c.iloc[-1]<up.iloc[-1]):
        return _s("SELL",68+adx*0.4,f"ST flip bear SL₹{up.iloc[-1]:.1f}")
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
    if p>hi*1.001 and vr>1.5 and 50<rsi<80:
        return _s("BUY",75+vr*5,f"52W HI break ₹{hi:.1f} {vr:.1f}x")
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

STRAT_FNS = {
    "ORB":s_orb,"VWAP":s_vwap,"EMA":s_ema,"MACD":s_macd,
    "BB":s_bb,"RSI":s_rsi,"ST":s_st,"Stoch":s_stoch,"W52":s_w52,"Pivot":s_pivot,
}
STRAT_LABELS = {
    "ORB":"ORB Breakout","VWAP":"VWAP Pullback","EMA":"EMA Momentum",
    "MACD":"MACD+ADX","BB":"BB Squeeze","RSI":"RSI Reversal",
    "ST":"SuperTrend","Stoch":"Stoch+EMA","W52":"52W Breakout","Pivot":"Pivot Bounce",
}

def run_strategies(df, enabled_keys, mode, rw) -> dict:
    bw=sw=tw=0.0; results={}; triggers=[]
    for k in enabled_keys:
        fn=STRAT_FNS[k]; w=rw.get(k,0.08)
        try:
            sig,conf,reason = fn(df,mode) if k=="ORB" else fn(df)
            results[k]={"signal":sig,"confidence":conf,"reason":reason,"label":STRAT_LABELS[k]}
            if sig=="BUY":  bw+=w*(conf/100); triggers.append(f"✅ {STRAT_LABELS[k]} BUY ({conf}%)")
            elif sig=="SELL": sw+=w*(conf/100); triggers.append(f"🔴 {STRAT_LABELS[k]} SELL ({conf}%)")
            tw+=w
        except Exception:
            pass
    score=(bw-sw)/tw if tw else 0
    sig="BUY" if score>0.20 else ("SELL" if score<-0.20 else "HOLD")
    return {"signal":sig,"score":round(score,3),"strategies":results,"triggers":triggers,
            "n_buy":sum(1 for v in results.values() if v["signal"]=="BUY"),
            "n_sell":sum(1 for v in results.values() if v["signal"]=="SELL")}

def mtf_check(ticker, primary_sig, enabled_keys, rw) -> bool:
    try:
        df15=get_data(ticker,"15m","60d")
        if df15 is None: return True
        df15=add_features(df15)
        if df15.empty: return True
        r=run_strategies(df15,enabled_keys,"Intraday (15m)",rw)
        return r["signal"]==primary_sig or r["signal"]=="HOLD"
    except Exception:
        return True

def candle_patterns(df) -> list:
    o,h,l,c=float(df["Open"].iloc[-1]),float(df["High"].iloc[-1]),float(df["Low"].iloc[-1]),float(df["Close"].iloc[-1])
    o2,c2=float(df["Open"].iloc[-2]),float(df["Close"].iloc[-2])
    body=abs(c-o); rng=h-l; wu=h-max(o,c); wl=min(o,c)-l
    pats=[]
    if rng>0 and body/rng<0.10: pats.append("Doji")
    if c2<o2 and c>o and c>o2 and o<c2: pats.append("🟢 Bull Engulf")
    if c2>o2 and c<o and c<o2 and o>c2: pats.append("🔴 Bear Engulf")
    if rng>0 and wl>2*body and wu<body*0.5: pats.append("🔨 Hammer")
    if rng>0 and wu>2*body and wl<body*0.5: pats.append("⭐ Shoot Star")
    if rng>0 and body/rng>0.85: pats.append("🟢 Marubozu" if c>o else "🔴 Marubozu")
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
    except Exception:
        return {}

# ╔══════════════════════════════════════════════════════════════╗
# ║                  POSITION SIZING                            ║
# ╚══════════════════════════════════════════════════════════════╝
def pos_size(price,atr,capital,risk_pct,direction="BUY") -> dict:
    risk=capital*risk_pct
    sl=round(price-atr,2) if direction=="BUY" else round(price+atr,2)
    tgt=round(price+2*atr,2) if direction=="BUY" else round(price-2*atr,2)
    qty=max(1,int(risk/max(atr,0.01))); qty=min(qty,int(capital*0.25/price))
    inv=round(qty*price,2); gain=round(qty*2*atr,2); loss=round(qty*atr,2)
    brok=round(inv*0.0005*2,2)
    return {"qty":qty,"invest":inv,"sl":sl,"target":tgt,
            "pot_gain":gain,"pot_loss":loss,"brokerage":brok,
            "net_gain":round(gain-brok,2),"rr":"1:2"}

# ╔══════════════════════════════════════════════════════════════╗
# ║                  FULL SCAN PIPELINE                         ║
# ╚══════════════════════════════════════════════════════════════╝
def scan_one(ticker, df_raw, mode, enabled_keys, rw,
             use_mtf, sent_cache, capital, risk_pct) -> dict | None:
    try:
        df=add_features(df_raw)
        if df.empty or len(df)<50: return None
        tech=run_strategies(df,enabled_keys,mode,rw)
        p=float(df["Close"].iloc[-1]); prev=float(df["Close"].iloc[-2])
        pct=(p-prev)/prev*100; atr=float(df["atr"].iloc[-1])
        tc=ticker.replace(".NS","")
        mtf_ok=True
        if use_mtf and tech["signal"] in ("BUY","SELL") and "Daily" in mode:
            mtf_ok=mtf_check(ticker,tech["signal"],enabled_keys,rw)
        cpats=candle_patterns(df); w52s=week52(df)
        sent=sent_cache.get(tc,{"score":0,"label":"Neutral","confidence":0,"summary":"—"})
        sw=st.session_state.sentiment_weight
        blended=(1-sw)*tech["score"]+sw*sent["score"]
        if not mtf_ok: blended*=0.7
        final="BUY" if blended>0.20 else ("SELL" if blended<-0.20 else "HOLD")
        position=pos_size(p,atr,capital,risk_pct,final) if final in ("BUY","SELL") and atr>0 else {}
        return {
            "ticker":tc,"price":round(p,2),"change_pct":round(pct,2),"atr":round(atr,2),
            "tech_score":tech["score"],"tech_signal":tech["signal"],
            "strategies":tech["strategies"],"triggers":tech["triggers"],
            "n_buy":tech["n_buy"],"n_sell":tech["n_sell"],
            "sent_score":sent["score"],"sent_label":sent["label"],
            "sent_conf":sent["confidence"],"sent_summary":sent["summary"],
            "final_score":round(blended,3),"final_signal":final,
            "position":position,"mtf_ok":mtf_ok,"w52":w52s,"candles":cpats,
        }
    except Exception:
        return None

# ╔══════════════════════════════════════════════════════════════╗
# ║                       SIDEBAR                               ║
# FIX 3 & 4: paper_mode and max_trades_day stored in           ║
# session_state before being used anywhere in the main body.   ║
# ╚══════════════════════════════════════════════════════════════╝
with st.sidebar:
    st.header("⚙️ Settings")

    # ── Zerodha ─────────────────────────────────────────────
    st.subheader("🔗 Zerodha Kite Connect")

    # REQUEST TOKEN GUIDE
    with st.expander("❓ How to get Request Token", expanded=False):
        st.markdown("""
**Steps every trading day:**
1. Add to `secrets.toml`:
   ```
   KITE_API_KEY = "your_api_key"
   KITE_API_SECRET = "your_api_secret"
   ```
2. Click **"Click to Login Zerodha"** below
3. Log in with Zerodha ID + password + 2FA
4. After login, Zerodha redirects to your app URL like:
   `https://yourapp.streamlit.app/?request_token=XXXXXXXX&action=login&status=success`
5. Copy the value after `request_token=` (everything between `=` and `&`)
6. Paste it in **Step 2** box and click **Connect**

⚠️ Token is **one-time use** and expires in ~2 minutes.
Get a fresh one each trading day.
        """)

    if not KITE_AVAILABLE:
        st.error("Run: `pip install kiteconnect`")
    else:
        st.session_state.paper_mode = st.toggle(
            "📝 Paper Trade Mode", value=st.session_state.paper_mode,
            help="Safe default. Disable only after thorough testing in paper mode."
        )
        if not st.session_state.paper_mode:
            st.warning("⚠️ LIVE MODE — Real orders will be placed!")

        if is_connected():
            profile_name = ""
            try:
                profile_name = st.session_state.kite.profile().get("user_name","")
            except Exception:
                pass
            st.success(f"✅ Connected{' — ' + profile_name if profile_name else ''}")
            if st.button("🔌 Disconnect"):
                st.session_state.kite=None
                st.session_state.access_token=""
                st.rerun()
        else:
            if "KITE_API_KEY" in st.secrets:
                kite_obj=kite_login()
                if kite_obj:
                    login_url=kite_obj.login_url()
                    st.markdown(f"**Step 1:** [Click to Login Zerodha ↗]({login_url})")
                    req_token=st.text_input(
                        "Step 2: Paste request_token here",
                        placeholder="Paste token from redirect URL...",
                        help="From redirect URL: ?request_token=XXXXX&..."
                    )
                    if st.button("🔑 Connect", type="primary") and req_token.strip():
                        with st.spinner("Connecting..."):
                            if kite_set_token(kite_obj, req_token.strip()):
                                st.success("✅ Connected!")
                                st.rerun()
            else:
                st.info("Add `KITE_API_KEY` + `KITE_API_SECRET` to `.streamlit/secrets.toml`")

    st.divider()
    mode=st.selectbox("Timeframe",["Swing (Daily)","Intraday (15m)","Intraday (5m)"])

    st.subheader("Strategies")
    enabled_keys=[]
    cols=st.columns(2)
    for i,k in enumerate(STRAT_FNS.keys()):
        with cols[i%2]:
            if st.checkbox(STRAT_LABELS[k],value=True,key=f"s_{k}"):
                enabled_keys.append(k)

    st.divider()
    use_mtf       = st.toggle("📊 MTF Confluence",value=True)
    use_sentiment = st.toggle("🤖 AI Sentiment",value=False)
    sent_w        = st.slider("Sentiment Weight",0.0,0.5,0.25,0.05,disabled=not use_sentiment)
    st.session_state.sentiment_weight = sent_w if use_sentiment else 0.0

    st.divider()
    st.session_state.capital        = st.number_input("Capital (₹)",10000,500000,
                                                       st.session_state.capital,5000)
    st.session_state.risk_per_trade = st.slider("Risk per Trade %",0.5,5.0,2.0,0.5)/100
    st.session_state.target_daily   = st.number_input("Daily Target (₹)",500,10000,
                                                       st.session_state.target_daily,500)
    st.session_state.max_trades_day = st.number_input("Max Trades/Day",1,20,
                                                       st.session_state.max_trades_day,1)

    st.divider()
    min_strats = st.slider("Min Strategies Agreeing",1,8,2,1)
    only_52hi  = st.checkbox("Only 52W High Breakouts",False)
    auto_ref   = st.checkbox("⏱️ Auto Refresh (5 min)")

    CAPITAL       = st.session_state.capital
    RISK_PER_TRADE= st.session_state.risk_per_trade
    TARGET_DAILY  = st.session_state.target_daily
    gain_pt       = int(CAPITAL*RISK_PER_TRADE*2)
    trades_needed = max(1,int(TARGET_DAILY/gain_pt)) if gain_pt>0 else 1
    st.caption(f"Risk/trade: ₹{int(CAPITAL*RISK_PER_TRADE):,} | Gain/trade: ₹{gain_pt:,}")
    st.caption(f"Need {trades_needed} winning trade(s) for ₹{TARGET_DAILY:,}")

    if st.button("📤 Square Off All", type="secondary", use_container_width=True):
        square_off_all(st.session_state.paper_mode)
        st.success("All positions squared off!")

# ╔══════════════════════════════════════════════════════════════╗
# ║                        MAIN UI                              ║
# ╚══════════════════════════════════════════════════════════════╝
st.title("📈 NSE Pro Trader v4.1 — Zerodha Integrated")

# Regime banner
regime = market_regime()
st.session_state.regime = regime
rw     = regime_weights(regime)
rcss   = {"Bull":"regime-bull","Bear":"regime-bear","Sideways":"regime-side"}.get(regime,"regime-side")
pm     = st.session_state.paper_mode   # FIX 3: always defined from session_state
mode_tag = "Paper" if pm else "LIVE"
color_tag= "#ffd600" if pm else "#ff1744"

col_r, col_m = st.columns([3,1])
with col_r:
    st.markdown(
        f"<span class='{rcss}'>🌐 Regime: {regime}</span> &nbsp; "
        f"<span style='color:{color_tag};font-weight:700;font-size:14px;'>"
        f"{'📝 PAPER MODE' if pm else '🔴 LIVE TRADING'}</span>",
        unsafe_allow_html=True
    )
with col_m:
    st.markdown(f"Orders today: **{st.session_state.orders_today}** / "
                f"{st.session_state.max_trades_day}")   # FIX 4: from session_state

st.divider()

# ── Live P&L header ───────────────────────────────────────────
def pnl_header():
    if is_connected() and not pm:
        pos, total_pnl = fetch_live_positions()
        st.session_state.positions = pos
    else:
        total_pnl = paper_pnl_mtm()
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("💰 Session P&L", f"₹{total_pnl:+,.2f}")
    c2.metric("🎯 Daily Target", f"₹{TARGET_DAILY:,}")
    pct_done = min(100, int(abs(total_pnl)/TARGET_DAILY*100)) if TARGET_DAILY>0 else 0
    c3.metric("📊 Target Progress", f"{pct_done}%")
    open_t = sum(1 for t in st.session_state.paper_trades if t["status"]=="Open")
    c4.metric("📂 Open Positions", open_t if pm else len([p for p in st.session_state.positions if p.get("quantity",0)!=0]))
    c5.metric("📋 Trades Today", st.session_state.orders_today)
    st.progress(min(1.0, pct_done/100), text=f"₹{total_pnl:+.0f} / ₹{TARGET_DAILY:,} target")
    return total_pnl

total_pnl = pnl_header()
st.divider()

# ── Scan buttons ──────────────────────────────────────────────
col_a, col_b = st.columns([3,1])
with col_a:
    do_scan = st.button("🔍 Scan Nifty 100", type="primary", use_container_width=True)
with col_b:
    reuse   = st.button("🔄 Re-filter Cache", use_container_width=True,
                        disabled=not st.session_state.scan_results)

if do_scan:
    if not enabled_keys:
        st.warning("Select at least one strategy."); st.stop()

    bar=st.progress(0,"⚡ Fetching data in parallel...")
    imap={"Intraday (5m)":"5m","Intraday (15m)":"15m","Swing (Daily)":"1d"}
    pmap={"Intraday (5m)":"60d","Intraday (15m)":"60d","Swing (Daily)":"2y"}
    data_cache=fetch_parallel(NIFTY100,imap[mode],pmap[mode])
    bar.progress(0.40,"✅ Data ready. Running strategies...")

    sent_cache={}
    if use_sentiment:
        bar.progress(0.42,"🤖 Fetching AI sentiment...")
        valid=[t for t in NIFTY100 if data_cache.get(t) is not None]
        payload=[]
        for ticker in valid:
            tc=ticker.replace(".NS",""); df0=data_cache[ticker]
            if df0 is not None and len(df0)>2:
                p=float(df0["Close"].iloc[-1]); pv=float(df0["Close"].iloc[-2])
                payload.append({"ticker":tc,"price":p,"pct":(p-pv)/pv*100,
                                 "headlines":list(get_news(tc))})
        for i in range(0,len(payload),5):
            sent_cache.update(ai_sentiment_batch(json.dumps(payload[i:i+5])))

    bar.progress(0.50,"🧠 Scoring strategies...")
    results=[]
    for i,ticker in enumerate(NIFTY100):
        bar.progress(0.50+(i+1)/len(NIFTY100)*0.50,
                     f"Analysing {ticker.replace('.NS','')} ({i+1}/{len(NIFTY100)})...")
        df_raw=data_cache.get(ticker)
        if df_raw is None: continue
        res=scan_one(ticker,df_raw,mode,enabled_keys,rw,use_mtf,
                     sent_cache,CAPITAL,RISK_PER_TRADE)
        if res and res["final_signal"] in ("BUY","SELL"):
            n=res["n_buy"] if res["final_signal"]=="BUY" else res["n_sell"]
            if n>=min_strats and (not only_52hi or res["w52"]["near_hi"]):
                results.append(res)
                if abs(res["final_score"])>0.45:
                    pos=res["position"]
                    _telegram(
                        f"{'🟢 BUY' if res['final_signal']=='BUY' else '🔴 SELL'}: {res['ticker']}\n"
                        f"₹{res['price']} Score:{res['final_score']:.2f} MTF:{'✅' if res['mtf_ok'] else '⚠️'}\n"
                        f"SL:₹{pos.get('sl','—')} Tgt:₹{pos.get('target','—')} Qty:{pos.get('qty','—')}\n"
                        f"Net:₹{pos.get('net_gain','—')} | {', '.join(res['candles']) or 'No candle pattern'}"
                    )
    bar.empty()
    st.session_state.scan_results=results
    st.session_state.scan_ts=datetime.now().strftime("%H:%M:%S")
    st.rerun()

results=st.session_state.scan_results
if results or reuse:
    buys =sorted([r for r in results if r["final_signal"]=="BUY"],  key=lambda x:-x["final_score"])
    sells=sorted([r for r in results if r["final_signal"]=="SELL"], key=lambda x:x["final_score"])

    c1,c2,c3,c4,c5,c6=st.columns(6)
    c1.metric("🟢 BUY",len(buys))
    c2.metric("🔴 SELL",len(sells))
    c3.metric("📊 Total",len(results))
    c4.metric("🌐 Regime",regime)
    c5.metric("🕐 Scanned",st.session_state.scan_ts or "—")
    c6.metric("📊 MTF OK",f"{sum(1 for r in results if r.get('mtf_ok'))}/{len(results)}")
    st.divider()

    tab1,tab2,tab3,tab4,tab5=st.tabs(["🟢 BUY","🔴 SELL","📋 Table","💰 Live P&L","📈 Analytics"])

    def order_btn(r):
        pos=r["position"]
        if not pos: return
        max_t = st.session_state.max_trades_day   # FIX 4
        if st.session_state.orders_today >= max_t:
            st.warning(f"Max {max_t} trades/day reached.")
            return
        lbl = (f"{'📝 Paper' if pm else '🚀 LIVE'} {r['final_signal']} "
               f"{r['ticker']} Qty:{pos['qty']} @ ₹{r['price']} "
               f"→ SL:₹{pos['sl']} Tgt:₹{pos['target']}")
        if st.button(lbl, key=f"ord_{r['ticker']}_{r['final_signal']}",
                     type="secondary" if pm else "primary",
                     use_container_width=True):
            result=place_order(
                symbol=r["ticker"], action=r["final_signal"],
                qty=pos["qty"], price=r["price"],
                sl=pos["sl"], target=pos["target"],
                paper_mode=pm,
            )
            if result.get("status") in ("paper","live"):
                st.success(f"✅ Order placed! ID: {result.get('id') or result.get('entry_id')}")
                st.rerun()
            else:
                st.error(f"❌ Order failed: {result.get('error')}")

    def render_cards(sig_list):
        if not sig_list:
            st.info("No signals. Try lowering Min Strategies or scanning again.")
            return
        for r in sig_list:
            pos=r.get("position",{})
            n=r["n_buy"] if r["final_signal"]=="BUY" else r["n_sell"]
            with st.expander(
                f"{'🟢' if r['final_signal']=='BUY' else '🔴'} **{r['ticker']}** "
                f"₹{r['price']} ({r['change_pct']:+.2f}%) | "
                f"Score:{r['final_score']:.3f} | {n} strats | "
                f"{'✅ MTF' if r.get('mtf_ok') else '⚠️ MTF'}"
            ):
                c1,c2,c3,c4=st.columns(4)
                c1.metric("Entry",  f"₹{r['price']}")
                c2.metric("Target", f"₹{pos.get('target','—')}", f"+₹{pos.get('pot_gain','—')}")
                c3.metric("SL",     f"₹{pos.get('sl','—')}",    f"-₹{pos.get('pot_loss','—')}")
                c4.metric("Qty",    pos.get("qty","—"),          f"₹{pos.get('invest','—')}")
                c5,c6,c7,c8=st.columns(4)
                c5.metric("Net Gain", f"₹{pos.get('net_gain','—')}")
                c6.metric("ATR",      f"₹{r['atr']}")
                c7.metric("52W Hi%",  f"{r['w52']['pct_hi']:.1f}%" if r.get('w52') else "—")
                c8.metric("Sent",     r["sent_label"])
                if r.get("candles"):
                    st.caption("📊 " + " | ".join(r["candles"]))
                st.markdown("**Strategies:**")
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
            for r in sorted(results,key=lambda x:-abs(x["final_score"])):
                pos=r.get("position",{})
                n=r["n_buy"] if r["final_signal"]=="BUY" else r["n_sell"]
                rows.append({
                    "Stock":r["ticker"],"Price":r["price"],"Chg%":r["change_pct"],
                    "Signal":r["final_signal"],"Score":r["final_score"],
                    "MTF":"✅" if r.get("mtf_ok") else "⚠️",
                    "Strats":f"{n}/{len(enabled_keys)}","Sentiment":r["sent_label"],
                    "Target":pos.get("target","—"),"SL":pos.get("sl","—"),
                    "Qty":pos.get("qty","—"),"NetGain(Rs)":pos.get("net_gain","—"),
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
                         use_container_width=True, height=500)
            csv_buf=io.BytesIO()
            df_t.to_csv(csv_buf,index=False)
            st.download_button("⬇️ Export CSV",csv_buf.getvalue(),
                               f"signals_{date.today()}.csv","text/csv")

    with tab4:
        st.subheader("💰 Live P&L Dashboard")
        if is_connected() and not pm:
            pos_list, live_pnl = fetch_live_positions()
            if pos_list:
                color="#00e676" if live_pnl>=0 else "#ff1744"
                st.markdown(f"**Live P&L: <span style='color:{color}'>₹{live_pnl:+,.2f}</span>**",
                            unsafe_allow_html=True)
                pos_df=pd.DataFrame([{
                    "Symbol":p["tradingsymbol"],"Qty":p["quantity"],
                    "Avg":p.get("average_price",0),"LTP":p.get("last_price",0),
                    "P&L":p.get("pnl",0),"Value":p.get("value",0),
                } for p in pos_list if p.get("quantity",0)!=0])
                if not pos_df.empty:
                    st.dataframe(pos_df.style.format(
                        {"Avg":"₹{:.2f}","LTP":"₹{:.2f}","P&L":"₹{:+.2f}","Value":"₹{:.2f}"}),
                        use_container_width=True)
                if st.button("🔄 Refresh P&L"): st.rerun()
            else:
                st.info("No open positions in Zerodha today.")
        else:
            pnl_now=paper_pnl_mtm()
            color="#00e676" if pnl_now>=0 else "#ff1744"
            st.markdown(f"**Paper Session P&L: <span style='color:{color}'>₹{pnl_now:+,.2f}</span>**",
                        unsafe_allow_html=True)
            t_pct=min(100,int(abs(pnl_now)/TARGET_DAILY*100)) if TARGET_DAILY>0 else 0
            st.progress(max(0.0,min(1.0,t_pct/100)), text=f"{t_pct}% of ₹{TARGET_DAILY:,} target")

            if st.session_state.paper_trades:
                # FIX 1: Use "symbol" key — consistent with place_order() dict
                rows=[{
                    "Symbol":   t.get("symbol", t.get("ticker","—")),   # handles both old & new
                    "Action":   t.get("action","—"),
                    "Entry":    t.get("entry",0),
                    "SL":       t.get("sl",0),
                    "Target":   t.get("target",0),
                    "Qty":      t.get("qty",0),
                    "Status":   t.get("status","—"),
                    "P&L (₹)":  t.get("pnl",0.0),
                    "Time":     t.get("time","—"),
                } for t in st.session_state.paper_trades]
                pt_df=pd.DataFrame(rows)
                def pnl_color(v):
                    if v>0: return "color:#00e676;font-weight:600"
                    if v<0: return "color:#ff1744;font-weight:600"
                    return ""
                st.dataframe(pt_df.style.map(pnl_color,subset=["P&L (₹)"])
                                        .format({"Entry":"₹{:.2f}","SL":"₹{:.2f}",
                                                 "Target":"₹{:.2f}","P&L (₹)":"₹{:+.2f}"}),
                             use_container_width=True, height=380)
                col_r1, col_r2 = st.columns(2)
                with col_r1:
                    if st.button("🔄 Refresh Paper P&L"): st.rerun()
                with col_r2:
                    if st.button("🗑️ Clear All Paper Trades"):
                        st.session_state.paper_trades=[]; st.rerun()
                tl_csv=io.BytesIO()
                pt_df.to_csv(tl_csv,index=False)
                st.download_button("⬇️ Export Trade Log",tl_csv.getvalue(),
                                   f"trades_{date.today()}.csv","text/csv")
            else:
                st.info("No paper trades yet. Place orders from the BUY/SELL tabs.")

    with tab5:
        st.subheader("📈 Analytics")
        if results:
            ca,cb=st.columns(2)
            with ca:
                st.markdown("**Score Distribution**")
                st.bar_chart(pd.DataFrame({"Ticker":[r["ticker"] for r in results],
                                           "Score":[r["final_score"] for r in results]}).set_index("Ticker"))
            with cb:
                st.markdown("**Strategy Hit Count**")
                sc={}
                for r in results:
                    for k,v in r["strategies"].items():
                        if v["signal"] in ("BUY","SELL"):
                            sc[STRAT_LABELS[k]]=sc.get(STRAT_LABELS[k],0)+1
                if sc:
                    st.bar_chart(pd.DataFrame.from_dict(sc,orient="index",columns=["Hits"]))
            st.markdown("**52-Week Proximity**")
            w52_rows=[{"Ticker":r["ticker"],"Signal":r["final_signal"],
                       "% from 52W Hi":r["w52"]["pct_hi"],"% from 52W Lo":r["w52"]["pct_lo"]}
                      for r in results if r.get("w52")]
            if w52_rows:
                st.dataframe(pd.DataFrame(w52_rows),use_container_width=True)

# ── Auto square-off at 3:20 PM IST ───────────────────────────
now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
if now_ist.hour==15 and now_ist.minute>=20 and st.session_state.orders_today>0:
    square_off_all(pm)
    _telegram(f"📤 Auto square-off 3:20 PM | P&L: ₹{paper_pnl_mtm():+.2f}")

if auto_ref:
    st.toast("Refreshing in 5 min...")
    time.sleep(300); st.rerun()
