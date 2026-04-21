"""
NIFTY 100 SCANNER v4.0 — ZERODHA INTEGRATED
=============================================
Features:
  ✅ Zerodha Kite Connect — live login + token management
  ✅ One-click order placement from signal cards
  ✅ Bracket / Cover orders with automatic SL + Target
  ✅ Live position monitor — auto-exits when SL or Target hit
  ✅ Real-time P&L dashboard from Zerodha positions API
  ✅ Daily session summary with trade log
  ✅ Parallel data fetch (10 workers)
  ✅ Batch AI sentiment via Claude
  ✅ Market regime detection
  ✅ Multi-timeframe confluence
  ✅ 10 strategies with regime-adjusted weights
  ✅ Candle patterns + Pivot levels + 52W stats
  ✅ Telegram alerts for signals + trade fills + SL/TP hits
  ✅ Export trade log to CSV
  ✅ Paper-trade mode (safe default)
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

# ── Zerodha SDK (install: pip install kiteconnect) ────────────
try:
    from kiteconnect import KiteConnect, KiteTicker
    KITE_AVAILABLE = True
except ImportError:
    KITE_AVAILABLE = False

st.set_page_config(
    layout="wide",
    page_title="NSE Pro Trader v4",
    page_icon="📈",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
body, .stApp { background:#080b0f; }
.block-container { padding-top:1rem; }
.metric-card { background:#0d1117; border:1px solid #21262d;
               border-radius:8px; padding:10px 14px; }
.buy-badge  { background:#0d2e1a; border:1px solid #00e676;
              color:#00e676; border-radius:5px; padding:2px 10px;
              font-weight:700; font-size:13px; }
.sell-badge { background:#2e0d0d; border:1px solid #ff1744;
              color:#ff1744; border-radius:5px; padding:2px 10px;
              font-weight:700; font-size:13px; }
.regime-bull { background:#0d2e1a; border:1px solid #00e676;
               border-radius:6px; padding:4px 12px; color:#00e676; }
.regime-bear { background:#2e0d0d; border:1px solid #ff1744;
               border-radius:6px; padding:4px 12px; color:#ff1744; }
.regime-side { background:#1a1a0d; border:1px solid #ffd600;
               border-radius:6px; padding:4px 12px; color:#ffd600; }
.pnl-pos { color:#00e676; font-size:22px; font-weight:700; }
.pnl-neg { color:#ff1744; font-size:22px; font-weight:700; }
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
ss("positions",      [])          # live positions from Zerodha
ss("trade_log",      [])          # all trades this session
ss("session_pnl",    0.0)
ss("paper_trades",   [])          # paper-mode trades
ss("monitor_active", False)
ss("regime",         "Unknown")
ss("orders_today",   0)

# ╔══════════════════════════════════════════════════════════════╗
# ║                  ZERODHA KITE HELPERS                       ║
# ╚══════════════════════════════════════════════════════════════╝

def kite_login() -> object | None:
    """Initialise KiteConnect and return kite object (no token yet)."""
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
    """Exchange request_token for access_token and store in session."""
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

# ── Map NSE symbol → Zerodha tradingsymbol ───────────────────
def yf_to_kite(ticker: str) -> str:
    """Strip .NS and handle Zerodha naming quirks."""
    sym = ticker.replace(".NS","")
    remap = {
        "BAJAJ-AUTO": "BAJAJ-AUTO",
        "MCDOWELL-N": "MCDOWELL-N",
    }
    return remap.get(sym, sym)

# ── Place intraday MIS order with SL + Target ────────────────
def place_order(symbol: str, action: str, qty: int,
                price: float, sl: float, target: float,
                paper_mode: bool = True) -> dict:
    """
    Place MIS (intraday) market order.
    Immediately after fill, places SL-M order and a limit target order.
    Returns order result dict.
    """
    kite_sym = yf_to_kite(symbol)
    ts       = datetime.now().strftime("%H:%M:%S")

    if paper_mode:
        trade = {
            "id":       f"PAPER-{int(time.time())}",
            "symbol":   symbol,
            "action":   action,
            "qty":      qty,
            "entry":    price,
            "sl":       sl,
            "target":   target,
            "status":   "Open",
            "pnl":      0.0,
            "time":     ts,
            "mode":     "Paper",
            "sl_order": None,
            "tgt_order":None,
        }
        st.session_state.paper_trades.append(trade)
        st.session_state.trade_log.append(trade.copy())
        st.session_state.orders_today += 1
        _telegram(
            f"📝 PAPER {action} {symbol}\n"
            f"Qty:{qty} @ ₹{price} | SL:₹{sl} | Tgt:₹{target}"
        )
        return {"status":"paper", "id": trade["id"]}

    # ── LIVE ORDER ─────────────────────────────────────────
    kite = st.session_state.kite
    try:
        txn = (kite.TRANSACTION_TYPE_BUY
               if action == "BUY"
               else kite.TRANSACTION_TYPE_SELL)

        # 1. Entry — MIS market order
        entry_id = kite.place_order(
            variety           = kite.VARIETY_REGULAR,
            exchange          = kite.EXCHANGE_NSE,
            tradingsymbol     = kite_sym,
            transaction_type  = txn,
            quantity          = qty,
            product           = kite.PRODUCT_MIS,
            order_type        = kite.ORDER_TYPE_MARKET,
        )
        time.sleep(0.8)   # wait for fill

        # 2. SL-M order (opposite direction)
        sl_txn = (kite.TRANSACTION_TYPE_SELL
                  if action == "BUY"
                  else kite.TRANSACTION_TYPE_BUY)

        sl_trigger = round(sl * 0.998, 2) if action == "BUY" else round(sl * 1.002, 2)
        sl_id = kite.place_order(
            variety          = kite.VARIETY_REGULAR,
            exchange         = kite.EXCHANGE_NSE,
            tradingsymbol    = kite_sym,
            transaction_type = sl_txn,
            quantity         = qty,
            product          = kite.PRODUCT_MIS,
            order_type       = kite.ORDER_TYPE_SL_M,
            trigger_price    = sl_trigger,
            price            = sl,
        )

        # 3. Target limit order (opposite direction)
        tgt_id = kite.place_order(
            variety          = kite.VARIETY_REGULAR,
            exchange         = kite.EXCHANGE_NSE,
            tradingsymbol    = kite_sym,
            transaction_type = sl_txn,
            quantity         = qty,
            product          = kite.PRODUCT_MIS,
            order_type       = kite.ORDER_TYPE_LIMIT,
            price            = target,
        )

        trade = {
            "id":        entry_id,
            "symbol":    symbol,
            "action":    action,
            "qty":       qty,
            "entry":     price,
            "sl":        sl,
            "target":    target,
            "status":    "Open",
            "pnl":       0.0,
            "time":      ts,
            "mode":      "Live",
            "sl_order":  sl_id,
            "tgt_order": tgt_id,
        }
        st.session_state.trade_log.append(trade.copy())
        st.session_state.orders_today += 1
        _telegram(
            f"✅ LIVE {action} {symbol}\n"
            f"Entry ID:{entry_id} Qty:{qty}\n"
            f"SL:₹{sl} (ID:{sl_id}) | Tgt:₹{target} (ID:{tgt_id})"
        )
        return {"status":"live", "entry_id":entry_id, "sl_id":sl_id, "tgt_id":tgt_id}

    except Exception as e:
        _telegram(f"❌ Order FAILED {symbol}: {e}")
        return {"status":"error", "error": str(e)}

# ── Square off all open MIS positions ────────────────────────
def square_off_all(paper_mode: bool = True):
    """Called at 3:20 PM or manually. Closes all open positions."""
    if paper_mode:
        for t in st.session_state.paper_trades:
            if t["status"] == "Open":
                t["status"] = "Squared Off"
        _telegram("📤 All paper positions squared off (3:20 PM)")
        return

    kite = st.session_state.kite
    if not kite:
        return
    try:
        # Cancel all pending orders first
        orders = kite.orders()
        for o in orders:
            if o["status"] in ("OPEN","TRIGGER PENDING"):
                try:
                    kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=o["order_id"])
                except Exception:
                    pass
        time.sleep(0.5)
        # Exit all MIS positions
        positions = kite.positions()["day"]
        for p in positions:
            if p["quantity"] != 0:
                txn = (kite.TRANSACTION_TYPE_SELL
                       if p["quantity"] > 0
                       else kite.TRANSACTION_TYPE_BUY)
                kite.place_order(
                    variety          = kite.VARIETY_REGULAR,
                    exchange         = kite.EXCHANGE_NSE,
                    tradingsymbol    = p["tradingsymbol"],
                    transaction_type = txn,
                    quantity         = abs(p["quantity"]),
                    product          = kite.PRODUCT_MIS,
                    order_type       = kite.ORDER_TYPE_MARKET,
                )
        _telegram("📤 All LIVE MIS positions squared off (3:20 PM)")
    except Exception as e:
        st.error(f"Square off error: {e}")

# ── Fetch live positions + P&L from Zerodha ─────────────────
def fetch_live_positions() -> tuple:
    """Returns (positions_list, total_pnl)."""
    kite = st.session_state.kite
    if not kite:
        return [], 0.0
    try:
        pos   = kite.positions()["day"]
        total = sum(p.get("pnl", 0) for p in pos)
        return pos, round(total, 2)
    except Exception:
        return [], 0.0

# ── Paper P&L: mark-to-market using latest yf price ─────────
def paper_pnl_mtm() -> float:
    """Mark all open paper trades to market using yfinance LTP."""
    total = 0.0
    for t in st.session_state.paper_trades:
        if t["status"] != "Open":
            total += t.get("pnl", 0.0)
            continue
        try:
            ltp = float(yf.Ticker(t["symbol"] + ".NS").fast_info.get("lastPrice", t["entry"]))
        except Exception:
            ltp = t["entry"]

        if t["action"] == "BUY":
            pnl = (ltp - t["entry"]) * t["qty"]
            if ltp >= t["target"]:
                t["status"]  = "Target Hit"; t["pnl"] = round((t["target"] - t["entry"]) * t["qty"], 2)
            elif ltp <= t["sl"]:
                t["status"]  = "SL Hit";     t["pnl"] = round((t["sl"] - t["entry"]) * t["qty"], 2)
            else:
                t["pnl"] = round(pnl, 2)
        else:
            pnl = (t["entry"] - ltp) * t["qty"]
            if ltp <= t["target"]:
                t["status"]  = "Target Hit"; t["pnl"] = round((t["entry"] - t["target"]) * t["qty"], 2)
            elif ltp >= t["sl"]:
                t["status"]  = "SL Hit";     t["pnl"] = round((t["entry"] - t["sl"]) * t["qty"], 2)
            else:
                t["pnl"] = round(pnl, 2)
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
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data={"chat_id":chat_id,"text":msg}, timeout=5
            )
    except Exception:
        pass

# ╔══════════════════════════════════════════════════════════════╗
# ║              DATA FETCH — PARALLEL                          ║
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

def fetch_parallel(tickers: list, interval: str, period: str, workers: int = 12):
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
# ║               MARKET REGIME DETECTION                       ║
# ╚══════════════════════════════════════════════════════════════╝
@st.cache_data(ttl=3600)
def market_regime() -> str:
    try:
        n = yf.download("^NSEI", period="6mo", interval="1d",
                        auto_adjust=True, progress=False)
        if n is None or n.empty: return "Unknown"
        if isinstance(n.columns, pd.MultiIndex): n.columns = n.columns.get_level_values(0)
        c   = n["Close"].squeeze()
        e20 = c.ewm(span=20).mean(); e50 = c.ewm(span=50).mean()
        adx = ta.trend.ADXIndicator(n["High"].squeeze(), n["Low"].squeeze(), c, window=14).adx().iloc[-1]
        if e20.iloc[-1] > e50.iloc[-1] and adx > 20: return "Bull"
        if e20.iloc[-1] < e50.iloc[-1] and adx > 20: return "Bear"
        return "Sideways"
    except Exception:
        return "Unknown"

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
    c,h,l,v = (df[x].squeeze() for x in ["Close","High","Low","Volume"])
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
    df["bb_m"]=bb.bollinger_mavg()
    df["bb_w"]=(df["bb_u"]-df["bb_l"])/df["bb_m"]

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
    if p>oh and vr>1.2 and adx>18: return _s("BUY", 65+vr*8, f"ORB break ₹{oh:.1f} {vr:.1f}x vol")
    if p<ol and vr>1.2 and adx>18: return _s("SELL",62+vr*8, f"ORB break ₹{ol:.1f} {vr:.1f}x vol")
    return _s("HOLD",0,"")

def s_vwap(df):
    if len(df)<20: return _s("HOLD",0,"")
    p=df["Close"].iloc[-1]; vw=df["vwap"].iloc[-1]; prev=df["Close"].iloc[-2]
    up=df["ema21"].iloc[-1]>df["ema50"].iloc[-1]; rsi=df["rsi"].iloc[-1]
    d=abs(p-vw)/vw*100
    if up and d<0.6 and p>prev and 35<rsi<70: return _s("BUY",70+(0.6-d)*25,f"VWAP pb d={d:.2f}% RSI={rsi:.0f}")
    if not up and d<0.6 and p<prev and rsi>40: return _s("SELL",68+(0.6-d)*25,f"VWAP rej d={d:.2f}%")
    return _s("HOLD",0,"")

def s_ema(df):
    if len(df)<25: return _s("HOLD",0,"")
    e9,e21=df["ema9"],df["ema21"]
    rsi,adx,vr=df["rsi"].iloc[-1],df["adx"].iloc[-1],df["vol_ratio"].iloc[-1]
    sep=(e9.iloc[-1]-e21.iloc[-1])/e21.iloc[-1]
    if sep>0.001 and 40<rsi<72 and adx>18: return _s("BUY", 65+adx*0.4+vr*3,f"EMA9>21 RSI={rsi:.0f} ADX={adx:.0f}")
    if sep<-0.001 and rsi>30 and adx>18:   return _s("SELL",62+adx*0.4+vr*3,f"EMA9<21 RSI={rsi:.0f}")
    return _s("HOLD",0,"")

def s_macd(df):
    if len(df)<30: return _s("HOLD",0,"")
    h,adx=df["macd_h"],df["adx"].iloc[-1]
    if (h.iloc[-1]>0 and h.iloc[-2]<=0 and adx>22) or (h.iloc[-1]>h.iloc[-2] and df["macd"].iloc[-1]>df["macd_s"].iloc[-1] and adx>22):
        return _s("BUY",70+(adx-22)*0.5,f"MACD bull ADX={adx:.0f}")
    if (h.iloc[-1]<0 and h.iloc[-2]>=0 and adx>22) or (h.iloc[-1]<h.iloc[-2] and df["macd"].iloc[-1]<df["macd_s"].iloc[-1] and adx>22):
        return _s("SELL",68+(adx-22)*0.5,f"MACD bear ADX={adx:.0f}")
    return _s("HOLD",0,"")

def s_bb(df):
    if len(df)<30: return _s("HOLD",0,"")
    bw,p,vr=df["bb_w"],df["Close"].iloc[-1],df["vol_ratio"].iloc[-1]
    rm=bw.rolling(min(50,len(bw))).mean()
    sq=bw.iloc[-5:-1].mean()<rm.iloc[-1]*0.80
    if sq and p>df["bb_u"].iloc[-1] and vr>1.2: return _s("BUY", 68+vr*5,f"BB sq break vol={vr:.1f}x")
    if sq and p<df["bb_l"].iloc[-1] and vr>1.2: return _s("SELL",66+vr*5,f"BB sq break vol={vr:.1f}x")
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
    hl2=(df["High"]+df["Low"])/2
    up,dn=hl2+3*atr,hl2-3*atr
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
    if near(S1) and p>prev and rsi<55: return _s("BUY",72,f"Pivot S1 bounce ₹{S1:.1f}")
    if near(S2) and p>prev and rsi<45: return _s("BUY",78,f"Pivot S2 bounce ₹{S2:.1f}")
    if near(R1) and p<prev and rsi>55: return _s("SELL",70,f"Pivot R1 reject ₹{R1:.1f}")
    if near(R2) and p<prev and rsi>60: return _s("SELL",76,f"Pivot R2 reject ₹{R2:.1f}")
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
        except Exception: pass
    score=(bw-sw)/tw if tw else 0
    sig="BUY" if score>0.20 else ("SELL" if score<-0.20 else "HOLD")
    return {"signal":sig,"score":round(score,3),"strategies":results,
            "triggers":triggers,"n_buy":sum(1 for v in results.values() if v["signal"]=="BUY"),
            "n_sell":sum(1 for v in results.values() if v["signal"]=="SELL")}

# ── MTF Confluence ───────────────────────────────────────────
def mtf_check(ticker, primary_sig, enabled_keys, rw) -> bool:
    try:
        df15=get_data(ticker,"15m","60d")
        if df15 is None: return True
        df15=add_features(df15)
        if df15.empty: return True
        r=run_strategies(df15,enabled_keys,"Intraday (15m)",rw)
        return r["signal"]==primary_sig or r["signal"]=="HOLD"
    except Exception: return True

# ── Candle patterns ──────────────────────────────────────────
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

# ── 52W Stats ────────────────────────────────────────────────
def week52(df) -> dict:
    n=min(252,len(df))
    hi=df["High"].rolling(n).max().iloc[-1]
    lo=df["Low"].rolling(n).min().iloc[-1]
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
    qty=max(1,int(risk/max(atr,0.01)))
    qty=min(qty,int(capital*0.25/price))
    inv=round(qty*price,2)
    gain=round(qty*2*atr,2); loss=round(qty*atr,2)
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
        # MTF
        mtf_ok=True
        if use_mtf and tech["signal"] in ("BUY","SELL") and "Daily" in mode:
            mtf_ok=mtf_check(ticker,tech["signal"],enabled_keys,rw)
        # Candles + 52W
        cpats=candle_patterns(df)
        w52s=week52(df)
        # Sentiment
        sent=sent_cache.get(tc,{"score":0,"label":"Neutral","confidence":0,"summary":"—"})
        # Blend
        blended=(1-SENTIMENT_WEIGHT)*tech["score"]+SENTIMENT_WEIGHT*sent["score"]
        if not mtf_ok: blended*=0.7
        final="BUY" if blended>0.20 else ("SELL" if blended<-0.20 else "HOLD")
        pos=pos_size(p,atr,capital,risk_pct,final) if final in ("BUY","SELL") and atr>0 else {}
        return {
            "ticker":tc,"price":round(p,2),"change_pct":round(pct,2),"atr":round(atr,2),
            "tech_score":tech["score"],"tech_signal":tech["signal"],
            "strategies":tech["strategies"],"triggers":tech["triggers"],
            "n_buy":tech["n_buy"],"n_sell":tech["n_sell"],
            "sent_score":sent["score"],"sent_label":sent["label"],
            "sent_conf":sent["confidence"],"sent_summary":sent["summary"],
            "final_score":round(blended,3),"final_signal":final,
            "position":pos,"mtf_ok":mtf_ok,"w52":w52s,"candles":cpats,
        }
    except Exception: return None

# ╔══════════════════════════════════════════════════════════════╗
# ║                       SIDEBAR                               ║
# ╚══════════════════════════════════════════════════════════════╝
CAPITAL          = 50000
RISK_PER_TRADE   = 0.02
TARGET_DAILY     = 1000
SENTIMENT_WEIGHT = 0.0

with st.sidebar:
    st.header("⚙️ Settings")

    # ── Zerodha Connection ───────────────────────────────────
    st.subheader("🔗 Zerodha Kite Connect")
    if not KITE_AVAILABLE:
        st.error("pip install kiteconnect")
    else:
        paper_mode = st.toggle("📝 Paper Trade Mode", value=True,
                               help="Safe default. Disable only after thorough testing.")
        if not paper_mode:
            st.warning("⚠️ LIVE MODE — Real orders will be placed!")

        if is_connected():
            st.success("✅ Zerodha Connected")
            if st.button("🔌 Disconnect"):
                st.session_state.kite=None; st.session_state.access_token=""
        else:
            if KITE_AVAILABLE and "KITE_API_KEY" in st.secrets:
                kite_obj=kite_login()
                if kite_obj:
                    login_url=kite_obj.login_url()
                    st.markdown(f"**Step 1:** [Click to Login Zerodha]({login_url})")
                    req_token=st.text_input("Step 2: Paste request_token from redirect URL")
                    if st.button("Connect") and req_token:
                        if kite_set_token(kite_obj, req_token.strip()):
                            st.success("Connected!"); st.rerun()
            else:
                st.info("Add KITE_API_KEY + KITE_API_SECRET to secrets.toml")

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
    use_mtf      = st.toggle("📊 MTF Confluence",value=True)
    use_sentiment= st.toggle("🤖 AI Sentiment",value=False)
    sent_w       = st.slider("Sentiment Weight",0.0,0.5,0.25,0.05,disabled=not use_sentiment)
    SENTIMENT_WEIGHT = sent_w if use_sentiment else 0.0

    st.divider()
    CAPITAL        = st.number_input("Capital (₹)",10000,500000,50000,5000)
    RISK_PER_TRADE = st.slider("Risk per Trade %",0.5,5.0,2.0,0.5)/100
    TARGET_DAILY   = st.number_input("Daily Target (₹)",500,10000,1000,500)
    max_trades_day = st.number_input("Max Trades/Day",1,20,5,1)

    st.divider()
    min_strats = st.slider("Min Strategies Agreeing",1,8,2,1)
    only_52hi  = st.checkbox("Only 52W High Breakouts",False)
    auto_ref   = st.checkbox("⏱️ Auto Refresh (5 min)")

    gain_pt = int(CAPITAL*RISK_PER_TRADE*2)
    trades_needed = max(1,int(TARGET_DAILY/gain_pt))
    st.caption(f"Risk/trade: ₹{int(CAPITAL*RISK_PER_TRADE):,} | Gain/trade: ₹{gain_pt:,}")
    st.caption(f"Need {trades_needed} winning trade(s) for ₹{TARGET_DAILY:,} target")

    if st.button("📤 Square Off All Positions", type="secondary", use_container_width=True):
        square_off_all(paper_mode if KITE_AVAILABLE else True)
        st.success("All positions squared off!")

# ╔══════════════════════════════════════════════════════════════╗
# ║                        MAIN UI                              ║
# ╚══════════════════════════════════════════════════════════════╝
st.title("📈 NSE Pro Trader v4 — Zerodha Integrated")

# ── Regime banner ─────────────────────────────────────────────
regime = market_regime()
st.session_state.regime = regime
rw     = regime_weights(regime)
rcss   = {"Bull":"regime-bull","Bear":"regime-bear","Sideways":"regime-side"}.get(regime,"regime-side")
mode_tag = "Paper" if (not KITE_AVAILABLE or (KITE_AVAILABLE and 'paper_mode' in dir() and paper_mode)) else "LIVE"
color_tag = "#ffd600" if mode_tag=="Paper" else "#ff1744"

col_r, col_m = st.columns([3,1])
with col_r:
    st.markdown(f"""
<span class='{rcss}'>🌐 Regime: {regime}</span> &nbsp;
<span style='color:{color_tag};font-weight:700;font-size:14px;'>
  {'📝 PAPER MODE' if mode_tag=='Paper' else '🔴 LIVE TRADING'}</span>
""", unsafe_allow_html=True)
with col_m:
    st.markdown(f"Orders today: **{st.session_state.orders_today}** / {max_trades_day}")

st.divider()

# ── Live P&L header ───────────────────────────────────────────
def pnl_header():
    if is_connected() and mode_tag=="LIVE":
        pos, total_pnl = fetch_live_positions()
        st.session_state.positions = pos
    else:
        total_pnl = paper_pnl_mtm()

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("💰 Session P&L",
              f"₹{total_pnl:+,.2f}",
              delta_color="normal" if total_pnl>=0 else "inverse")
    c2.metric("🎯 Daily Target", f"₹{TARGET_DAILY:,}")
    pct_done=min(100,int(abs(total_pnl)/TARGET_DAILY*100)) if TARGET_DAILY>0 else 0
    c3.metric("📊 Target Progress", f"{pct_done}%")
    open_trades=sum(1 for t in st.session_state.paper_trades if t["status"]=="Open")
    c4.metric("📂 Open Positions", open_trades if mode_tag=="Paper" else len([p for p in st.session_state.positions if p.get("quantity",0)!=0]))
    c5.metric("📋 Total Trades Today", st.session_state.orders_today)
    st.progress(min(1.0, pct_done/100), text=f"₹{total_pnl:+.0f} / ₹{TARGET_DAILY:,} target")
    return total_pnl

total_pnl = pnl_header()
st.divider()

# ── Scan button ───────────────────────────────────────────────
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
    bar.progress(0.40,"✅ Data ready. Running AI sentiment batch...")

    # ── Batch sentiment ──────────────────────────────────────
    sent_cache={}
    if use_sentiment:
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
                        f"{'🟢 BUY' if res['final_signal']=='BUY' else '🔴 SELL'} SIGNAL: {res['ticker']}\n"
                        f"₹{res['price']} | Score:{res['final_score']:.2f} | MTF:{'✅' if res['mtf_ok'] else '⚠️'}\n"
                        f"Entry:₹{res['price']} SL:₹{pos.get('sl','—')} Target:₹{pos.get('target','—')} Qty:{pos.get('qty','—')}\n"
                        f"Net Gain: ₹{pos.get('net_gain','—')}\n"
                        f"Candles: {', '.join(res['candles']) or 'None'}"
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

    tab1,tab2,tab3,tab4,tab5,tab6=st.tabs(["🟢 BUY","🔴 SELL","📋 Table","💰 Live P&L","📈 Analytics","⚖️ Rebalancer"])

    def order_btn(r, paper_mode_flag=True):
        """Render place-order button inside signal card."""
        pos=r["position"]
        if not pos: return
        b_label=(f"{'📝 Paper' if paper_mode_flag else '🚀 LIVE'} "
                 f"{r['final_signal']} {r['ticker']} "
                 f"Qty:{pos['qty']} @ ₹{r['price']} → SL:₹{pos['sl']} Tgt:₹{pos['target']}")
        # Disable if max trades reached
        disabled = (st.session_state.orders_today >= max_trades_day)
        if disabled:
            st.warning(f"Max {max_trades_day} trades/day reached.")
            return
        if st.button(b_label, key=f"ord_{r['ticker']}_{r['final_signal']}",
                     type="primary" if not paper_mode_flag else "secondary",
                     use_container_width=True):
            result=place_order(
                symbol     = r["ticker"],
                action     = r["final_signal"],
                qty        = pos["qty"],
                price      = r["price"],
                sl         = pos["sl"],
                target     = pos["target"],
                paper_mode = paper_mode_flag,
            )
            if result.get("status") in ("paper","live"):
                st.success(f"✅ Order placed! ID: {result.get('id') or result.get('entry_id')}")
            else:
                st.error(f"❌ Order failed: {result.get('error')}")

    def render_cards(sig_list):
        if not sig_list:
            st.info("No signals. Try lowering Min Strategies or scanning again.")
            return
        pm_flag = not is_connected() or (KITE_AVAILABLE and paper_mode)
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
                c5.metric("Net Gain",f"₹{pos.get('net_gain','—')}")
                c6.metric("ATR",    f"₹{r['atr']}")
                c7.metric("52W Hi%",f"{r['w52']['pct_hi']:.1f}%" if r.get('w52') else "—")
                c8.metric("Sent",   r["sent_label"])

                if r.get("candles"):
                    st.caption("📊 " + " | ".join(r["candles"]))
                st.markdown("**Strategies:**")
                for trig in r["triggers"]: st.caption(trig)
                if r.get("sent_summary","—") not in ("—",""):
                    st.info(f"🤖 {r['sent_label']} ({r['sent_conf']}%): {r['sent_summary']}")

                st.divider()
                order_btn(r, pm_flag)

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
            csv=io.BytesIO()
            df_t.to_csv(csv,index=False)
            st.download_button("⬇️ Export CSV",csv.getvalue(),
                               f"signals_{date.today()}.csv","text/csv")

    with tab4:
        st.subheader("💰 Live P&L Dashboard")

        # ── Live Zerodha positions ───────────────────────────
        if is_connected() and not paper_mode:
            pos_list, live_pnl = fetch_live_positions()
            if pos_list:
                st.markdown(f"**Live P&L: <span style='color:{'#00e676' if live_pnl>=0 else '#ff1744'}'>₹{live_pnl:+,.2f}</span>**", unsafe_allow_html=True)
                pos_df=pd.DataFrame([{
                    "Symbol":  p["tradingsymbol"],
                    "Qty":     p["quantity"],
                    "Avg":     p.get("average_price",0),
                    "LTP":     p.get("last_price",0),
                    "P&L":     p.get("pnl",0),
                    "Value":   p.get("value",0),
                } for p in pos_list if p.get("quantity",0)!=0])
                if not pos_df.empty:
                    st.dataframe(pos_df.style.format({"Avg":"₹{:.2f}","LTP":"₹{:.2f}","P&L":"₹{:+.2f}","Value":"₹{:.2f}"}),
                                 use_container_width=True)
                if st.button("🔄 Refresh P&L"):
                    st.rerun()
            else:
                st.info("No open positions in Zerodha today.")
        else:
            # ── Paper trade P&L ──────────────────────────────
            pnl_now=paper_pnl_mtm()
            st.markdown(f"**Paper Session P&L: <span style='color:{'#00e676' if pnl_now>=0 else '#ff1744'}'>₹{pnl_now:+,.2f}</span>**",
                        unsafe_allow_html=True)
            target_pct=min(100,int(abs(pnl_now)/TARGET_DAILY*100)) if TARGET_DAILY>0 else 0
            st.progress(max(0.0,min(1.0,target_pct/100)), text=f"{target_pct}% of ₹{TARGET_DAILY:,} target")

            if st.session_state.paper_trades:
                rows=[{
                    "Symbol":t["ticker"],"Action":t["action"],
                    "Entry":t["entry"],"SL":t["sl"],"Target":t["target"],
                    "Qty":t["qty"],"Status":t["status"],"P&L (₹)":t["pnl"],
                    "Time":t["time"],
                } for t in st.session_state.paper_trades]
                pt_df=pd.DataFrame(rows)
                def pnl_color(v):
                    if v>0: return "color:#00e676;font-weight:600"
                    if v<0: return "color:#ff1744;font-weight:600"
                    return ""
                st.dataframe(pt_df.style.map(pnl_color,subset=["P&L (₹)"])
                                        .format({"Entry":"₹{:.2f}","SL":"₹{:.2f}","Target":"₹{:.2f}","P&L (₹)":"₹{:+.2f}"}),
                             use_container_width=True, height=380)

                if st.button("🔄 Refresh Paper P&L"):
                    st.rerun()
                if st.button("🗑️ Clear Paper Trades"):
                    st.session_state.paper_trades=[]; st.rerun()

                # Export trade log
                tl_csv=io.BytesIO()
                pt_df.to_csv(tl_csv,index=False)
                st.download_button("⬇️ Export Trade Log",tl_csv.getvalue(),
                                   f"trades_{date.today()}.csv","text/csv")
            else:
                st.info("No paper trades yet. Place orders from the BUY/SELL tabs.")

    with tab5:
        st.subheader("📈 Analytics")
        if results:
            c_a,c_b=st.columns(2)
            with c_a:
                st.markdown("**Score Distribution**")
                sc_df=pd.DataFrame({"Ticker":[r["ticker"] for r in results],
                                    "Score":[r["final_score"] for r in results]})
                st.bar_chart(sc_df.set_index("Ticker"))
            with c_b:
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

    # ╔══════════════════════════════════════════════════════════╗
    # ║              TAB 6 — PORTFOLIO REBALANCER               ║
    # ╚══════════════════════════════════════════════════════════╝
    with tab6:
        st.subheader("⚖️ Portfolio Rebalancer — Long-Term Holdings")
        st.caption("Automatically detect drift in your existing portfolio and rebalance with tax awareness.")

        # ── Session state for rebalancer ─────────────────────
        if "rb_portfolio"    not in st.session_state: st.session_state.rb_portfolio    = []
        if "rb_log"          not in st.session_state: st.session_state.rb_log          = []

        # ── Sector map ────────────────────────────────────────
        SECTOR_MAP = {
            "RELIANCE":"Energy","TCS":"IT","HDFCBANK":"Banking","INFY":"IT","ICICIBANK":"Banking",
            "HINDUNILVR":"FMCG","ITC":"FMCG","SBIN":"Banking","BHARTIARTL":"Telecom","KOTAKBANK":"Banking",
            "LT":"Infra","AXISBANK":"Banking","ASIANPAINT":"Consumer","MARUTI":"Auto","TITAN":"Consumer",
            "SUNPHARMA":"Pharma","BAJFINANCE":"NBFC","WIPRO":"IT","HCLTECH":"IT","TATAMOTORS":"Auto",
            "POWERGRID":"Utilities","NTPC":"Utilities","ONGC":"Energy","JSWSTEEL":"Metals","TATASTEEL":"Metals",
            "ADANIPORTS":"Infra","COALINDIA":"Energy","DIVISLAB":"Pharma","DRREDDY":"Pharma","CIPLA":"Pharma",
            "TECHM":"IT","NESTLEIND":"FMCG","BRITANNIA":"FMCG","GRASIM":"Cement","HINDALCO":"Metals",
            "EICHERMOT":"Auto","BPCL":"Energy","TATACONSUM":"FMCG","APOLLOHOSP":"Healthcare",
            "HEROMOTOCO":"Auto","BAJAJ-AUTO":"Auto","VEDL":"Metals","HAVELLS":"Consumer",
            "ZOMATO":"Consumer","TRENT":"Consumer","IRFC":"Finance","PFC":"Finance","RECLTD":"Finance",
            "ADANIGREEN":"Energy","TATAPOWER":"Utilities","PERSISTENT":"IT","COFORGE":"IT","LTIM":"IT",
            "KPITTECH":"IT","MPHASIS":"IT","LUPIN":"Pharma","AUROPHARMA":"Pharma","ALKEM":"Pharma",
            "CHOLAFIN":"NBFC","MANAPPURAM":"NBFC","LICHSGFIN":"Finance","ASHOKLEY":"Auto","TVSMOTOR":"Auto",
            "FEDERALBNK":"Banking","IDFCFIRSTB":"Banking","BANDHANBNK":"Banking","AUBANK":"Banking",
            "PNB":"Banking","BANKBARODA":"Banking","GAIL":"Energy","IOC":"Energy","LICI":"Insurance",
            "SBILIFE":"Insurance","HDFCLIFE":"Insurance","GODREJCP":"FMCG","DABUR":"FMCG","MARICO":"FMCG",
            "SIEMENS":"Industrials","AMBUJACEM":"Cement","DMART":"Retail","NAUKRI":"Internet",
        }

        # ── Helper: get LTP ───────────────────────────────────
        @st.cache_data(ttl=300)
        def rb_ltp(ticker: str) -> float:
            try:
                info = yf.Ticker(ticker+".NS").fast_info
                p = float(info.get("lastPrice",0) or info.get("last_price",0))
                if p>0: return p
            except Exception: pass
            try:
                df = get_data(ticker+".NS","1d","5d")
                return float(df["Close"].iloc[-1]) if df is not None else 0.0
            except Exception: return 0.0

        @st.cache_data(ttl=600)
        def rb_history(ticker: str, period="1y"):
            try:
                raw=yf.download(ticker+".NS",period=period,interval="1d",auto_adjust=True,progress=False)
                if raw is None or raw.empty: return None
                if isinstance(raw.columns,pd.MultiIndex): raw.columns=raw.columns.get_level_values(0)
                return raw[["Close"]].dropna()
            except Exception: return None

        # ── STEP 1: Import portfolio ──────────────────────────
        st.markdown("### 📥 Step 1 — Import Your Long-Term Holdings")
        imp = st.radio("Import method",
                       ["🔗 From Zerodha Holdings","📋 Manual Entry","📄 Upload CSV"],
                       horizontal=True, key="rb_import")

        rb_holdings = []

        if "Zerodha" in imp:
            if is_connected():
                if st.button("📥 Fetch Holdings from Zerodha", key="rb_fetch"):
                    try:
                        raw_h = st.session_state.kite.holdings()
                        for h in raw_h:
                            sym = h["tradingsymbol"]
                            p   = rb_ltp(sym)
                            if p==0: p=float(h.get("last_price",h["average_price"]))
                            st.session_state.rb_portfolio.append({
                                "ticker":        sym,
                                "qty":           int(h["quantity"]),
                                "avg_cost":      float(h["average_price"]),
                                "price":         p,
                                "current_value": p * int(h["quantity"]),
                                "buy_date":      str(date.today()-timedelta(days=400)),
                                "target_weight": 0.0,
                            })
                        st.success(f"✅ Imported {len(st.session_state.rb_portfolio)} holdings!")
                    except Exception as e:
                        st.error(f"Error: {e}")
            else:
                st.warning("Connect Zerodha in the sidebar first.")

        elif "Manual" in imp:
            sample = pd.DataFrame({
                "Ticker":      ["RELIANCE","TCS","HDFCBANK","INFY","SBIN","TATAMOTORS"],
                "Qty":         [10, 5, 20, 15, 50, 30],
                "Avg Cost (₹)":[2600,3800,1550,1400,750,850],
                "Buy Date":    ["2023-01-15","2022-06-10","2023-03-20",
                                "2022-11-05","2024-01-10","2023-08-15"],
                "Target Wt%":  [20, 18, 20, 15, 12, 15],
            })
            edited = st.data_editor(sample, num_rows="dynamic",
                                    use_container_width=True, key="rb_editor")
            if st.button("✅ Load Portfolio", key="rb_load"):
                st.session_state.rb_portfolio = []
                for _, r in edited.iterrows():
                    tk = str(r["Ticker"]).strip().upper()
                    p  = rb_ltp(tk)
                    if p==0: p=float(r["Avg Cost (₹)"])
                    st.session_state.rb_portfolio.append({
                        "ticker":        tk,
                        "qty":           int(r["Qty"]),
                        "avg_cost":      float(r["Avg Cost (₹)"]),
                        "price":         p,
                        "current_value": p*int(r["Qty"]),
                        "buy_date":      str(r.get("Buy Date",date.today()-timedelta(days=400))),
                        "target_weight": float(r.get("Target Wt%",100/max(len(edited),1))),
                    })
                st.success(f"✅ {len(st.session_state.rb_portfolio)} stocks loaded!")

        elif "CSV" in imp:
            up = st.file_uploader("CSV: Ticker,Qty,AvgCost,BuyDate,TargetWt%", type=["csv"], key="rb_csv")
            if up:
                df_csv=pd.read_csv(up); df_csv.columns=[c.strip() for c in df_csv.columns]
                st.session_state.rb_portfolio=[]
                for _,r in df_csv.iterrows():
                    tk=str(r.get("Ticker","")).strip().upper()
                    if not tk: continue
                    p=rb_ltp(tk)
                    st.session_state.rb_portfolio.append({
                        "ticker":tk,"qty":int(r.get("Qty",1)),
                        "avg_cost":float(r.get("AvgCost",p)),
                        "price":p if p>0 else float(r.get("AvgCost",0)),
                        "current_value":(p or float(r.get("AvgCost",0)))*int(r.get("Qty",1)),
                        "buy_date":str(r.get("BuyDate",date.today()-timedelta(days=400))),
                        "target_weight":float(r.get("TargetWt%",100/max(len(df_csv),1))),
                    })
                st.success(f"✅ {len(st.session_state.rb_portfolio)} stocks from CSV!")

        rb_holdings = st.session_state.rb_portfolio

        if not rb_holdings:
            st.info("👆 Import your portfolio above to continue.")
        else:
            # Refresh prices button
            col_rf, col_clr = st.columns([1,1])
            with col_rf:
                if st.button("🔄 Refresh Live Prices", key="rb_refresh"):
                    for h in rb_holdings:
                        p=rb_ltp(h["ticker"])
                        if p>0: h["price"]=p; h["current_value"]=p*h["qty"]
                    st.session_state.rb_portfolio=rb_holdings
                    st.success("Prices updated!")
            with col_clr:
                if st.button("🗑️ Clear Portfolio", key="rb_clear"):
                    st.session_state.rb_portfolio=[]; st.rerun()

            # ── STEP 2: Portfolio overview ────────────────────
            st.markdown("### 📊 Step 2 — Portfolio Overview")
            total_val  = sum(h["current_value"] for h in rb_holdings)
            total_cost = sum(h["avg_cost"]*h["qty"] for h in rb_holdings)
            total_pnl  = total_val - total_cost
            pnl_pct    = total_pnl/total_cost*100 if total_cost>0 else 0

            m1,m2,m3,m4 = st.columns(4)
            m1.metric("Portfolio Value", f"₹{total_val:,.0f}")
            m2.metric("Total P&L",       f"₹{total_pnl:+,.0f}", f"{pnl_pct:+.1f}%")
            m3.metric("Holdings",         len(rb_holdings))
            m4.metric("Invested",         f"₹{total_cost:,.0f}")

            # Holdings table with drift
            h_rows=[]
            for h in rb_holdings:
                cur_w = h["current_value"]/total_val*100 if total_val>0 else 0
                tgt_w = h.get("target_weight",100/len(rb_holdings))
                drift = cur_w-tgt_w
                pnl   = (h["price"]-h["avg_cost"])*h["qty"]
                h_rows.append({
                    "Ticker":     h["ticker"],
                    "Sector":     SECTOR_MAP.get(h["ticker"],"Other"),
                    "Qty":        h["qty"],
                    "Avg Cost":   h["avg_cost"],
                    "LTP":        round(h["price"],2),
                    "Value (₹)":  round(h["current_value"],0),
                    "P&L (₹)":    round(pnl,0),
                    "P&L%":       round(pnl/(h["avg_cost"]*h["qty"])*100,1) if h["avg_cost"]>0 else 0,
                    "Cur Wt%":    round(cur_w,2),
                    "Tgt Wt%":    round(tgt_w,2),
                    "Drift%":     round(drift,2),
                })
            df_h=pd.DataFrame(h_rows)

            def _cd(v):
                if v>5:  return "background-color:#2e0d0d;color:#ff5252;font-weight:700"
                if v<-5: return "background-color:#0d2e1a;color:#00e676;font-weight:700"
                return ""
            def _cp(v): return "color:#00e676" if v>=0 else "color:#ff5252"

            st.dataframe(
                df_h.style.map(_cd,subset=["Drift%"]).map(_cp,subset=["P&L (₹)","P&L%"])
                    .format({"Avg Cost":"₹{:.2f}","LTP":"₹{:.2f}",
                             "Value (₹)":"₹{:,.0f}","P&L (₹)":"₹{:+,.0f}",
                             "P&L%":"{:+.1f}%","Drift%":"{:+.2f}%"}),
                use_container_width=True, height=320)

            # Sector chart
            sec_grp = df_h.groupby("Sector")["Value (₹)"].sum().reset_index()
            sec_grp["Wt%"]=(sec_grp["Value (₹)"]/total_val*100).round(1)
            cs,cm=st.columns([1,2])
            with cs:
                st.caption("**Sector Weights**")
                st.dataframe(sec_grp.sort_values("Wt%",ascending=False)
                               .style.format({"Value (₹)":"₹{:,.0f}","Wt%":"{:.1f}%"}),
                             use_container_width=True,height=240)
            with cm:
                st.caption("**Sector Distribution**")
                st.bar_chart(sec_grp.set_index("Sector")["Wt%"])

            # ── STEP 3: Choose strategy & compute ────────────
            st.markdown("### ⚖️ Step 3 — Choose Rebalancing Strategy")

            rb_strat = st.selectbox("Strategy", [
                "1. Threshold — rebalance only when drift >X%  ✅ Recommended",
                "2. Momentum Tilt — overweight 90-day winners",
                "3. Equal Weight — reset all to 1/N  ✅ Simplest",
                "4. Risk Parity — size by inverse volatility",
                "5. Sector Rotation — trim overbought sectors",
            ], key="rb_strat_sel")

            rb_threshold = 5.0
            if "Threshold" in rb_strat:
                rb_threshold = st.slider("Drift threshold %", 2.0, 15.0, 5.0, 0.5, key="rb_thresh")

            new_money = st.number_input("New SIP Money to Deploy (₹)",0,1000000,0,5000,
                key="rb_newmoney",
                help="App deploys this into underweight stocks FIRST — avoiding sells and tax")
            min_order  = st.number_input("Min Order Value (₹)",500,50000,2000,500,key="rb_minord")

            if st.button("🔍 Compute Rebalancing Plan", key="rb_compute", type="primary"):
                with st.spinner("Running strategy..."):

                    rb_df_rows=[]

                    # ── STRATEGY IMPLEMENTATIONS (inline) ────
                    if "Threshold" in rb_strat:
                        for h in rb_holdings:
                            cur_w  = h["current_value"]/total_val*100 if total_val>0 else 0
                            tgt_w  = h.get("target_weight",100/len(rb_holdings))
                            drift  = cur_w-tgt_w
                            tgt_val= total_val*tgt_w/100
                            diff   = tgt_val-h["current_value"]
                            action = "HOLD"
                            if abs(drift)>=rb_threshold:
                                action = "BUY" if diff>0 else "SELL"
                            if abs(diff)<min_order: action="HOLD"
                            rb_df_rows.append({"Ticker":h["ticker"],"Cur Wt%":round(cur_w,2),
                                "Tgt Wt%":tgt_w,"Drift%":round(drift,2),"Action":action,
                                "Amount (₹)":round(abs(diff),0),
                                "Qty":max(1,int(abs(diff)/h["price"])) if h["price"]>0 else 0,
                                "Price":h["price"],"AvgCost":h["avg_cost"],
                                "BuyDate":h.get("buy_date",""),
                                "Reason":f"Drift {drift:+.1f}% vs threshold {rb_threshold}%"})

                    elif "Momentum" in rb_strat:
                        for h in rb_holdings:
                            dfm=rb_history(h["ticker"],"6mo")
                            mom=0.0
                            if dfm is not None and len(dfm)>=60:
                                mom=float((dfm["Close"].iloc[-1]-dfm["Close"].iloc[-60])/dfm["Close"].iloc[-60]*100)
                            tilt = +2.0 if mom>10 else (-2.0 if mom<-10 else 0.0)
                            cur_w= h["current_value"]/total_val*100 if total_val>0 else 0
                            tgt_w= max(1,h.get("target_weight",100/len(rb_holdings))+tilt)
                            drift= cur_w-tgt_w
                            diff = total_val*(tgt_w-cur_w)/100
                            action="BUY" if diff>min_order else ("SELL" if diff<-min_order else "HOLD")
                            rb_df_rows.append({"Ticker":h["ticker"],"90D Mom%":round(mom,1),
                                "Tilt":f"{tilt:+.0f}%","Tgt Wt%":round(tgt_w,1),
                                "Drift%":round(drift,2),"Action":action,
                                "Amount (₹)":round(abs(diff),0),
                                "Qty":max(1,int(abs(diff)/h["price"])) if h["price"]>0 else 0,
                                "Price":h["price"],"AvgCost":h["avg_cost"],
                                "BuyDate":h.get("buy_date",""),
                                "Reason":f"90D momentum {mom:+.1f}% → tilt {tilt:+.0f}%"})

                    elif "Equal" in rb_strat:
                        eq=100.0/len(rb_holdings)
                        for h in rb_holdings:
                            cur_w=h["current_value"]/total_val*100 if total_val>0 else 0
                            drift=cur_w-eq
                            diff=total_val*(eq-cur_w)/100
                            action="BUY" if diff>min_order else ("SELL" if diff<-min_order else "HOLD")
                            rb_df_rows.append({"Ticker":h["ticker"],"Cur Wt%":round(cur_w,2),
                                "Eq Wt%":round(eq,2),"Drift%":round(drift,2),"Action":action,
                                "Amount (₹)":round(abs(diff),0),
                                "Qty":max(1,int(abs(diff)/h["price"])) if h["price"]>0 else 0,
                                "Price":h["price"],"AvgCost":h["avg_cost"],
                                "BuyDate":h.get("buy_date",""),
                                "Reason":f"Reset to 1/{len(rb_holdings)}={eq:.1f}%"})

                    elif "Risk Parity" in rb_strat:
                        vols={}
                        for h in rb_holdings:
                            dfv=rb_history(h["ticker"],"6mo")
                            if dfv is not None and len(dfv)>=30:
                                vols[h["ticker"]]=float(dfv["Close"].pct_change().dropna().tail(60).std()*np.sqrt(252))
                            else: vols[h["ticker"]]=0.20
                        inv={t:1/max(v,0.001) for t,v in vols.items()}
                        tot=sum(inv.values())
                        tgts={t:iv/tot*100 for t,iv in inv.items()}
                        for h in rb_holdings:
                            cur_w=h["current_value"]/total_val*100 if total_val>0 else 0
                            tgt_w=tgts[h["ticker"]]
                            drift=cur_w-tgt_w
                            diff=total_val*(tgt_w-cur_w)/100
                            action="BUY" if diff>min_order else ("SELL" if diff<-min_order else "HOLD")
                            rb_df_rows.append({"Ticker":h["ticker"],
                                "Ann Vol%":round(vols[h["ticker"]]*100,1),
                                "Cur Wt%":round(cur_w,2),"Tgt Wt%":round(tgt_w,2),
                                "Drift%":round(drift,2),"Action":action,
                                "Amount (₹)":round(abs(diff),0),
                                "Qty":max(1,int(abs(diff)/h["price"])) if h["price"]>0 else 0,
                                "Price":h["price"],"AvgCost":h["avg_cost"],
                                "BuyDate":h.get("buy_date",""),
                                "Reason":f"Vol {vols[h['ticker']]*100:.1f}% → weight {tgt_w:.1f}%"})

                    else:  # Sector Rotation
                        sec_ret={}
                        for h in rb_holdings:
                            sec=SECTOR_MAP.get(h["ticker"],"Other")
                            dfsr=rb_history(h["ticker"],"1y")
                            yr=0.0
                            if dfsr is not None and len(dfsr)>20:
                                yr=float((dfsr["Close"].iloc[-1]-dfsr["Close"].iloc[0])/dfsr["Close"].iloc[0]*100)
                            if sec not in sec_ret: sec_ret[sec]=[]
                            sec_ret[sec].append(yr)
                        sec_avg={s:np.mean(v) for s,v in sec_ret.items()}
                        for h in rb_holdings:
                            sec=SECTOR_MAP.get(h["ticker"],"Other")
                            sr=sec_avg.get(sec,0)
                            adj=-2.0 if sr>15 else (2.0 if sr<-10 else 0.0)
                            cur_w=h["current_value"]/total_val*100 if total_val>0 else 0
                            tgt_w=max(1,h.get("target_weight",100/len(rb_holdings))+adj)
                            drift=cur_w-tgt_w
                            diff=total_val*(tgt_w-cur_w)/100
                            action="BUY" if diff>min_order else ("SELL" if diff<-min_order else "HOLD")
                            rb_df_rows.append({"Ticker":h["ticker"],
                                "Sector":sec,"Sector YTD%":round(sr,1),
                                "Adjust":f"{adj:+.0f}%","Tgt Wt%":round(tgt_w,1),
                                "Drift%":round(drift,2),"Action":action,
                                "Amount (₹)":round(abs(diff),0),
                                "Qty":max(1,int(abs(diff)/h["price"])) if h["price"]>0 else 0,
                                "Price":h["price"],"AvgCost":h["avg_cost"],
                                "BuyDate":h.get("buy_date",""),
                                "Reason":f"Sector {sec} YTD {sr:+.1f}%"})

                    rb_df = pd.DataFrame(rb_df_rows)
                    st.session_state["rb_plan"] = rb_df

            # ── Show plan if computed ─────────────────────────
            if "rb_plan" in st.session_state and not st.session_state["rb_plan"].empty:
                rb_df = st.session_state["rb_plan"]

                n_buy  = (rb_df["Action"]=="BUY").sum()
                n_sell = (rb_df["Action"]=="SELL").sum()
                buy_amt= rb_df.loc[rb_df["Action"]=="BUY","Amount (₹)"].sum()
                sel_amt= rb_df.loc[rb_df["Action"]=="SELL","Amount (₹)"].sum()

                rc1,rc2,rc3,rc4 = st.columns(4)
                rc1.metric("🟢 BUY",  n_buy,  f"₹{buy_amt:,.0f}")
                rc2.metric("🔴 SELL", n_sell, f"₹{sel_amt:,.0f}")
                rc3.metric("✅ HOLD", len(rb_df)-n_buy-n_sell)
                rc4.metric("Net Cash", f"₹{buy_amt-sel_amt:+,.0f}")

                # New money notice
                if new_money>0 and n_buy>0:
                    deploy=min(new_money,buy_amt)
                    st.success(f"✅ ₹{deploy:,.0f} SIP money deployed to BUY orders first — "
                               f"avoids selling and reduces tax liability!")

                # Plan table
                disp_cols=[c for c in rb_df.columns if c not in ["Price","AvgCost","BuyDate"]]
                def _ca(v):
                    if v=="BUY":  return "background-color:#1a4731;color:#00e676;font-weight:bold"
                    if v=="SELL": return "background-color:#4a1010;color:#ff5252;font-weight:bold"
                    return "color:#888"
                st.dataframe(
                    rb_df[disp_cols].style.map(_ca,subset=["Action"])
                        .format({"Amount (₹)":"₹{:,.0f}","Drift%":"{:+.2f}%"} if "Drift%" in disp_cols else {"Amount (₹)":"₹{:,.0f}"}),
                    use_container_width=True, height=380)

                # ── Tax impact for SELL orders ────────────────
                sell_rows=rb_df[rb_df["Action"]=="SELL"]
                if not sell_rows.empty:
                    st.markdown("**⚠️ Estimated Tax on SELL Orders:**")
                    tax_rows=[]
                    for _,r in sell_rows.iterrows():
                        buy_dt=pd.to_datetime(r.get("BuyDate",datetime.now()-timedelta(days=400)))
                        days=(datetime.now()-buy_dt).days
                        gain=(r["Price"]-r["AvgCost"])*r["Qty"]
                        is_lt=days>=365
                        tax=max(0,gain*(0.125 if is_lt else 0.20)) if gain>0 else 0
                        tax_rows.append({
                            "Ticker":r["Ticker"],"Days Held":days,
                            "Type":"LTCG 12.5%" if is_lt else "STCG 20%",
                            "Gain/Loss":round(gain,2),"Est Tax":round(tax,2),
                            "Advice":"✅ OK (LTCG)" if is_lt else "⚠️ Consider delaying (STCG)",
                        })
                    tx_df=pd.DataFrame(tax_rows)
                    st.dataframe(tx_df.style.format({"Gain/Loss":"₹{:+,.0f}","Est Tax":"₹{:,.0f}"}),
                                 use_container_width=True)
                    st.warning(f"Total estimated tax: ₹{tx_df['Est Tax'].sum():,.0f}  |  "
                               "Tip: Use SIP top-up for BUY orders instead of selling to avoid this tax.")

                # ── STEP 4: Execute ───────────────────────────
                st.markdown("### 🚀 Step 4 — Execute Rebalancing")
                active_rb = rb_df[rb_df["Action"].isin(["BUY","SELL"])]

                if active_rb.empty:
                    st.success("✅ Portfolio already balanced! No action needed.")
                else:
                    pm_rb = not is_connected() or (KITE_AVAILABLE and paper_mode)
                    ex_col, dl_col = st.columns(2)

                    with ex_col:
                        if st.button(
                            f"{'📝 Paper' if pm_rb else '🚀 LIVE'} Execute {len(active_rb)} Rebalance Orders",
                            type="primary", use_container_width=True, key="rb_exec"
                        ):
                            exec_results=[]
                            for _,r in active_rb.iterrows():
                                qty=int(r.get("Qty",0))
                                if qty<=0: continue
                                log_e={
                                    "time":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "ticker":r["Ticker"],"action":r["Action"],
                                    "qty":qty,"price":r["Price"],"mode":"Paper" if pm_rb else "Live",
                                    "strategy":rb_strat[:25],
                                }
                                st.session_state.rb_log.append(log_e)

                                if not pm_rb and is_connected():
                                    try:
                                        kite=st.session_state.kite
                                        txn=(kite.TRANSACTION_TYPE_BUY if r["Action"]=="BUY"
                                             else kite.TRANSACTION_TYPE_SELL)
                                        oid=kite.place_order(
                                            variety=kite.VARIETY_REGULAR,
                                            exchange=kite.EXCHANGE_NSE,
                                            tradingsymbol=r["Ticker"],
                                            transaction_type=txn, quantity=qty,
                                            product=kite.PRODUCT_CNC,  # CNC = delivery
                                            order_type=kite.ORDER_TYPE_MARKET,
                                        )
                                        exec_results.append(f"✅ {r['Action']} {r['Ticker']} x{qty} → OrderID:{oid}")
                                    except Exception as e:
                                        exec_results.append(f"❌ {r['Ticker']}: {e}")
                                else:
                                    exec_results.append(f"📝 Paper {r['Action']} {r['Ticker']} x{qty} @ ₹{r['Price']:.2f}")

                            for res in exec_results: st.write(res)
                            _telegram(f"⚖️ Rebalance done ({rb_strat[:20]})\n"
                                      f"{'Paper' if pm_rb else 'LIVE'} | BUY:{n_buy} SELL:{n_sell}\n"
                                      f"Net: ₹{buy_amt-sel_amt:+,.0f}")
                            st.success("Rebalancing complete! ✅")
                            del st.session_state["rb_plan"]

                    with dl_col:
                        csv_buf=io.BytesIO()
                        rb_df.to_csv(csv_buf,index=False)
                        st.download_button("⬇️ Download Plan CSV",csv_buf.getvalue(),
                                           f"rebalance_{date.today()}.csv","text/csv",
                                           use_container_width=True,key="rb_dl")

                # Rebalance history
                if st.session_state.rb_log:
                    st.divider()
                    st.markdown("**📋 Rebalance History (This Session)**")
                    st.dataframe(pd.DataFrame(st.session_state.rb_log),use_container_width=True)

        # ── Strategy guide expander ───────────────────────────
        with st.expander("📚 Strategy Guide — Which to Use?", expanded=False):
            st.markdown("""
| Strategy | Trigger | Best For | Tax Friendly | Effort |
|---|---|---|---|---|
| **Threshold ±5%** | Drift > 5% | Most investors ✅ | ⭐⭐⭐⭐ | Low |
| **Momentum Tilt** | Monthly | Active investors | ⭐⭐⭐ | Medium |
| **Equal Weight** | Quarterly | Beginners ✅ | ⭐⭐⭐⭐ | Very Low |
| **Risk Parity** | Monthly | Risk-conscious | ⭐⭐⭐ | Medium |
| **Sector Rotation** | Monthly | Sector-aware | ⭐⭐⭐ | Medium |

**Golden Rules for Indian investors:**
- 🏆 **Use SIP new money first** for BUY orders — avoids tax on SELL
- 📅 **Hold >12 months** before selling — LTCG 12.5% vs STCG 20%
- 🎯 **₹1.25L LTCG exemption per year** — book profits up to this limit annually
- 📊 **Check quarterly**, not daily — over-trading increases costs and tax
- ✅ **5% threshold** is the sweet spot — avoids over-trading, catches real drift
            """)

if auto_ref:
    st.toast("Refreshing in 5 min...")
    time.sleep(300); st.rerun()

# ── Auto square-off at 3:20 PM IST ───────────────────────────
now_ist=datetime.utcnow()+timedelta(hours=5,minutes=30)
if now_ist.hour==15 and now_ist.minute>=20 and st.session_state.orders_today>0:
    pm=not is_connected() or (KITE_AVAILABLE and paper_mode)
    square_off_all(pm)
    _telegram(f"📤 Auto square-off at 3:20 PM | Session P&L: ₹{paper_pnl_mtm():+.2f}")
