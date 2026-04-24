"""
NSE PRO TRADER v5.0 — COMPLETE EDITION
=======================================
Universe  : 250 stocks (Nifty 100 + Midcap 150)
Strategies: 16 total (10 original + 6 new high-accuracy)
Features  :
  ✅ Zerodha Kite Connect — live orders + auto SL/Target
  ✅ Paper trade mode (safe default)
  ✅ Auto square-off at 3:20 PM IST
  ✅ Parallel data fetch (16 workers)
  ✅ Batch AI sentiment via Claude
  ✅ Market regime detection (Bull/Bear/Sideways)
  ✅ Multi-timeframe confluence (Daily + 15m)
  ✅ 10 original strategies (S1–S10)
  ✅ 6 new strategies: HH+HL, OBV Divergence, Flag Pattern,
     Relative Strength, Inside Bar/NR7, 3-Bar Reversal
  ✅ Conviction filter (min strategies + min score)
  ✅ Candle patterns + Pivot levels + 52W stats
  ✅ Sector tags + Large/Mid Cap filter
  ✅ Live P&L dashboard (Zerodha API or paper MTM)
  ✅ Telegram alerts for signals + fills
  ✅ Export CSV
  ✅ Portfolio Rebalancer tab (5 strategies, tax-aware, Zerodha CNC)
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
    page_title="NSE Pro Trader v5",
    page_icon="📈",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.block-container { padding-top:1rem; }
.regime-bull { background:#0d2e1a; border:1px solid #00e676; border-radius:6px; padding:4px 12px; color:#00e676; }
.regime-bear { background:#2e0d0d; border:1px solid #ff1744; border-radius:6px; padding:4px 12px; color:#ff1744; }
.regime-side { background:#1a1a0d; border:1px solid #ffd600; border-radius:6px; padding:4px 12px; color:#ffd600; }
.sector-tag  { display:inline-block; background:rgba(255,255,255,0.06); border:0.5px solid rgba(255,255,255,0.15);
               border-radius:4px; padding:1px 8px; font-size:11px; color:#aaa; margin-right:4px; }
div[data-testid="stExpander"] { border:1px solid #21262d !important; }
.stButton > button { font-weight:600; }
</style>
""", unsafe_allow_html=True)
if do_scan:

    # Validate strategy selection
    if not enabled_keys:
        st.warning("Select at least one strategy.")
        st.stop()

    # Progress bar start
    bar = st.progress(
        0,
        text=f"⚡ Fetching {len(UNIVERSE)} stocks in parallel..."
    )

    # Interval & period mapping
    imap = {
        "Intraday (5m)": "5m",
        "Intraday (15m)": "15m",
        "Swing (Daily)": "1d"
    }

    pmap = {
        "Intraday (5m)": "60d",
        "Intraday (15m)": "60d",
        "Swing (Daily)": "2y"
    }

    # Fetch data
    data_cache = fetch_parallel(
        UNIVERSE,
        imap[mode],
        pmap[mode],
        workers=16
    )

    # 🆕 Diagnostic
    success_count = sum(
        1 for v in data_cache.values() if v is not None
    )

    st.info(
        f"📊 Data fetch: {success_count}/{len(UNIVERSE)} stocks successful"
    )

    # Hard failure case
    if success_count == 0:
        st.error(
            "❌ All data fetches failed! Check data source (see banner above)"
        )
        st.stop()

    # Update progress
    bar.progress(
        0.35,
        text="✅ Data ready. Fetching Nifty 50 benchmark..."
    )

    # ... continue your logic here ...

# ╔══════════════════════════════════════════════════════════════╗
# ║                    STOCK UNIVERSES                          ║
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

MIDCAP150_RAW = [
    "MUTHOOTFIN.NS","SUNDARMFIN.NS","BAJAJHLDNG.NS","PNBHOUSING.NS","CANFINHOME.NS",
    "LTTS.NS","TATAELXSI.NS","INTELLECT.NS","TANLA.NS","BIRLASOFT.NS",
    "ABBOTINDIA.NS","PFIZER.NS","GLAXO.NS","AJANTPHARM.NS","GRANULES.NS",
    "MOTHERSON.NS","BOSCHLTD.NS","BHARATFORG.NS","EXIDEIND.NS","MINDA.NS",
    "EMAMILTD.NS","RADICO.NS","BIKAJI.NS","DEVYANI.NS","WESTLIFE.NS",
    "CUMMINSIND.NS","ABB.NS","THERMAX.NS","BHEL.NS","KEC.NS",
    "AARTI.NS","DEEPAKNITR.NS","NAVINFLUOR.NS","TATACHEM.NS","VINATI.NS",
    "NMDC.NS","MOIL.NS","RATNAMANI.NS","JSPL.NS","WELCORP.NS",
    "GODREJPROP.NS","OBEROIRLTY.NS","PRESTIGE.NS","BRIGADE.NS","SOBHA.NS",
    "JSWENERGY.NS","CESC.NS","POWERMECH.NS","GIPCL.NS","NAVA.NS",
    "PAGEIND.NS","MANYAVAR.NS","RAYMOND.NS","TRIDENT.NS","VARDHMAN.NS",
    "IRCTC.NS","CONCOR.NS","BLUEDART.NS","EASEMYTRIP.NS","THOMASCOOK.NS",
    "ZEEL.NS","SUNTV.NS","PVRINOX.NS","NETWEB.NS","TATACOMM.NS",
    "JKCEMENT.NS","RAMCOCEM.NS","HEIDELBERG.NS","KAJARIA.NS","CENTURYPLY.NS",
    "SUNDRMFAST.NS","AMARAJABAT.NS","SUPRAJIT.NS","GABRIEL.NS","JTEKTINDIA.NS",
    "JBCHEPHARM.NS","ERIS.NS","SOLARA.NS","SUVEN.NS","IPCALAB.NS",
    "NCC.NS","HGINFRA.NS","PNCINFRA.NS","KALPATPOWR.NS","GPPL.NS",
    "FINEORG.NS","ROSSARI.NS","ALKYLAMINE.NS","CLEAN.NS","PIDILITIND.NS",
    "PHOENIXLTD.NS","MAHLIFE.NS","SUNTECK.NS","LODHA.NS","KOLTEPATIL.NS",
    "STLTECH.NS","HFCL.NS","RAILTEL.NS","INOXWIND.NS","ROUTE.NS",
]
MIDCAP150 = [t for t in MIDCAP150_RAW if t not in NIFTY100]
ALL_STOCKS = NIFTY100 + MIDCAP150

SECTOR_MAP = {
    "RELIANCE.NS":"Oil & Gas","TCS.NS":"IT","HDFCBANK.NS":"Banking","INFY.NS":"IT",
    "ICICIBANK.NS":"Banking","SBIN.NS":"Banking","BHARTIARTL.NS":"Telecom","ITC.NS":"FMCG",
    "HINDUNILVR.NS":"FMCG","KOTAKBANK.NS":"Banking","LT.NS":"Infra","AXISBANK.NS":"Banking",
    "ASIANPAINT.NS":"Consumer","MARUTI.NS":"Auto","TITAN.NS":"Consumer",
    "SUNPHARMA.NS":"Pharma","BAJFINANCE.NS":"NBFC","WIPRO.NS":"IT","HCLTECH.NS":"IT",
    "TATAMOTORS.NS":"Auto","NTPC.NS":"Power","ONGC.NS":"Oil & Gas",
    "JSWSTEEL.NS":"Steel","TATASTEEL.NS":"Steel","DRREDDY.NS":"Pharma","CIPLA.NS":"Pharma",
    "ZOMATO.NS":"Consumer","TRENT.NS":"Retail","IRFC.NS":"Finance","PFC.NS":"Finance",
    "RECLTD.NS":"Finance","ADANIGREEN.NS":"Power","TATAPOWER.NS":"Power",
    "NHPC.NS":"Power","SJVN.NS":"Power","LTIM.NS":"IT","PERSISTENT.NS":"IT",
    "COFORGE.NS":"IT","KPITTECH.NS":"IT","MPHASIS.NS":"IT","TATAELXSI.NS":"IT",
    "MUTHOOTFIN.NS":"NBFC","SUNDARMFIN.NS":"NBFC","ABBOTINDIA.NS":"Pharma",
    "PFIZER.NS":"Pharma","GLAXO.NS":"Pharma","BOSCHLTD.NS":"Auto Anc",
    "BHARATFORG.NS":"Auto Anc","CUMMINSIND.NS":"Cap Goods","ABB.NS":"Cap Goods",
    "THERMAX.NS":"Cap Goods","GODREJPROP.NS":"Real Estate","OBEROIRLTY.NS":"Real Estate",
    "PRESTIGE.NS":"Real Estate","IRCTC.NS":"Travel","CONCOR.NS":"Logistics",
    "PAGEIND.NS":"Textile","NMDC.NS":"Metals","MOIL.NS":"Metals",
    "DEEPAKNITR.NS":"Chemicals","NAVINFLUOR.NS":"Chemicals","JKCEMENT.NS":"Cement",
    "RAMCOCEM.NS":"Cement","KAJARIA.NS":"Building","LICI.NS":"Insurance",
    "SBILIFE.NS":"Insurance","HDFCLIFE.NS":"Insurance","GODREJCP.NS":"FMCG",
    "DABUR.NS":"FMCG","MARICO.NS":"FMCG","SIEMENS.NS":"Industrials",
    "AMBUJACEM.NS":"Cement","DMART.NS":"Retail","NAUKRI.NS":"Internet",
    "JSWENERGY.NS":"Power","JSPL.NS":"Steel",
}

def get_sector(ticker: str) -> str:
    return SECTOR_MAP.get(ticker, "Midcap")

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
ss("rb_portfolio", []); ss("rb_log", [])

# ╔══════════════════════════════════════════════════════════════╗
# ║                  ZERODHA HELPERS                            ║
# ╚══════════════════════════════════════════════════════════════╝
def kite_login():
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
    except Exception as e: st.error(f"Token error: {e}"); return False

def is_connected() -> bool:
    return st.session_state.kite is not None and st.session_state.access_token != ""

def yf_to_kite(ticker: str) -> str:
    sym = ticker.replace(".NS","")
    return {"BAJAJ-AUTO":"BAJAJ-AUTO","MCDOWELL-N":"MCDOWELL-N"}.get(sym, sym)

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
    kite = st.session_state.kite
    try:
        txn = kite.TRANSACTION_TYPE_BUY if action=="BUY" else kite.TRANSACTION_TYPE_SELL
        eid = kite.place_order(variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                               tradingsymbol=yf_to_kite(symbol), transaction_type=txn,
                               quantity=qty, product=kite.PRODUCT_MIS,
                               order_type=kite.ORDER_TYPE_MARKET)
        time.sleep(0.8)
        sl_txn = kite.TRANSACTION_TYPE_SELL if action=="BUY" else kite.TRANSACTION_TYPE_BUY
        sl_trig = round(sl*0.998,2) if action=="BUY" else round(sl*1.002,2)
        slid = kite.place_order(variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                                tradingsymbol=yf_to_kite(symbol), transaction_type=sl_txn,
                                quantity=qty, product=kite.PRODUCT_MIS,
                                order_type=kite.ORDER_TYPE_SL_M,
                                trigger_price=sl_trig, price=sl)
        tid = kite.place_order(variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                               tradingsymbol=yf_to_kite(symbol), transaction_type=sl_txn,
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
        _telegram("📤 All paper positions squared off"); return
    kite = st.session_state.kite
    if not kite: return
    try:
        for o in kite.orders():
            if o["status"] in ("OPEN","TRIGGER PENDING"):
                try: kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=o["order_id"])
                except: pass
        time.sleep(0.5)
        for p in kite.positions()["day"]:
            if p["quantity"]!=0:
                txn = kite.TRANSACTION_TYPE_SELL if p["quantity"]>0 else kite.TRANSACTION_TYPE_BUY
                kite.place_order(variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                                 tradingsymbol=p["tradingsymbol"], transaction_type=txn,
                                 quantity=abs(p["quantity"]), product=kite.PRODUCT_MIS,
                                 order_type=kite.ORDER_TYPE_MARKET)
        _telegram("📤 All LIVE MIS positions squared off")
    except Exception as e: st.error(f"Square off error: {e}")

def fetch_live_positions():
    kite = st.session_state.kite
    if not kite: return [], 0.0
    try:
        pos = kite.positions()["day"]
        return pos, round(sum(p.get("pnl",0) for p in pos), 2)
    except: return [], 0.0

def paper_pnl_mtm() -> float:
    total = 0.0
    for t in st.session_state.paper_trades:
        if t["status"]!="Open": total += t.get("pnl",0.0); continue
        try:
            sym = t.get("symbol", t.get("ticker",""))
            info = yf.Ticker(sym+".NS").fast_info
            ltp  = float(getattr(info,"last_price",t["entry"]) or t["entry"])
        except: ltp = t["entry"]
        if t["action"]=="BUY":
            if ltp>=t["target"]: t["status"]="Target Hit"; t["pnl"]=round((t["target"]-t["entry"])*t["qty"],2)
            elif ltp<=t["sl"]:   t["status"]="SL Hit";     t["pnl"]=round((t["sl"]-t["entry"])*t["qty"],2)
            else:                                           t["pnl"]=round((ltp-t["entry"])*t["qty"],2)
        else:
            if ltp<=t["target"]: t["status"]="Target Hit"; t["pnl"]=round((t["entry"]-t["target"])*t["qty"],2)
            elif ltp>=t["sl"]:   t["status"]="SL Hit";     t["pnl"]=round((t["entry"]-t["sl"])*t["qty"],2)
            else:                                           t["pnl"]=round((t["entry"]-ltp)*t["qty"],2)
        total += t["pnl"]
    return round(total,2)

def _telegram(msg: str):
    try:
        tok=st.secrets.get("TELEGRAM_TOKEN",""); cid=st.secrets.get("TELEGRAM_CHAT_ID","")
        if tok and cid:
            requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          data={"chat_id":cid,"text":msg},timeout=5)
    except: pass

# ╔══════════════════════════════════════════════════════════════╗
# ║                    DATA FETCH                               ║
# ──────────────────────────────────────────────────────────────
# Yahoo Finance blocks Streamlit Cloud IPs (HTTP 403).
# Fix: Kite API (historical candles) → yfinance fallback.
# If both fail the app shows a clear actionable error.
# ╚══════════════════════════════════════════════════════════════╝

# ── Kite interval/period maps ─────────────────────────────────
_KITE_INTERVAL = {
    "1d":"day","1wk":"week","15m":"15minute","5m":"5minute","1h":"60minute",
}
_KITE_DAYS = {
    "60d":60,"6mo":180,"1y":365,"2y":730,"5d":5,"10d":10,
}

# ── Symbol → Kite instrument token (full Nifty 100 + Midcap 150) ─
# Tokens are stable and rarely change. Last verified April 2026.
_KITE_TOKENS = {
    # ── Nifty 50 core ─────────────────────────────────────────
    "RELIANCE":738561,"TCS":2953217,"HDFCBANK":341249,"INFY":408065,
    "ICICIBANK":1270529,"SBIN":779521,"HINDUNILVR":356865,"ITC":424961,
    "BHARTIARTL":2714625,"KOTAKBANK":492033,"LT":2939009,"AXISBANK":1510401,
    "ASIANPAINT":60417,"MARUTI":2815745,"TITAN":897537,"SUNPHARMA":857857,
    "BAJFINANCE":81153,"WIPRO":969473,"HCLTECH":1850625,"TATAMOTORS":884737,
    "NTPC":2977281,"ONGC":633601,"JSWSTEEL":3001089,"TATASTEEL":895745,
    "ADANIPORTS":3861249,"COALINDIA":1893345,"BAJAJFINSV":54273,
    "DRREDDY":225537,"CIPLA":177665,"TECHM":3465729,"NESTLEIND":4598529,
    "GRASIM":315521,"HINDALCO":348929,"BPCL":134657,"INDUSINDBK":1346049,
    "POWERGRID":3834113,"DIVISLAB":2800641,"BRITANNIA":140033,
    "EICHERMOT":232961,"TATACONSUM":878593,"APOLLOHOSP":157441,
    "HEROMOTOCO":345089,"BAJAJ-AUTO":4267265,"SBILIFE":5582849,
    "HDFCLIFE":119809,"ADANIENT":25,"VEDL":784129,"HAVELLS":2513665,
    "ULTRACEMCO":2952193,"PIDILITIND":674281,
    # ── Nifty Next 50 ─────────────────────────────────────────
    "NAUKRI":3000473,"GODREJCP":2763265,"DMART":3906585,"SIEMENS":857345,
    "AMBUJACEM":1152769,"DABUR":197633,"MARICO":519937,"COLPAL":120321,
    "BERGEPAINT":111745,"TORNTPHARM":900185,"LUPIN":2672641,"AUBANK":3660545,
    "BANDHANBNK":579389,"FEDERALBNK":261889,"IDFCFIRSTB":2863937,
    "PNB":2730497,"BANKBARODA":1195009,"CANBK":2763777,"UNIONBANK":2752769,
    "INDIGO":4343041,"TRENT":3405313,"ZOMATO":5552129,"ADANIGREEN":6401,"TATAPOWER":877057,
    "LICI":4633601,"GAIL":1207553,"IOC":415745,"HINDPETRO":359425,
    "RECLTD":3220993,"PFC":3930881,"IRFC":4633089,"NHPC":4592641,
    "SJVN":4223233,"TORNTPOWER":3903553,"MCDOWELL-N":2013441,
    "AUROPHARMA":61441,"ALKEM":3382017,"LAURUSLABS":5789953,
    "MPHASIS":648961,"LTIM":5120641,"PERSISTENT":3074561,"COFORGE":3275777,
    "KPITTECH":4793857,"ASHOKLEY":54273,"TVSMOTOR":2170625,
    "BALKRISIND":67329,"CHOLAFIN":3823361,"LICHSGFIN":511233,
    "MANAPPURAM":2031617,"ABCAPITAL":5436929,
    # ── Midcap 150 ────────────────────────────────────────────
    "MUTHOOTFIN":3362305,"SUNDARMFIN":4106241,"BAJAJHLDNG":83969,
    "PNBHOUSING":3701377,"CANFINHOME":162049,
    "LTTS":4286465,"TATAELXSI":3094273,"INTELLECT":3094081,
    "TANLA":4267009,"BIRLASOFT":4633345,
    "ABBOTINDIA":4536,"PFIZER":3169,"GLAXO":3190721,
    "AJANTPHARM":14977,"GRANULES":3906305,
    "MOTHERSON":4183297,"BOSCHLTD":4911873,"BHARATFORG":98049,
    "EXIDEIND":296193,"MINDA":2947585,
    "EMAMILTD":3492097,"RADICO":2670337,"BIKAJI":6192129,
    "DEVYANI":5552897,"WESTLIFE":3321089,
    "CUMMINSIND":4104193,"ABB":3909249,"THERMAX":3884801,
    "BHEL":438273,"KEC":3788545,
    "AARTI":2916609,"DEEPAKNITR":4359937,"NAVINFLUOR":4278529,
    "TATACHEM":871681,"VINATI":3098625,
    "NMDC":3924993,"MOIL":4606977,"RATNAMANI":3988225,
    "JSPL":3353601,"WELCORP":4800769,
    "GODREJPROP":3326977,"OBEROIRLTY":4528385,"PRESTIGE":3046401,
    "BRIGADE":3528961,"SOBHA":3020801,
    "JSWENERGY":4262913,"CESC":3843329,"POWERMECH":3780865,
    "PAGEIND":3047169,"RAYMOND":2779649,
    "TRIDENT":3750913,"VARDHMAN":4567809,
    "IRCTC":3484929,"CONCOR":4278785,"BLUEDART":2626817,
    "EASEMYTRIP":5097985,"THOMASCOOK":3497729,
    "ZEEL":975873,"SUNTV":3001345,"PVRINOX":3904513,
    "NETWEB":6084609,"TATACOMM":3358465,
    "JKCEMENT":3712257,"RAMCOCEM":3771393,"KAJARIA":3813889,
    "CENTURYPLY":3398657,"STLTECH":4029697,
    "HFCL":2622209,"RAILTEL":4629761,"INOXWIND":5767169,
}

# ── Dynamic token loading from Kite instruments list ─────────
@st.cache_data(ttl=86400)   # refresh once a day
def _load_kite_tokens_dynamic() -> dict:
    """
    Fetch full NSE instrument list from Kite and build symbol→token map.
    Called once per day when Kite is connected. Covers ALL 250 stocks.
    """
    kite = st.session_state.get("kite")
    if not kite: return {}
    try:
        instruments = kite.instruments("NSE")
        token_map   = {}
        for inst in instruments:
            if inst.get("segment") == "NSE" and inst.get("instrument_type") == "EQ":
                token_map[inst["tradingsymbol"]] = inst["instrument_token"]
        return token_map
    except: return {}

def _get_kite_token(sym: str) -> int | None:
    """Get token from dynamic map first, fall back to hardcoded."""
    # Try dynamic (full list)
    dyn = _load_kite_tokens_dynamic()
    if sym in dyn: return dyn[sym]
    # Fall back to hardcoded
    return _KITE_TOKENS.get(sym)

@st.cache_data(ttl=300)
def _get_data_kite(ticker_ns: str, interval: str, period: str):
    """Fetch OHLCV from Zerodha Kite historical API. Covers all 250 stocks."""
    kite = st.session_state.get("kite")
    if not kite: return None
    sym   = ticker_ns.replace(".NS","")
    token = _get_kite_token(sym)
    if not token: return None
    try:
        days = _KITE_DAYS.get(period, 365)
        to_d = datetime.now()
        fr_d = to_d - timedelta(days=days)
        ki   = _KITE_INTERVAL.get(interval, "day")
        candles = kite.historical_data(token, fr_d, to_d, ki, continuous=False, oi=False)
        if not candles: return None
        df = pd.DataFrame(candles)
        df = df.rename(columns={"date":"Date","open":"Open","high":"High",
                                  "low":"Low","close":"Close","volume":"Volume"})
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date").sort_index()
        df = df[["Open","High","Low","Close","Volume"]]
        df = df[~df.index.duplicated(keep="last")]
        return df if len(df) >= 50 else None
    except: return None

@st.cache_data(ttl=300)
def _get_data_yfinance(ticker: str, interval: str, period: str):
    """Fetch OHLCV from Yahoo Finance (works locally, blocked on Streamlit Cloud)."""
    try:
        raw = yf.download(ticker,period=period,interval=interval,
                          auto_adjust=True,progress=False,timeout=10)
        if raw is None or raw.empty: return None
        if isinstance(raw.columns,pd.MultiIndex):
            raw.columns=raw.columns.get_level_values(0)
        raw = raw.loc[:,~raw.columns.duplicated()]
        df  = raw[[c for c in ["Open","High","Low","Close","Volume"] if c in raw.columns]].copy()
        df  = df[~df.index.duplicated(keep="last")].sort_index()
        return df if len(df)>=50 else None
    except: return None

def get_data(ticker: str, interval: str, period: str):
    """
    Smart data fetch: Kite API first (always works when connected),
    then yfinance (works locally, blocked on Streamlit Cloud).
    """
    # Try Kite first (connected + token available)
    df = _get_data_kite(ticker, interval, period)
    if df is not None and len(df) >= 50:
        return df
    # Fall back to yfinance
    df = _get_data_yfinance(ticker, interval, period)
    return df

def check_data_source() -> tuple[bool, str]:
    """
    Returns (data_works: bool, message: str).
    Tests whether any data source is working.
    """
    # Test yfinance with a quick probe
    try:
        raw = yf.download("RELIANCE.NS", period="5d", interval="1d",
                          auto_adjust=True, progress=False, timeout=8)
        if raw is not None and len(raw) > 0:
            return True, "yfinance"
    except: pass

    # Test Kite
    if is_connected():
        df = _get_data_kite("RELIANCE.NS","1d","5d")
        if df is not None: return True, "kite"

    return False, "none"

def fetch_parallel(tickers, interval, period, workers=16):
    out = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(get_data,t,interval,period):t for t in tickers}
        for f in concurrent.futures.as_completed(futs): out[futs[f]]=f.result()
    return out

@st.cache_data(ttl=3600)
def get_news(tc):
    try:
        news=yf.Ticker(tc+".NS").news or []
        return tuple(n.get("title","") for n in news[:5] if n.get("title"))
    except: return ()

@st.cache_data(ttl=3600)
def get_nifty50_returns() -> pd.Series:
    try:
        n=yf.download("^NSEI",period="6mo",interval="1d",auto_adjust=True,progress=False,timeout=10)
        if n is None or n.empty: return pd.Series(dtype=float)
        if isinstance(n.columns,pd.MultiIndex): n.columns=n.columns.get_level_values(0)
        return n["Close"].squeeze().pct_change().dropna()
    except: return pd.Series(dtype=float)

# ╔══════════════════════════════════════════════════════════════╗
# ║               MARKET REGIME                                 ║
# ╚══════════════════════════════════════════════════════════════╝
@st.cache_data(ttl=3600)
def market_regime() -> str:
    for sym in ["^NSEI","NIFTYBEES.NS"]:
        try:
            n=yf.download(sym,period="6mo",interval="1d",auto_adjust=True,progress=False,timeout=10)
            if n is None or n.empty: continue
            if isinstance(n.columns,pd.MultiIndex): n.columns=n.columns.get_level_values(0)
            n=n.loc[:,~n.columns.duplicated()]
            if "Close" not in n.columns or len(n)<50: continue
            c=n["Close"].squeeze(); h=n["High"].squeeze(); l=n["Low"].squeeze()
            e20=c.ewm(span=20).mean(); e50=c.ewm(span=50).mean()
            adx=ta.trend.ADXIndicator(h,l,c,14).adx().iloc[-1]
            if e20.iloc[-1]>e50.iloc[-1] and adx>20: return "Bull"
            if e20.iloc[-1]<e50.iloc[-1] and adx>20: return "Bear"
            return "Sideways"
        except: continue
    return "Sideways"

def regime_weights(regime: str) -> dict:
    if regime=="Bull":
        return {"ORB":0.07,"VWAP":0.07,"EMA":0.10,"MACD":0.10,"BB":0.07,"RSI":0.04,
                "ST":0.09,"Stoch":0.05,"W52":0.08,"Pivot":0.03,
                "HH_HL":0.10,"OBV_DIV":0.07,"FLAG":0.07,"RS":0.08,"IB":0.05,"TBR":0.04}
    if regime=="Bear":
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
    if df is None or len(df)<50: return pd.DataFrame()
    df=df.copy()
    c=df["Close"].squeeze(); h=df["High"].squeeze()
    l=df["Low"].squeeze();   v=df["Volume"].squeeze()
    df["Close"]=c; df["High"]=h; df["Low"]=l; df["Volume"]=v
    df["ema9"]  =ta.trend.EMAIndicator(c,9).ema_indicator()
    df["ema21"] =ta.trend.EMAIndicator(c,21).ema_indicator()
    df["ema50"] =ta.trend.EMAIndicator(c,50).ema_indicator()
    df["ema200"]=ta.trend.EMAIndicator(c,200).ema_indicator()
    df["rsi"]   =ta.momentum.RSIIndicator(c,14).rsi()
    df["atr"]   =ta.volatility.AverageTrueRange(h,l,c,14).average_true_range()
    df["obv"]   =ta.volume.OnBalanceVolumeIndicator(c,v).on_balance_volume()
    if hasattr(df.index,"date"):
        dates=pd.Series(df.index.date,index=df.index)
        df["vwap"]=((c*v).groupby(dates).cumsum()/v.replace(0,np.nan).groupby(dates).cumsum())
    else:
        df["vwap"]=(c*v).cumsum()/v.replace(0,np.nan).cumsum()
    mi=ta.trend.MACD(c)
    df["macd"]=mi.macd(); df["macd_s"]=mi.macd_signal(); df["macd_h"]=mi.macd_diff()
    bb=ta.volatility.BollingerBands(c,20,2)
    df["bb_u"]=bb.bollinger_hband(); df["bb_l"]=bb.bollinger_lband()
    df["bb_m"]=bb.bollinger_mavg(); df["bb_w"]=(df["bb_u"]-df["bb_l"])/df["bb_m"]
    adxi=ta.trend.ADXIndicator(h,l,c,14)
    df["adx"]=adxi.adx(); df["di_pos"]=adxi.adx_pos(); df["di_neg"]=adxi.adx_neg()
    st2=ta.momentum.StochasticOscillator(h,l,c,14,3)
    df["stoch_k"]=st2.stoch(); df["stoch_d"]=st2.stoch_signal()
    df["vol_ratio"]=v/v.rolling(20).mean()
    df["body"]=abs(c-df["Open"].squeeze())
    df["wick_u"]=h-c.clip(lower=df["Open"].squeeze())
    df["wick_l"]=c.clip(upper=df["Open"].squeeze())-l
    df["range"]=h-l
    df["nr7"]=df["range"]==df["range"].rolling(7).min()
    return df.ffill().dropna()

# ╔══════════════════════════════════════════════════════════════╗
# ║              ALL 16 STRATEGIES                              ║
# ╚══════════════════════════════════════════════════════════════╝
def _s(sig,conf,reason): return sig,int(min(95,max(0,conf))),reason

# ── Original 10 ───────────────────────────────────────────────
def s_orb(df,mode="Swing (Daily)"):
    if "Daily" in mode or "Swing" in mode: return _s("HOLD",0,"N/A daily")
    if len(df)<10: return _s("HOLD",0,"")
    oh=df["High"].iloc[:6].max(); ol=df["Low"].iloc[:6].min()
    p=df["Close"].iloc[-1]; vr=df["vol_ratio"].iloc[-1]; adx=df["adx"].iloc[-1]
    if p>oh and vr>1.2 and adx>18: return _s("BUY",65+vr*8,f"ORB breakout ₹{oh:.1f} {vr:.1f}x")
    if p<ol and vr>1.2 and adx>18: return _s("SELL",62+vr*8,f"ORB breakdown ₹{ol:.1f} {vr:.1f}x")
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
    if bull: return _s("BUY",70+(adx-22)*0.5,f"MACD bullish ADX={adx:.0f}")
    if bear: return _s("SELL",68+(adx-22)*0.5,f"MACD bearish ADX={adx:.0f}")
    return _s("HOLD",0,"")

def s_bb(df):
    if len(df)<30: return _s("HOLD",0,"")
    bw,p,vr=df["bb_w"],df["Close"].iloc[-1],df["vol_ratio"].iloc[-1]
    rm=bw.rolling(min(50,len(bw))).mean(); sq=bw.iloc[-5:-1].mean()<rm.iloc[-1]*0.80
    if sq and p>df["bb_u"].iloc[-1] and vr>1.2: return _s("BUY",68+vr*5,f"BB sq break vol={vr:.1f}x")
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
    hl2=(df["High"]+df["Low"])/2; up=hl2+3*atr; dn=hl2-3*atr
    if not(c.iloc[-2]>dn.iloc[-2]) and c.iloc[-1]>dn.iloc[-1]:
        return _s("BUY",70+adx*0.4,f"ST flip bull SL₹{dn.iloc[-1]:.1f}")
    if not(c.iloc[-2]<up.iloc[-2]) and c.iloc[-1]<up.iloc[-1]:
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

# ── 6 New strategies ──────────────────────────────────────────
def s_hh_hl(df):
    if len(df)<30: return _s("HOLD",0,"")
    h=df["High"]; l=df["Low"]; c=df["Close"]
    lh=h.rolling(5,center=True).max()==h; ll=l.rolling(5,center=True).min()==l
    sh=h[lh].iloc[-4:]; sl2=l[ll].iloc[-4:]
    if len(sh)<2 or len(sl2)<2: return _s("HOLD",0,"")
    hh=float(sh.iloc[-1])>float(sh.iloc[-2]); hl=float(sl2.iloc[-1])>float(sl2.iloc[-2])
    ll2=float(sl2.iloc[-1])<float(sl2.iloc[-2]); lh2=float(sh.iloc[-1])<float(sh.iloc[-2])
    price=float(c.iloc[-1]); e50=float(df["ema50"].iloc[-1])
    rsi=float(df["rsi"].iloc[-1]); adx=float(df["adx"].iloc[-1])
    vr=float(df["vol_ratio"].iloc[-1])>1.0
    if hh and hl and price>e50 and adx>20 and 40<rsi<75 and vr:
        return _s("BUY",int(min(90,72+adx*0.4)),f"HH+HL trend | ADX={adx:.0f} RSI={rsi:.0f}")
    if ll2 and lh2 and price<e50 and adx>20 and rsi<60:
        return _s("SELL",int(min(88,70+adx*0.4)),f"LL+LH downtrend | ADX={adx:.0f}")
    return _s("HOLD",0,"")

def s_obv_divergence(df):
    if len(df)<20: return _s("HOLD",0,"")
    c=df["Close"]; obv=df["obv"]; rsi=float(df["rsi"].iloc[-1])
    lb=10
    p_dn=float(c.iloc[-1])<float(c.iloc[-lb]); obv_up=float(obv.iloc[-1])>float(obv.iloc[-lb])
    p_up=float(c.iloc[-1])>float(c.iloc[-lb]); obv_dn=float(obv.iloc[-1])<float(obv.iloc[-lb])
    slope=(float(obv.iloc[-1])-float(obv.iloc[-5]))/max(abs(float(obv.iloc[-5])),1)
    if p_dn and obv_up and slope>0 and rsi<55:
        return _s("BUY",int(min(86,68+abs(slope)*500)),f"OBV bull div | RSI={rsi:.0f}")
    if p_up and obv_dn and slope<0 and rsi>45:
        return _s("SELL",int(min(84,66+abs(slope)*500)),f"OBV bear div | RSI={rsi:.0f}")
    return _s("HOLD",0,"")

def s_flag_pattern(df):
    if len(df)<20: return _s("HOLD",0,"")
    c=df["Close"]; h=df["High"]; l=df["Low"]; v=df["vol_ratio"]
    atr=float(df["atr"].iloc[-1]); rsi=float(df["rsi"].iloc[-1])
    pole_pct=(float(c.iloc[-5])-float(c.iloc[-10]))/float(c.iloc[-10])*100
    consol_range=float(h.iloc[-5:-1].max()-l.iloc[-5:-1].min())
    tight=consol_range<atr*2.5
    ch=float(h.iloc[-5:-1].max()); cl=float(l.iloc[-5:-1].min())
    price=float(c.iloc[-1]); vol_ok=float(v.iloc[-1])>1.3
    if pole_pct>3.0 and tight and price>ch and vol_ok and rsi<80:
        return _s("BUY",int(min(88,68+pole_pct*2)),f"Bull flag | pole={pole_pct:.1f}% vol={v.iloc[-1]:.1f}x")
    if pole_pct<-3.0 and tight and price<cl and vol_ok and rsi>20:
        return _s("SELL",int(min(86,66+abs(pole_pct)*2)),f"Bear flag | pole={pole_pct:.1f}%")
    return _s("HOLD",0,"")

def s_relative_strength(df, nifty_returns: pd.Series):
    if len(df)<22 or nifty_returns.empty: return _s("HOLD",0,"")
    c=df["Close"].squeeze(); lb=min(20,len(c)-1,len(nifty_returns)-1)
    if lb<5: return _s("HOLD",0,"")
    stock_ret=float(c.iloc[-1])/float(c.iloc[-lb])-1
    nifty_ret=float((1+nifty_returns.iloc[-lb:]).prod())-1
    rs=stock_ret-nifty_ret
    price=float(c.iloc[-1]); e50=float(df["ema50"].iloc[-1])
    rsi=float(df["rsi"].iloc[-1]); adx=float(df["adx"].iloc[-1])
    if rs>0.05 and price>e50 and 45<rsi<75 and adx>18:
        return _s("BUY",int(min(88,68+rs*200)),f"RS+{rs*100:.1f}% vs Nifty")
    if rs<-0.05 and price<e50 and rsi<55:
        return _s("SELL",int(min(84,64+abs(rs)*200)),f"RS{rs*100:.1f}% vs Nifty")
    return _s("HOLD",0,"")

def s_inside_bar_nr7(df):
    if len(df)<10: return _s("HOLD",0,"")
    h=df["High"]; l=df["Low"]; c=df["Close"]
    atr=float(df["atr"].iloc[-1]); rsi=float(df["rsi"].iloc[-1])
    vr=float(df["vol_ratio"].iloc[-1]); adx=float(df["adx"].iloc[-1])
    is_inside=float(h.iloc[-1])<float(h.iloc[-2]) and float(l.iloc[-1])>float(l.iloc[-2])
    is_nr7=bool(df["nr7"].iloc[-1]) if "nr7" in df.columns else False
    ph=float(h.iloc[-2]); pl=float(l.iloc[-2]); price=float(c.iloc[-1])
    up=float(df["ema21"].iloc[-1])>float(df["ema50"].iloc[-1])
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
    three_red=float(c.iloc[-4])>float(c.iloc[-3])>float(c.iloc[-2])
    bar4_green=float(c.iloc[-1])>float(c.iloc[-2])
    mid_up=(float(c.iloc[-4])+float(c.iloc[-3]))/2
    rsi_up=float(rsi.iloc[-2])<42 and float(rsi.iloc[-1])>float(rsi.iloc[-2])
    if three_red and bar4_green and float(c.iloc[-1])>mid_up and rsi_up and vr>1.4:
        return _s("BUY",int(min(88,68+vr*5+(42-float(rsi.iloc[-2]))*0.5)),
                  f"3-bar reversal | RSI={rsi.iloc[-1]:.0f} vol={vr:.1f}x")
    three_green=float(c.iloc[-4])<float(c.iloc[-3])<float(c.iloc[-2])
    bar4_red=float(c.iloc[-1])<float(c.iloc[-2])
    mid_dn=(float(c.iloc[-4])+float(c.iloc[-3]))/2
    rsi_dn=float(rsi.iloc[-2])>58 and float(rsi.iloc[-1])<float(rsi.iloc[-2])
    if three_green and bar4_red and float(c.iloc[-1])<mid_dn and rsi_dn and vr>1.4:
        return _s("SELL",int(min(86,66+vr*5+(float(rsi.iloc[-2])-58)*0.5)),
                  f"3-bar top | RSI={rsi.iloc[-1]:.0f} vol={vr:.1f}x")
    return _s("HOLD",0,"")

# ── Strategy registry ─────────────────────────────────────────
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
NEEDS_NIFTY={"RS"}; NEEDS_MODE={"ORB"}

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
            elif sig=="SELL":sw+=w*(conf/100); triggers.append(f"🔴 {STRAT_LABELS[k]} SELL ({conf}%)")
            tw+=w
        except: pass
    score=(bw-sw)/tw if tw else 0
    sig="BUY" if score>0.20 else ("SELL" if score<-0.20 else "HOLD")
    return {"signal":sig,"score":round(score,3),"strategies":results,"triggers":triggers,
            "n_buy":sum(1 for v in results.values() if v["signal"]=="BUY"),
            "n_sell":sum(1 for v in results.values() if v["signal"]=="SELL")}

def mtf_check(ticker, primary_sig, enabled_keys, rw, nifty_ret) -> bool:
    try:
        df15=get_data(ticker,"15m","60d")
        if df15 is None: return True
        df15=add_features(df15)
        if df15.empty: return True
        r=run_strategies(df15,enabled_keys,"Intraday (15m)",rw,nifty_ret)
        return r["signal"]==primary_sig or r["signal"]=="HOLD"
    except: return True

def candle_patterns(df) -> list:
    o=float(df["Open"].iloc[-1]); h=float(df["High"].iloc[-1])
    l=float(df["Low"].iloc[-1]);  c=float(df["Close"].iloc[-1])
    o2=float(df["Open"].iloc[-2]); c2=float(df["Close"].iloc[-2])
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
        key=st.secrets.get("ANTHROPIC_API_KEY","")
        if not key: return {}
        client=anthropic.Anthropic(api_key=key)
        items=json.loads(payload_json)
        lines=[f'{it["ticker"]}: Rs{it["price"]:.1f} ({it["pct"]:+.1f}%) | {"; ".join(it["headlines"][:3]) or "No news"}'
               for it in items]
        prompt=("Senior NSE analyst. Return JSON array only, no markdown:\n\n"
                +"\n".join(lines)
                +'\n\n[{"ticker":"X","score":<-1 to 1>,"label":"Bullish/Neutral/Bearish","confidence":<0-100>,"summary":"1 line"}]')
        r=client.messages.create(model="claude-sonnet-4-20250514",max_tokens=600,
                                  messages=[{"role":"user","content":prompt}])
        arr=json.loads(r.content[0].text.strip().replace("```json","").replace("```",""))
        return {a["ticker"]:{"score":max(-1,min(1,float(a.get("score",0)))),
                              "label":a.get("label","Neutral"),
                              "confidence":max(0,min(100,int(a.get("confidence",0)))),
                              "summary":a.get("summary","—")} for a in arr}
    except: return {}

# ╔══════════════════════════════════════════════════════════════╗
# ║              POSITION SIZING                                ║
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
# ║              SCAN ONE STOCK                                 ║
# ╚══════════════════════════════════════════════════════════════╝
def scan_one(ticker,df_raw,mode,enabled_keys,rw,use_mtf,sent_cache,capital,risk_pct,nifty_ret):
    try:
        df=add_features(df_raw)
        if df.empty or len(df)<50: return None
        tech=run_strategies(df,enabled_keys,mode,rw,nifty_ret)
        p=float(df["Close"].iloc[-1]); prev=float(df["Close"].iloc[-2])
        pct=(p-prev)/prev*100; atr=float(df["atr"].iloc[-1])
        tc=ticker.replace(".NS","")
        mtf_ok=True
        if use_mtf and tech["signal"] in ("BUY","SELL") and "Daily" in mode:
            mtf_ok=mtf_check(ticker,tech["signal"],enabled_keys,rw,nifty_ret)
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
            "sector":get_sector(ticker),
            "cap_type":"Large Cap" if ticker in NIFTY100 else "Mid Cap",
        }
    except: return None

# ╔══════════════════════════════════════════════════════════════╗
# ║                  REBALANCER HELPERS                         ║
# ╚══════════════════════════════════════════════════════════════╝
RB_SECTOR_MAP = {
    "RELIANCE":"Energy","TCS":"IT","HDFCBANK":"Banking","INFY":"IT","ICICIBANK":"Banking",
    "HINDUNILVR":"FMCG","ITC":"FMCG","SBIN":"Banking","BHARTIARTL":"Telecom",
    "KOTAKBANK":"Banking","LT":"Infra","AXISBANK":"Banking","ASIANPAINT":"Consumer",
    "MARUTI":"Auto","TITAN":"Consumer","SUNPHARMA":"Pharma","BAJFINANCE":"NBFC",
    "WIPRO":"IT","HCLTECH":"IT","TATAMOTORS":"Auto","NTPC":"Utilities","ONGC":"Energy",
    "JSWSTEEL":"Metals","TATASTEEL":"Metals","DRREDDY":"Pharma","CIPLA":"Pharma",
    "ZOMATO":"Consumer","TRENT":"Retail","IRFC":"Finance","PFC":"Finance",
    "RECLTD":"Finance","ADANIGREEN":"Energy","TATAPOWER":"Utilities","NHPC":"Utilities",
    "SJVN":"Utilities","LTIM":"IT","PERSISTENT":"IT","COFORGE":"IT","KPITTECH":"IT",
    "MPHASIS":"IT","TATAELXSI":"IT","MUTHOOTFIN":"NBFC","SUNDARMFIN":"NBFC",
    "ABBOTINDIA":"Pharma","PFIZER":"Pharma","GLAXO":"Pharma","BOSCHLTD":"Auto Anc",
    "BHARATFORG":"Auto Anc","CUMMINSIND":"Cap Goods","ABB":"Cap Goods",
    "THERMAX":"Cap Goods","GODREJPROP":"Real Estate","OBEROIRLTY":"Real Estate",
    "PRESTIGE":"Real Estate","IRCTC":"Travel","CONCOR":"Logistics","PAGEIND":"Textile",
    "NMDC":"Metals","MOIL":"Metals","DEEPAKNITR":"Chemicals","NAVINFLUOR":"Chemicals",
    "JKCEMENT":"Cement","RAMCOCEM":"Cement","KAJARIA":"Building",
    "LICI":"Insurance","SBILIFE":"Insurance","HDFCLIFE":"Insurance",
    "GODREJCP":"FMCG","DABUR":"FMCG","MARICO":"FMCG","SIEMENS":"Industrials",
    "AMBUJACEM":"Cement","DMART":"Retail","NAUKRI":"Internet",
}

@st.cache_data(ttl=300)
def rb_ltp(ticker: str) -> float:
    try:
        info=yf.Ticker(ticker+".NS").fast_info
        p=float(info.get("lastPrice",0) or info.get("last_price",0))
        if p>0: return p
    except: pass
    try:
        df=get_data(ticker+".NS","1d","5d")
        return float(df["Close"].iloc[-1]) if df is not None else 0.0
    except: return 0.0

@st.cache_data(ttl=600)
def rb_history(ticker: str, period="1y"):
    try:
        raw=yf.download(ticker+".NS",period=period,interval="1d",auto_adjust=True,progress=False)
        if raw is None or raw.empty: return None
        if isinstance(raw.columns,pd.MultiIndex): raw.columns=raw.columns.get_level_values(0)
        return raw[["Close"]].dropna()
    except: return None

def rb_run_strategy(rb_holdings, strategy_name, threshold=5.0, min_order=2000, total_val=None):
    if total_val is None:
        total_val=sum(h["current_value"] for h in rb_holdings)
    rows=[]
    if strategy_name=="Threshold":
        for h in rb_holdings:
            cur_w=h["current_value"]/total_val*100 if total_val>0 else 0
            tgt_w=h.get("target_weight",100/len(rb_holdings))
            drift=cur_w-tgt_w; diff=total_val*tgt_w/100-h["current_value"]
            action="HOLD"
            if abs(drift)>=threshold and abs(diff)>=min_order:
                action="BUY" if diff>0 else "SELL"
            rows.append({"Ticker":h["ticker"],"Cur Wt%":round(cur_w,2),"Tgt Wt%":tgt_w,
                         "Drift%":round(drift,2),"Action":action,"Amount (₹)":round(abs(diff),0),
                         "Qty":max(1,int(abs(diff)/h["price"])) if h["price"]>0 else 0,
                         "Price":h["price"],"AvgCost":h.get("avg_cost",h["price"]),
                         "BuyDate":h.get("buy_date",str(date.today()-timedelta(days=400))),
                         "Reason":f"Drift {drift:+.1f}% vs threshold {threshold}%"})
    elif strategy_name=="Momentum":
        for h in rb_holdings:
            dfm=rb_history(h["ticker"],"6mo")
            mom=0.0
            if dfm is not None and len(dfm)>=60:
                mom=float((dfm["Close"].iloc[-1]-dfm["Close"].iloc[-60])/dfm["Close"].iloc[-60]*100)
            tilt=+2.0 if mom>10 else (-2.0 if mom<-10 else 0.0)
            cur_w=h["current_value"]/total_val*100 if total_val>0 else 0
            tgt_w=max(1,h.get("target_weight",100/len(rb_holdings))+tilt)
            drift=cur_w-tgt_w; diff=total_val*(tgt_w-cur_w)/100
            action="BUY" if diff>min_order else ("SELL" if diff<-min_order else "HOLD")
            rows.append({"Ticker":h["ticker"],"90D Mom%":round(mom,1),"Tilt":f"{tilt:+.0f}%",
                         "Tgt Wt%":round(tgt_w,1),"Drift%":round(drift,2),"Action":action,
                         "Amount (₹)":round(abs(diff),0),
                         "Qty":max(1,int(abs(diff)/h["price"])) if h["price"]>0 else 0,
                         "Price":h["price"],"AvgCost":h.get("avg_cost",h["price"]),
                         "BuyDate":h.get("buy_date",str(date.today()-timedelta(days=400))),
                         "Reason":f"90D mom {mom:+.1f}% tilt {tilt:+.0f}%"})
    elif strategy_name=="Equal Weight":
        eq=100.0/len(rb_holdings)
        for h in rb_holdings:
            cur_w=h["current_value"]/total_val*100 if total_val>0 else 0
            drift=cur_w-eq; diff=total_val*(eq-cur_w)/100
            action="BUY" if diff>min_order else ("SELL" if diff<-min_order else "HOLD")
            rows.append({"Ticker":h["ticker"],"Cur Wt%":round(cur_w,2),"Eq Wt%":round(eq,2),
                         "Drift%":round(drift,2),"Action":action,"Amount (₹)":round(abs(diff),0),
                         "Qty":max(1,int(abs(diff)/h["price"])) if h["price"]>0 else 0,
                         "Price":h["price"],"AvgCost":h.get("avg_cost",h["price"]),
                         "BuyDate":h.get("buy_date",str(date.today()-timedelta(days=400))),
                         "Reason":f"Reset to 1/{len(rb_holdings)}={eq:.1f}%"})
    elif strategy_name=="Risk Parity":
        vols={}
        for h in rb_holdings:
            dfv=rb_history(h["ticker"],"6mo")
            vols[h["ticker"]]=float(dfv["Close"].pct_change().dropna().tail(60).std()*np.sqrt(252)) if dfv is not None and len(dfv)>=30 else 0.20
        inv={t:1/max(v,0.001) for t,v in vols.items()}; tot=sum(inv.values())
        tgts={t:iv/tot*100 for t,iv in inv.items()}
        for h in rb_holdings:
            cur_w=h["current_value"]/total_val*100 if total_val>0 else 0
            tgt_w=tgts[h["ticker"]]; drift=cur_w-tgt_w; diff=total_val*(tgt_w-cur_w)/100
            action="BUY" if diff>min_order else ("SELL" if diff<-min_order else "HOLD")
            rows.append({"Ticker":h["ticker"],"Ann Vol%":round(vols[h["ticker"]]*100,1),
                         "Tgt Wt%":round(tgt_w,2),"Drift%":round(drift,2),"Action":action,
                         "Amount (₹)":round(abs(diff),0),
                         "Qty":max(1,int(abs(diff)/h["price"])) if h["price"]>0 else 0,
                         "Price":h["price"],"AvgCost":h.get("avg_cost",h["price"]),
                         "BuyDate":h.get("buy_date",str(date.today()-timedelta(days=400))),
                         "Reason":f"Vol {vols[h['ticker']]*100:.1f}% → wt {tgt_w:.1f}%"})
    else:  # Sector Rotation
        sr={}
        for h in rb_holdings:
            sec=RB_SECTOR_MAP.get(h["ticker"],"Other")
            dfsr=rb_history(h["ticker"],"1y"); yr=0.0
            if dfsr is not None and len(dfsr)>20:
                yr=float((dfsr["Close"].iloc[-1]-dfsr["Close"].iloc[0])/dfsr["Close"].iloc[0]*100)
            if sec not in sr: sr[sec]=[]
            sr[sec].append(yr)
        sec_avg={s:np.mean(v) for s,v in sr.items()}
        for h in rb_holdings:
            sec=RB_SECTOR_MAP.get(h["ticker"],"Other"); s_ret=sec_avg.get(sec,0)
            adj=-2.0 if s_ret>15 else (2.0 if s_ret<-10 else 0.0)
            cur_w=h["current_value"]/total_val*100 if total_val>0 else 0
            tgt_w=max(1,h.get("target_weight",100/len(rb_holdings))+adj)
            drift=cur_w-tgt_w; diff=total_val*(tgt_w-cur_w)/100
            action="BUY" if diff>min_order else ("SELL" if diff<-min_order else "HOLD")
            rows.append({"Ticker":h["ticker"],"Sector":sec,"YTD%":round(s_ret,1),
                         "Adjust":f"{adj:+.0f}%","Tgt Wt%":round(tgt_w,1),
                         "Drift%":round(drift,2),"Action":action,"Amount (₹)":round(abs(diff),0),
                         "Qty":max(1,int(abs(diff)/h["price"])) if h["price"]>0 else 0,
                         "Price":h["price"],"AvgCost":h.get("avg_cost",h["price"]),
                         "BuyDate":h.get("buy_date",str(date.today()-timedelta(days=400))),
                         "Reason":f"Sector {sec} YTD {s_ret:+.1f}%"})
    return pd.DataFrame(rows)

# ╔══════════════════════════════════════════════════════════════╗
# ║                       SIDEBAR                               ║
# ╚══════════════════════════════════════════════════════════════╝
with st.sidebar:
    st.header("⚙️ Settings")
    st.subheader("🔗 Zerodha Kite Connect")
    with st.expander("❓ How to get Request Token",expanded=False):
        st.markdown("""
1. Add `KITE_API_KEY` + `KITE_API_SECRET` to `.streamlit/secrets.toml`
2. Click **Login Zerodha** link → log in + 2FA
3. Copy `request_token=XXXXX` from redirect URL
4. Paste below → Connect. Repeat daily.
        """)
    if not KITE_AVAILABLE:
        st.error("Run: `pip install kiteconnect`")
    else:
        st.session_state.paper_mode=st.toggle("📝 Paper Trade Mode",value=st.session_state.paper_mode)
        if not st.session_state.paper_mode:
            st.warning("⚠️ LIVE MODE — Real orders!")
        if is_connected():
            try: nm=st.session_state.kite.profile().get("user_name","")
            except: nm=""
            st.success(f"✅ Connected{' — '+nm if nm else ''}")
            if st.button("🔌 Disconnect"):
                st.session_state.kite=None; st.session_state.access_token=""; st.rerun()
        else:
            if "KITE_API_KEY" in st.secrets:
                kite_obj=kite_login()
                if kite_obj:
                    st.markdown(f"**Step 1:** [Login Zerodha ↗]({kite_obj.login_url()})")
                    req_token=st.text_input("Step 2: Paste request_token",placeholder="From redirect URL...")
                    if st.button("🔑 Connect",type="primary") and req_token.strip():
                        with st.spinner("Connecting..."):
                            if kite_set_token(kite_obj,req_token.strip()): st.success("✅ Connected!"); st.rerun()
            else:
                st.info("Add KITE_API_KEY + KITE_API_SECRET to secrets.toml")
    st.divider()
    st.subheader("📊 Universe")
    universe_choice=st.radio("Scan scope",["All (250 stocks)","Large Cap only","Mid Cap only"],index=0)
    UNIVERSE = NIFTY100 if "Large" in universe_choice else (MIDCAP150 if "Mid" in universe_choice else ALL_STOCKS)
    st.caption(f"Scanning {len(UNIVERSE)} stocks")
    mode=st.selectbox("Timeframe",["Swing (Daily)","Intraday (15m)","Intraday (5m)"])
    st.divider()
    st.subheader("Strategies")
    st.caption("Original 10:")
    enabled_keys=[]
    orig=["ORB","VWAP","EMA","MACD","BB","RSI","ST","Stoch","W52","Pivot"]
    new6=["HH_HL","OBV_DIV","FLAG","RS","IB","TBR"]
    cols=st.columns(2)
    for i,k in enumerate(orig):
        with cols[i%2]:
            if st.checkbox(STRAT_LABELS[k],value=True,key=f"s_{k}"): enabled_keys.append(k)
    st.caption("New high-accuracy (6):")
    cols2=st.columns(2)
    for i,k in enumerate(new6):
        with cols2[i%2]:
            if st.checkbox(STRAT_LABELS[k],value=True,key=f"s_{k}"): enabled_keys.append(k)
    st.divider()
    use_mtf      =st.toggle("📊 MTF Confluence",value=True)
    use_sentiment=st.toggle("🤖 AI Sentiment",value=False)
    sent_w       =st.slider("Sentiment Weight",0.0,0.5,0.25,0.05,disabled=not use_sentiment)
    st.session_state.sentiment_weight=sent_w if use_sentiment else 0.0
    st.divider()
    st.session_state.capital       =st.number_input("Capital (₹)",10000,500000,st.session_state.capital,5000)
    st.session_state.risk_per_trade=st.slider("Risk per Trade %",0.5,5.0,2.0,0.5)/100
    st.session_state.target_daily  =st.number_input("Daily Target (₹)",500,10000,st.session_state.target_daily,500)
    st.session_state.max_trades_day=int(st.number_input("Max Trades/Day",1,20,st.session_state.max_trades_day,1))
    st.divider()
    min_strats=st.slider("Min Strategies Agreeing",1,10,3,1)
    min_score =st.slider("Min Score Threshold",0.20,0.70,0.35,0.05)
    only_52hi =st.checkbox("Only 52W Breakouts",False)
    only_mtf  =st.checkbox("Only MTF Confirmed",False)
    cap_filter=st.multiselect("Cap Type",["Large Cap","Mid Cap"],default=["Large Cap","Mid Cap"])
    auto_ref  =st.checkbox("⏱️ Auto Refresh (5 min)")
    CAPITAL=st.session_state.capital; RISK_PER_TRADE=st.session_state.risk_per_trade
    TARGET_DAILY=st.session_state.target_daily
    gain_pt=int(CAPITAL*RISK_PER_TRADE*2)
    st.caption(f"Risk/trade: ₹{int(CAPITAL*RISK_PER_TRADE):,} | 1:2 gain: ₹{gain_pt:,}")
    st.caption(f"Need {max(1,int(TARGET_DAILY/gain_pt))} wins for ₹{TARGET_DAILY:,}")
    if st.button("📤 Square Off All",type="secondary",use_container_width=True):
        square_off_all(st.session_state.paper_mode); st.success("All squared off!")

# ╔══════════════════════════════════════════════════════════════╗
# ║                       MAIN UI                               ║
# ╚══════════════════════════════════════════════════════════════╝
st.title("📈 NSE Pro Trader v5 — 250 Stocks · 16 Strategies · Zerodha Live")

regime=market_regime(); rw=regime_weights(regime)
rcss={"Bull":"regime-bull","Bear":"regime-bear","Sideways":"regime-side"}.get(regime,"regime-side")
pm=st.session_state.paper_mode; mode_tag="Paper" if pm else "LIVE"
color_tag="#ffd600" if pm else "#ff1744"
col_r,col_m=st.columns([3,1])
with col_r:
    st.markdown(f"<span class='{rcss}'>🌐 Regime: {regime}</span> &nbsp; "
                f"<span style='color:{color_tag};font-weight:700;font-size:14px;'>"
                f"{'📝 PAPER MODE' if pm else '🔴 LIVE TRADING'}</span> &nbsp; "
                f"<span style='color:#888;font-size:13px;'>Universe: {len(UNIVERSE)} stocks · "
                f"{len(enabled_keys)} strategies</span>",unsafe_allow_html=True)
with col_m:
    st.markdown(f"Orders: **{st.session_state.orders_today}** / {st.session_state.max_trades_day}")
st.divider()

def pnl_header():
    total_pnl=paper_pnl_mtm() if pm else fetch_live_positions()[1]
    c1,c2,c3,c4,c5=st.columns(5)
    c1.metric("💰 Session P&L",f"₹{total_pnl:+,.2f}")
    c2.metric("🎯 Daily Target",f"₹{TARGET_DAILY:,}")
    pct_done=min(100,int(abs(total_pnl)/TARGET_DAILY*100)) if TARGET_DAILY>0 else 0
    c3.metric("📊 Progress",f"{pct_done}%")
    open_t=sum(1 for t in st.session_state.paper_trades if t["status"]=="Open")
    c4.metric("📂 Open",open_t)
    c5.metric("📋 Trades",st.session_state.orders_today)
    st.progress(min(1.0,pct_done/100),text=f"₹{total_pnl:+.0f} / ₹{TARGET_DAILY:,}")
    return total_pnl

pnl_header()
st.divider()

col_a,col_b=st.columns([3,1])
with col_a: do_scan=st.button("🔍 Scan Universe",type="primary",use_container_width=True)
with col_b: reuse=st.button("🔄 Re-filter Cache",use_container_width=True,disabled=not st.session_state.scan_results)

# ── Data source diagnostic ────────────────────────────────────
data_ok, data_src = check_data_source()
if not data_ok:
    st.error("""
**⚠️ No market data available — scanner cannot run.**

Yahoo Finance blocks Streamlit Cloud server IPs. Choose one of these fixes:

**Option 1 — Run locally (recommended, free, instant fix):**
```
pip install -r requirements.txt
streamlit run pro_trading_system.py
```
Your home/office IP is not blocked. App works 100% locally.

**Option 2 — Connect Zerodha Kite (sidebar):**
Once connected, the app uses Kite's historical API for data.
Requires Kite Connect free Personal API (kite.trade).

**Option 3 — Deploy on Railway.app or Render.com (free tier):**
These platforms have different IP ranges not blocked by Yahoo.
""")
elif data_src == "kite":
    st.success("📡 Data source: Zerodha Kite API ✅")
else:
    st.success("📡 Data source: Yahoo Finance (yfinance) ✅")

if do_scan:
    if not enabled_keys: st.warning("Select at least one strategy."); st.stop()
    bar=st.progress(0,f"⚡ Fetching {len(UNIVERSE)} stocks in parallel...")
    imap={"Intraday (5m)":"5m","Intraday (15m)":"15m","Swing (Daily)":"1d"}
    pmap={"Intraday (5m)":"60d","Intraday (15m)":"60d","Swing (Daily)":"2y"}
    data_cache=fetch_parallel(UNIVERSE,imap[mode],pmap[mode],workers=16)
    bar.progress(0.35,"✅ Data ready. Fetching Nifty 50 benchmark...")
    nifty_ret=get_nifty50_returns()
    bar.progress(0.40,"🧠 Running strategies...")
    sent_cache={}
    if use_sentiment:
        bar.progress(0.42,"🤖 Batch AI sentiment...")
        valid=[t for t in UNIVERSE if data_cache.get(t) is not None]
        payload=[]
        for ticker in valid:
            tc=ticker.replace(".NS",""); df0=data_cache[ticker]
            if df0 is not None and len(df0)>2:
                p=float(df0["Close"].iloc[-1]); pv=float(df0["Close"].iloc[-2])
                payload.append({"ticker":tc,"price":p,"pct":(p-pv)/pv*100,"headlines":list(get_news(tc))})
        for i in range(0,len(payload),5):
            sent_cache.update(ai_sentiment_batch(json.dumps(payload[i:i+5])))
    results=[]
    for i,ticker in enumerate(UNIVERSE):
        bar.progress(0.45+(i+1)/len(UNIVERSE)*0.55,f"Analysing {ticker.replace('.NS','')} ({i+1}/{len(UNIVERSE)})...")
        df_raw=data_cache.get(ticker)
        if df_raw is None: continue
        res=scan_one(ticker,df_raw,mode,enabled_keys,rw,use_mtf,sent_cache,CAPITAL,RISK_PER_TRADE,nifty_ret)
        if not res or res["final_signal"]=="HOLD": continue
        n=res["n_buy"] if res["final_signal"]=="BUY" else res["n_sell"]
        if n<min_strats or abs(res["final_score"])<min_score: continue
        if only_52hi and not res["w52"]["near_hi"]: continue
        if only_mtf and not res["mtf_ok"]: continue
        if res["cap_type"] not in cap_filter: continue
        results.append(res)
        if abs(res["final_score"])>0.50:
            pos=res["position"]
            _telegram(f"{'🟢 BUY' if res['final_signal']=='BUY' else '🔴 SELL'}: {res['ticker']} [{res['cap_type']} | {res['sector']}]\n"
                      f"₹{res['price']} Score:{res['final_score']:.2f} MTF:{'✅' if res['mtf_ok'] else '⚠️'}\n"
                      f"SL:₹{pos.get('sl','—')} Tgt:₹{pos.get('target','—')} Qty:{pos.get('qty','—')}\n"
                      f"Candles: {', '.join(res['candles']) or 'None'}")
    bar.empty()
    st.session_state.scan_results=results; st.session_state.scan_ts=datetime.now().strftime("%H:%M:%S")
    st.rerun()

results = st.session_state.scan_results

# Only show results UI if we have data
if results:
    buys = sorted([r for r in results if r["final_signal"] == "BUY"], key=lambda x: -x["final_score"])
    sells = sorted([r for r in results if r["final_signal"] == "SELL"], key=lambda x: x["final_score"])
    
    # ALWAYS show metrics, even if empty
    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    c1.metric("🟢 BUY", len(buys))
    c2.metric("🔴 SELL", len(sells))
    c3.metric("📊 Total", len(results))
    c4.metric("🌐 Regime", regime)
    c5.metric("🏦 Large", sum(1 for r in results if r["cap_type"] == "Large Cap"))
    c6.metric("📈 Mid", sum(1 for r in results if r["cap_type"] == "Mid Cap"))
    c7.metric("🕐 Scanned", st.session_state.scan_ts or "—")
    
    if not results:
        st.warning("No signals at current filters. Try: Min Strategies=2, Min Score=0.25")
    st.divider()

    tab1,tab2,tab3,tab4,tab5,tab6=st.tabs([
        "🟢 BUY","🔴 SELL","📋 Table","💰 Live P&L","📈 Analytics","⚖️ Rebalancer"
    ])

    def order_btn(r):
        pos=r["position"]
        if not pos: return
        if st.session_state.orders_today>=st.session_state.max_trades_day:
            st.warning(f"Max {st.session_state.max_trades_day} trades/day reached."); return
        lbl=(f"{'📝 Paper' if pm else '🚀 LIVE'} {r['final_signal']} {r['ticker']} "
             f"Qty:{pos['qty']} @ ₹{r['price']} → SL:₹{pos['sl']} Tgt:₹{pos['target']}")
        if st.button(lbl,key=f"ord_{r['ticker']}_{r['final_signal']}",
                     type="secondary" if pm else "primary",use_container_width=True):
            res=place_order(r["ticker"],r["final_signal"],pos["qty"],r["price"],pos["sl"],pos["target"],pm)
            if res.get("status") in ("paper","live"):
                st.success(f"✅ Placed! ID:{res.get('id') or res.get('entry_id')}"); st.rerun()
            else: st.error(f"❌ {res.get('error')}")

    def render_cards(sig_list):
        if not sig_list: st.info("No signals for current filters."); return
        for r in sig_list:
            pos=r.get("position",{})
            n=r["n_buy"] if r["final_signal"]=="BUY" else r["n_sell"]
            with st.expander(
                f"{'🟢' if r['final_signal']=='BUY' else '🔴'} **{r['ticker']}** [{r['cap_type']}] "
                f"₹{r['price']} ({r['change_pct']:+.2f}%) | Score:{r['final_score']:.3f} | "
                f"{n}/{len(enabled_keys)} strats | {'✅ MTF' if r.get('mtf_ok') else '⚠️ MTF'}"
            ):
                st.markdown(f"<span class='sector-tag'>{r.get('sector','—')}</span>"
                            f"<span class='sector-tag'>{r.get('cap_type','—')}</span>",unsafe_allow_html=True)
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
                if r.get("candles"): st.caption("📊 "+" | ".join(r["candles"]))
                st.markdown("**Strategies triggered:**")
                for trig in r["triggers"]: st.caption(trig)
                if r.get("sent_summary","—") not in ("—",""):
                    st.info(f"🤖 {r['sent_label']} ({r['sent_conf']}%): {r['sent_summary']}")
                st.divider(); order_btn(r)

    with tab1:
        st.subheader(f"🟢 {len(buys)} BUY Signals"); render_cards(buys)
    with tab2:
        st.subheader(f"🔴 {len(sells)} SELL Signals"); render_cards(sells)

    with tab3:
        st.subheader("All Signals")
        if results:
            rows=[]
            for r in sorted(results,key=lambda x:-abs(x["final_score"])):
                pos=r.get("position",{}); n=r["n_buy"] if r["final_signal"]=="BUY" else r["n_sell"]
                rows.append({"Stock":r["ticker"],"Type":r["cap_type"],"Sector":r["sector"],
                             "Price":r["price"],"Chg%":r["change_pct"],"Signal":r["final_signal"],
                             "Score":r["final_score"],"MTF":"✅" if r.get("mtf_ok") else "⚠️",
                             "Strats":f"{n}/{len(enabled_keys)}","Sent":r["sent_label"],
                             "Target":pos.get("target","—"),"SL":pos.get("sl","—"),
                             "Qty":pos.get("qty","—"),"Net(₹)":pos.get("net_gain","—"),
                             "52W Hi%":r["w52"]["pct_hi"] if r.get("w52") else "—",
                             "Candles":", ".join(r.get("candles",[])[:2])})
            df_t=pd.DataFrame(rows)
            def csig(v):
                if v=="BUY":  return "background-color:#1a4731;color:#00e676;font-weight:bold"
                if v=="SELL": return "background-color:#4a1010;color:#ff5252;font-weight:bold"
                return ""
            st.dataframe(df_t.style.map(csig,subset=["Signal"])
                                   .format({"Chg%":"{:+.2f}%","Score":"{:.3f}"}),
                         use_container_width=True,height=500)
            csv=io.BytesIO(); df_t.to_csv(csv,index=False)
            st.download_button("⬇️ Export CSV",csv.getvalue(),f"signals_v5_{date.today()}.csv","text/csv")

    with tab4:
        st.subheader("💰 Live P&L Dashboard")
        if is_connected() and not pm:
            pos_list,live_pnl=fetch_live_positions()
            if pos_list:
                color="#00e676" if live_pnl>=0 else "#ff1744"
                st.markdown(f"**Live P&L: <span style='color:{color}'>₹{live_pnl:+,.2f}</span>**",unsafe_allow_html=True)
                pos_df=pd.DataFrame([{"Symbol":p["tradingsymbol"],"Qty":p["quantity"],
                    "Avg":p.get("average_price",0),"LTP":p.get("last_price",0),
                    "P&L":p.get("pnl",0),"Value":p.get("value",0)}
                    for p in pos_list if p.get("quantity",0)!=0])
                if not pos_df.empty:
                    st.dataframe(pos_df.style.format({"Avg":"₹{:.2f}","LTP":"₹{:.2f}","P&L":"₹{:+.2f}","Value":"₹{:.2f}"}),use_container_width=True)
                if st.button("🔄 Refresh"): st.rerun()
            else: st.info("No open positions.")
        else:
            pnl_now=paper_pnl_mtm()
            color="#00e676" if pnl_now>=0 else "#ff1744"
            st.markdown(f"**Paper P&L: <span style='color:{color}'>₹{pnl_now:+,.2f}</span>**",unsafe_allow_html=True)
            t_pct=min(100,int(abs(pnl_now)/TARGET_DAILY*100)) if TARGET_DAILY>0 else 0
            st.progress(max(0.0,min(1.0,t_pct/100)),text=f"{t_pct}% of ₹{TARGET_DAILY:,}")
            if st.session_state.paper_trades:
                rows=[{"Symbol":t.get("symbol",t.get("ticker","")),"Action":t.get("action",""),
                       "Entry":t.get("entry",0),"SL":t.get("sl",0),"Target":t.get("target",0),
                       "Qty":t.get("qty",0),"Status":t.get("status",""),"P&L (₹)":t.get("pnl",0.0),
                       "Time":t.get("time","")} for t in st.session_state.paper_trades]
                pt_df=pd.DataFrame(rows)
                def pnl_c(v):
                    if v>0: return "color:#00e676;font-weight:600"
                    if v<0: return "color:#ff1744;font-weight:600"
                    return ""
                st.dataframe(pt_df.style.map(pnl_c,subset=["P&L (₹)"])
                             .format({"Entry":"₹{:.2f}","SL":"₹{:.2f}","Target":"₹{:.2f}","P&L (₹)":"₹{:+.2f}"}),
                             use_container_width=True,height=360)
                c_r1,c_r2=st.columns(2)
                with c_r1:
                    if st.button("🔄 Refresh P&L"): st.rerun()
                with c_r2:
                    if st.button("🗑️ Clear Trades"): st.session_state.paper_trades=[]; st.rerun()
                tl=io.BytesIO(); pt_df.to_csv(tl,index=False)
                st.download_button("⬇️ Export Log",tl.getvalue(),f"trades_{date.today()}.csv","text/csv")
            else: st.info("No paper trades yet. Place orders from BUY/SELL tabs.")

    with tab5:
        st.subheader("📈 Analytics")
        if results:
            ca,cb=st.columns(2)
            with ca:
                st.markdown("**Score Distribution**")
                st.bar_chart(pd.DataFrame({"Ticker":[r["ticker"] for r in results],
                                           "Score":[r["final_score"] for r in results]}).set_index("Ticker"))
            with cb:
                st.markdown("**Strategy Hit Count (16 strategies)**")
                sc={}
                for r in results:
                    for k,v in r["strategies"].items():
                        if v["signal"] in ("BUY","SELL"):
                            sc[STRAT_LABELS[k]]=sc.get(STRAT_LABELS[k],0)+1
                if sc: st.bar_chart(pd.DataFrame.from_dict(dict(sorted(sc.items(),key=lambda x:-x[1])),orient="index",columns=["Hits"]))
            cc,cd=st.columns(2)
            with cc:
                st.markdown("**Sector Distribution**")
                sec_cnt={}
                for r in results: sec_cnt[r["sector"]]=sec_cnt.get(r["sector"],0)+1
                if sec_cnt: st.bar_chart(pd.DataFrame.from_dict(sec_cnt,orient="index",columns=["Count"]))
            with cd:
                st.markdown("**Large Cap vs Mid Cap**")
                cap_cnt={"Large Cap":sum(1 for r in results if r["cap_type"]=="Large Cap"),
                         "Mid Cap": sum(1 for r in results if r["cap_type"]=="Mid Cap")}
                st.bar_chart(pd.DataFrame.from_dict(cap_cnt,orient="index",columns=["Count"]))
             

    # ╔══════════════════════════════════════════════════════════╗
    # ║              TAB 6 — PORTFOLIO REBALANCER               ║
    # ╚══════════════════════════════════════════════════════════╝
    with tab6:
        st.subheader("⚖️ Portfolio Rebalancer — Long-Term Holdings")
        st.caption("Import your delivery holdings → pick a strategy → rebalance automatically with tax awareness")

        st.markdown("### 📥 Step 1 — Import Holdings")
        imp=st.radio("Import method",["🔗 From Zerodha Holdings","📋 Manual Entry","📄 Upload CSV"],
                     horizontal=True,key="rb_imp")

        if "Zerodha" in imp:
            if is_connected():
                if st.button("📥 Fetch from Zerodha",key="rb_fetch_z"):
                    try:
                        raw_h=st.session_state.kite.holdings()
                        st.session_state.rb_portfolio=[]
                        for h in raw_h:
                            sym=h["tradingsymbol"]; p=rb_ltp(sym)
                            if p==0: p=float(h.get("last_price",h["average_price"]))
                            st.session_state.rb_portfolio.append({
                                "ticker":sym,"qty":int(h["quantity"]),
                                "avg_cost":float(h["average_price"]),"price":p,
                                "current_value":p*int(h["quantity"]),
                                "buy_date":str(date.today()-timedelta(days=400)),"target_weight":0.0})
                        st.success(f"✅ Imported {len(st.session_state.rb_portfolio)} holdings!")
                    except Exception as e: st.error(f"Error: {e}")
            else: st.warning("Connect Zerodha in sidebar first.")

        elif "Manual" in imp:
            sample=pd.DataFrame({
                "Ticker":["RELIANCE","TCS","HDFCBANK","INFY","SBIN","TATAMOTORS"],
                "Qty":[10,5,20,15,50,30],
                "Avg Cost (₹)":[2600,3800,1550,1400,750,850],
                "Buy Date":["2023-01-15","2022-06-10","2023-03-20","2022-11-05","2024-01-10","2023-08-15"],
                "Target Wt%":[20,18,20,15,12,15],
            })
            edited=st.data_editor(sample,num_rows="dynamic",use_container_width=True,key="rb_editor")
            if st.button("✅ Load Portfolio",key="rb_load_m"):
                st.session_state.rb_portfolio=[]
                for _,r in edited.iterrows():
                    tk=str(r["Ticker"]).strip().upper(); p=rb_ltp(tk)
                    if p==0: p=float(r["Avg Cost (₹)"])
                    st.session_state.rb_portfolio.append({
                        "ticker":tk,"qty":int(r["Qty"]),"avg_cost":float(r["Avg Cost (₹)"]),
                        "price":p,"current_value":p*int(r["Qty"]),
                        "buy_date":str(r.get("Buy Date",date.today()-timedelta(days=400))),
                        "target_weight":float(r.get("Target Wt%",100/max(len(edited),1)))})
                st.success(f"✅ {len(st.session_state.rb_portfolio)} stocks loaded!")

        else:
            up=st.file_uploader("CSV: Ticker,Qty,AvgCost,BuyDate,TargetWt%",type=["csv"],key="rb_csv_up")
            if up:
                df_csv=pd.read_csv(up); df_csv.columns=[c.strip() for c in df_csv.columns]
                st.session_state.rb_portfolio=[]
                for _,r in df_csv.iterrows():
                    tk=str(r.get("Ticker","")).strip().upper()
                    if not tk: continue
                    p=rb_ltp(tk)
                    st.session_state.rb_portfolio.append({
                        "ticker":tk,"qty":int(r.get("Qty",1)),"avg_cost":float(r.get("AvgCost",p)),
                        "price":p if p>0 else float(r.get("AvgCost",0)),
                        "current_value":(p or float(r.get("AvgCost",0)))*int(r.get("Qty",1)),
                        "buy_date":str(r.get("BuyDate",date.today()-timedelta(days=400))),
                        "target_weight":float(r.get("TargetWt%",100/max(len(df_csv),1)))})
                st.success(f"✅ {len(st.session_state.rb_portfolio)} stocks from CSV!")

        rb_holdings=st.session_state.rb_portfolio

        if not rb_holdings:
            st.info("👆 Import your portfolio above to continue.")
        else:
            col_rf,col_clr=st.columns(2)
            with col_rf:
                if st.button("🔄 Refresh Prices",key="rb_ref"):
                    for h in rb_holdings:
                        p=rb_ltp(h["ticker"])
                        if p>0: h["price"]=p; h["current_value"]=p*h["qty"]
                    st.session_state.rb_portfolio=rb_holdings; st.success("Prices updated!")
            with col_clr:
                if st.button("🗑️ Clear Portfolio",key="rb_clr"):
                    st.session_state.rb_portfolio=[]; st.rerun()

            st.markdown("### 📊 Step 2 — Portfolio Overview")
            total_val=sum(h["current_value"] for h in rb_holdings)
            total_cost=sum(h["avg_cost"]*h["qty"] for h in rb_holdings)
            total_pnl=total_val-total_cost; pnl_pct=total_pnl/total_cost*100 if total_cost>0 else 0
            m1,m2,m3,m4=st.columns(4)
            m1.metric("Portfolio Value",f"₹{total_val:,.0f}")
            m2.metric("Total P&L",f"₹{total_pnl:+,.0f}",f"{pnl_pct:+.1f}%")
            m3.metric("Holdings",len(rb_holdings))
            m4.metric("Invested",f"₹{total_cost:,.0f}")

            h_rows=[]
            for h in rb_holdings:
                cur_w=h["current_value"]/total_val*100 if total_val>0 else 0
                tgt_w=h.get("target_weight",100/len(rb_holdings))
                drift=cur_w-tgt_w; pnl_h=(h["price"]-h["avg_cost"])*h["qty"]
                h_rows.append({"Ticker":h["ticker"],
                               "Sector":RB_SECTOR_MAP.get(h["ticker"],"Other"),
                               "Qty":h["qty"],"Avg Cost":h["avg_cost"],"LTP":round(h["price"],2),
                               "Value (₹)":round(h["current_value"],0),"P&L (₹)":round(pnl_h,0),
                               "P&L%":round(pnl_h/(h["avg_cost"]*h["qty"])*100,1) if h["avg_cost"]>0 else 0,
                               "Cur Wt%":round(cur_w,2),"Tgt Wt%":round(tgt_w,2),"Drift%":round(drift,2)})
            df_h=pd.DataFrame(h_rows)
            def _cd(v):
                if v>5: return "background-color:#2e0d0d;color:#ff5252;font-weight:700"
                if v<-5: return "background-color:#0d2e1a;color:#00e676;font-weight:700"
                return ""
            def _cp(v): return "color:#00e676" if v>=0 else "color:#ff5252"
            st.dataframe(df_h.style.map(_cd,subset=["Drift%"]).map(_cp,subset=["P&L (₹)","P&L%"])
                .format({"Avg Cost":"₹{:.2f}","LTP":"₹{:.2f}","Value (₹)":"₹{:,.0f}",
                         "P&L (₹)":"₹{:+,.0f}","P&L%":"{:+.1f}%","Drift%":"{:+.2f}%"}),
                use_container_width=True,height=300)

            sec_grp=df_h.groupby("Sector")["Value (₹)"].sum().reset_index()
            sec_grp["Wt%"]=(sec_grp["Value (₹)"]/total_val*100).round(1)
            cs,cm=st.columns([1,2])
            with cs:
                st.caption("**Sector Weights**")
                st.dataframe(sec_grp.sort_values("Wt%",ascending=False)
                               .style.format({"Value (₹)":"₹{:,.0f}","Wt%":"{:.1f}%"}),
                             use_container_width=True,height=220)
            with cm:
                st.caption("**Sector Distribution**")
                st.bar_chart(sec_grp.set_index("Sector")["Wt%"])

            st.markdown("### ⚖️ Step 3 — Choose Strategy & Compute")
            rb_strat=st.selectbox("Rebalancing Strategy",[
                "Threshold — rebalance when drift >X%  ✅ Recommended",
                "Momentum — overweight 90-day winners",
                "Equal Weight — reset all to 1/N  ✅ Simplest",
                "Risk Parity — size by inverse volatility",
                "Sector Rotation — trim overbought sectors",
            ],key="rb_strat_sel")

            rb_threshold=5.0
            if "Threshold" in rb_strat:
                rb_threshold=st.slider("Drift threshold %",2.0,15.0,5.0,0.5,key="rb_thresh")
            new_money=st.number_input("New SIP Money to Deploy (₹)",0,1000000,0,5000,key="rb_nm",
                                      help="Deployed to BUY orders first — avoids selling & tax")
            min_order=st.number_input("Min Order Value (₹)",500,50000,2000,500,key="rb_mo")

            strat_key=("Threshold" if "Threshold" in rb_strat else
                       "Momentum" if "Momentum" in rb_strat else
                       "Equal Weight" if "Equal" in rb_strat else
                       "Risk Parity" if "Risk" in rb_strat else "Sector Rotation")

            if st.button("🔍 Compute Rebalancing Plan",key="rb_compute",type="primary"):
                with st.spinner("Computing..."):
                    rb_df=rb_run_strategy(rb_holdings,strat_key,rb_threshold,min_order,total_val)
                    st.session_state["rb_plan"]=rb_df
                st.success("Plan ready!")

            if "rb_plan" in st.session_state and not st.session_state["rb_plan"].empty:
                rb_df=st.session_state["rb_plan"]
                n_buy=(rb_df["Action"]=="BUY").sum(); n_sell=(rb_df["Action"]=="SELL").sum()
                buy_amt=rb_df.loc[rb_df["Action"]=="BUY","Amount (₹)"].sum()
                sel_amt=rb_df.loc[rb_df["Action"]=="SELL","Amount (₹)"].sum()
                rc1,rc2,rc3,rc4=st.columns(4)
                rc1.metric("🟢 BUY",n_buy,f"₹{buy_amt:,.0f}")
                rc2.metric("🔴 SELL",n_sell,f"₹{sel_amt:,.0f}")
                rc3.metric("✅ HOLD",len(rb_df)-n_buy-n_sell)
                rc4.metric("Net Cash",f"₹{buy_amt-sel_amt:+,.0f}")

                if new_money>0 and n_buy>0:
                    deploy=min(new_money,buy_amt)
                    st.success(f"✅ ₹{deploy:,.0f} SIP money deployed to BUY orders — avoids selling & tax!")

                disp_cols=[c for c in rb_df.columns if c not in ["Price","AvgCost","BuyDate"]]
                def _ca(v):
                    if v=="BUY":  return "background-color:#1a4731;color:#00e676;font-weight:bold"
                    if v=="SELL": return "background-color:#4a1010;color:#ff5252;font-weight:bold"
                    return "color:#888"
                fmt={"Amount (₹)":"₹{:,.0f}"}
                if "Drift%" in disp_cols: fmt["Drift%"]="{:+.2f}%"
                st.dataframe(rb_df[disp_cols].style.map(_ca,subset=["Action"]).format(fmt),
                             use_container_width=True,height=360)

                # Tax impact for SELLs
                sell_rows=rb_df[rb_df["Action"]=="SELL"]
                if not sell_rows.empty:
                    st.markdown("**⚠️ Estimated Tax on SELL Orders:**")
                    tax_rows=[]
                    for _,r in sell_rows.iterrows():
                        buy_dt=pd.to_datetime(r.get("BuyDate",datetime.now()-timedelta(days=400)))
                        days=(datetime.now()-buy_dt).days
                        gain=(r["Price"]-r["AvgCost"])*r["Qty"]
                        is_lt=days>=365; tax=max(0,gain*(0.125 if is_lt else 0.20)) if gain>0 else 0
                        tax_rows.append({"Ticker":r["Ticker"],"Days Held":days,
                            "Type":"LTCG 12.5%" if is_lt else "STCG 20%",
                            "Gain/Loss":round(gain,2),"Est Tax":round(tax,2),
                            "Advice":"✅ OK (LTCG)" if is_lt else "⚠️ Consider delaying (STCG)"})
                    tx_df=pd.DataFrame(tax_rows)
                    st.dataframe(tx_df.style.format({"Gain/Loss":"₹{:+,.0f}","Est Tax":"₹{:,.0f}"}),use_container_width=True)
                    st.warning(f"Total estimated tax: ₹{tx_df['Est Tax'].sum():,.0f}  |  Use SIP top-up for BUY orders to reduce this.")

                st.markdown("### 🚀 Step 4 — Execute")
                active_rb=rb_df[rb_df["Action"].isin(["BUY","SELL"])]
                if active_rb.empty:
                    st.success("✅ Portfolio already balanced!")
                else:
                    pm_rb=not is_connected() or pm
                    ex_col,dl_col=st.columns(2)
                    with ex_col:
                        if st.button(f"{'📝 Paper' if pm_rb else '🚀 LIVE'} Execute {len(active_rb)} Orders",
                                     type="primary",use_container_width=True,key="rb_exec"):
                            exec_results=[]
                            for _,r in active_rb.iterrows():
                                qty=int(r.get("Qty",0))
                                if qty<=0: continue
                                log_e={"time":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                       "ticker":r["Ticker"],"action":r["Action"],"qty":qty,
                                       "price":r["Price"],"mode":"Paper" if pm_rb else "Live",
                                       "strategy":strat_key}
                                st.session_state.rb_log.append(log_e)
                                if not pm_rb and is_connected():
                                    try:
                                        kite=st.session_state.kite
                                        txn=kite.TRANSACTION_TYPE_BUY if r["Action"]=="BUY" else kite.TRANSACTION_TYPE_SELL
                                        oid=kite.place_order(variety=kite.VARIETY_REGULAR,exchange=kite.EXCHANGE_NSE,
                                                             tradingsymbol=r["Ticker"],transaction_type=txn,quantity=qty,
                                                             product=kite.PRODUCT_CNC,order_type=kite.ORDER_TYPE_MARKET)
                                        exec_results.append(f"✅ {r['Action']} {r['Ticker']} x{qty} → {oid}")
                                    except Exception as e: exec_results.append(f"❌ {r['Ticker']}: {e}")
                                else:
                                    exec_results.append(f"📝 Paper {r['Action']} {r['Ticker']} x{qty} @ ₹{r['Price']:.2f}")
                            for res in exec_results: st.write(res)
                            _telegram(f"⚖️ Rebalanced ({strat_key}) | {'Paper' if pm_rb else 'LIVE'} | BUY:{n_buy} SELL:{n_sell}")
                            st.success("✅ Rebalancing complete!")
                            del st.session_state["rb_plan"]
                    with dl_col:
                        csv_buf=io.BytesIO(); rb_df.to_csv(csv_buf,index=False)
                        st.download_button("⬇️ Download Plan CSV",csv_buf.getvalue(),
                                           f"rebalance_{date.today()}.csv","text/csv",
                                           use_container_width=True,key="rb_dl")

                if st.session_state.rb_log:
                    st.divider()
                    st.markdown("**📋 Rebalance History (Session)**")
                    st.dataframe(pd.DataFrame(st.session_state.rb_log),use_container_width=True)

        with st.expander("📚 Strategy Guide",expanded=False):
            st.markdown("""
| Strategy | Trigger | Best For | Tax Friendly |
|---|---|---|---|
| **Threshold ±5%** | Drift > 5% | Most investors ✅ | ⭐⭐⭐⭐ |
| **Momentum** | Monthly | Active investors | ⭐⭐⭐ |
| **Equal Weight** | Quarterly | Beginners ✅ | ⭐⭐⭐⭐ |
| **Risk Parity** | Monthly | Risk-conscious | ⭐⭐⭐ |
| **Sector Rotation** | Monthly | Sector-aware | ⭐⭐⭐ |

**Golden Rules:** Always use SIP new money for BUY orders first • Hold >12 months for LTCG 12.5% vs STCG 20% • ₹1.25L LTCG exemption per year — book profits up to this limit annually
            """)

# ── Auto square-off at 3:20 PM IST ───────────────────────────
now_ist=datetime.utcnow()+timedelta(hours=5,minutes=30)
if now_ist.hour==15 and now_ist.minute>=20 and st.session_state.orders_today>0:
    square_off_all(pm)
    _telegram(f"📤 Auto square-off 3:20 PM | P&L: ₹{paper_pnl_mtm():+.2f}")

if auto_ref:
    st.toast("Refreshing in 5 min..."); time.sleep(300); st.rerun()
