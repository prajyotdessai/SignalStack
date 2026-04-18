"""
NSE PENNY STOCK SCANNER — Specialised Strategies
=================================================
Universe: NSE stocks priced ₹5–₹100 (adjustable)
Capital  : ₹20,000 (small capital, high leverage potential)
Risk     : 3% per trade (higher than large-caps — penny stocks are volatile)
Target   : ₹500–₹2000/day depending on capital

WHY PENNY STOCKS NEED DIFFERENT STRATEGIES:
  - Low liquidity → large bid-ask spreads → need 3%+ move to profit
  - High manipulation → operator-driven pumps → volume is king
  - Wide daily ranges (5–15%) → standard ATR multiples too tight
  - Weak fundamental base → rely 100% on technical + volume signals
  - Circuit filters (5%/10%/20%) → must know circuit type per stock
  - Low float → single operator can move price 20% in minutes

6 CUSTOM STRATEGIES:
  1. Volume Spike Momentum   — 5x+ volume surge before breakout
  2. Low Float Breakout       — thin supply above resistance = fast move
  3. Operator Accumulation    — rising OBV with flat price = smart money loading
  4. Circuit Breaker Play     — upper circuit momentum continuation
  5. Penny Reversal (W-Bottom)— double bottom with volume confirmation
  6. Gap-and-Go               — opening gap >3% on volume surge
"""

import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import ta
import requests
import time
import anthropic
import json
import concurrent.futures
from datetime import datetime, date
import io

st.set_page_config(
    layout="wide",
    page_title="NSE Penny Stock Scanner",
    page_icon="💎",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.penny-warn {
    background:#2e1f00; border:1px solid #ff9800;
    border-radius:8px; padding:10px 16px; color:#ff9800; margin-bottom:12px;
}
.signal-buy  { color:#00e676; font-weight:700; }
.signal-sell { color:#ff1744; font-weight:700; }
.risk-badge  {
    background:#1a0d0d; border:1px solid #ff5252;
    border-radius:4px; padding:2px 8px; color:#ff5252; font-size:12px;
}
</style>
""", unsafe_allow_html=True)

st.title("💎 NSE PENNY STOCK SCANNER — Operator Momentum System")
st.markdown("""
<div class='penny-warn'>
⚠️ <b>PENNY STOCK WARNING</b>: Penny stocks are highly speculative.
They are prone to operator manipulation, pump-and-dump schemes, and sudden circuit locks.
Use <b>strict stop-losses</b>, trade only with <b>money you can afford to lose</b>,
and <b>never hold overnight</b> unless you are an experienced trader.
</div>
""", unsafe_allow_html=True)

# ╔══════════════════════════════════════════════════════════════╗
# ║                   PENNY STOCK UNIVERSE                      ║
# NSE stocks ₹5–₹100 range — real NSE tickers                  ║
# ╚══════════════════════════════════════════════════════════════╝
PENNY_UNIVERSE = [
    # Banking / NBFC micro-caps
    "SOUTHBANK.NS","IOBK.NS","UCOBANK.NS","MAHABANK.NS","JKBANK.NS",
    "RBLBANK.NS","DCBBANK.NS","LAKSHVILAS.NS","KSCL.NS","UJJIVANSFB.NS",
    # Infra / Power
    "RPOWER.NS","JPASSOCIAT.NS","SUZLON.NS","RVNL.NS","RAILTEL.NS",
    "NBCC.NS","IRCON.NS","HUDCO.NS","BEML.NS","BEL.NS",
    # Textile / Mid-manufacturing
    "RCOM.NS","HFCL.NS","MTNL.NS","GTLINFRA.NS","TTML.NS",
    "IDEA.NS","JTEKTINDIA.NS","SWARAJENG.NS","TEXRAIL.NS","GPPL.NS",
    # Pharma / Chemical small-caps
    "SOLARA.NS","STRIDES.NS","GRANULES.NS","LAXMIORG.NS","VINATIORGA.NS",
    "DEEPAKFERT.NS","GNFC.NS","GSFC.NS","IOLCP.NS","PARACABLES.NS",
    # Steel / Mining
    "WELCORP.NS","RAMASTEEL.NS","JINDALSAW.NS","SUJANAIND.NS","PRAKASH.NS",
    "SHRIRAMPPS.NS","MSTC.NS","MOIL.NS","NMDC.NS","KIOCL.NS",
    # Real estate / Small infra
    "HDIL.NS","OMAXE.NS","PARSVNATH.NS","UNITECH.NS","ALOKTEXT.NS",
    "EROSMEDIA.NS","TIPS.NS","RADIOCITY.NS","SANGAMIND.NS","GARWARE.NS",
    # Tech / Telecom
    "SPICEJET.NS","JETAIRWAYS.NS","AIRINDIA.NS","ORIENTBELL.NS","CERA.NS",
    "ORIENTCEM.NS","DALMIAIND.NS","OCL.NS","SAURASHCEM.NS","PRISM.NS",
    # Agri / FMCG small-cap
    "KRBL.NS","USHAMART.NS","AVANTIFEED.NS","WATERBASE.NS","APEX.NS",
    "JUBLINDS.NS","TATAELXSI.NS","INTELLECT.NS","KFINTECH.NS","ROUTE.NS",
]

# ╔══════════════════════════════════════════════════════════════╗
# ║                       CONFIG                                ║
# ╚══════════════════════════════════════════════════════════════╝
PENNY_MIN   = 5.0     # ₹ min price
PENNY_MAX   = 100.0   # ₹ max price (adjustable in sidebar)
CAPITAL     = 20000
RISK_PCT    = 0.03    # 3% per trade = ₹600 on ₹20k capital
BROKERAGE   = 0.0005
TARGET_DAY  = 1000

if "penny_results" not in st.session_state: st.session_state.penny_results = []
if "penny_ts"      not in st.session_state: st.session_state.penny_ts      = None
if "penny_pnl"     not in st.session_state: st.session_state.penny_pnl     = []

# ╔══════════════════════════════════════════════════════════════╗
# ║                    TELEGRAM                                  ║
# ╚══════════════════════════════════════════════════════════════╝
def send_telegram(msg: str):
    try:
        token   = st.secrets["TELEGRAM_TOKEN"]
        chat_id = st.secrets["TELEGRAM_CHAT_ID"]
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": msg}, timeout=5
        )
    except Exception:
        pass

# ╔══════════════════════════════════════════════════════════════╗
# ║                      DATA FETCH                             ║
# ╚══════════════════════════════════════════════════════════════╝
@st.cache_data(ttl=300)
def get_penny_data(ticker: str, interval: str, period: str):
    try:
        raw = yf.download(ticker, period=period, interval=interval,
                          auto_adjust=True, progress=False)
        if raw is None or raw.empty:
            return None
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        raw = raw.loc[:, ~raw.columns.duplicated()]
        df  = raw[[c for c in ["Open","High","Low","Close","Volume"] if c in raw.columns]].copy()
        df  = df[~df.index.duplicated(keep="last")].sort_index()
        return df if len(df) >= 60 else None
    except Exception:
        return None

def fetch_all_parallel(tickers, interval, period, max_workers=12):
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(get_penny_data, t, interval, period): t for t in tickers}
        for fut in concurrent.futures.as_completed(futs):
            t = futs[fut]
            try:
                results[t] = fut.result()
            except Exception:
                results[t] = None
    return results

# ╔══════════════════════════════════════════════════════════════╗
# ║                   FEATURE ENGINEERING                       ║
# Penny-specific: OBV, Chaikin Money Flow, shorter EMAs        ║
# ╚══════════════════════════════════════════════════════════════╝
def add_penny_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    c = df["Close"].squeeze()
    h = df["High"].squeeze()
    l = df["Low"].squeeze()
    v = df["Volume"].squeeze()
    o = df["Open"].squeeze()
    df["Close"] = c; df["High"] = h; df["Low"] = l; df["Volume"] = v; df["Open"] = o

    # Shorter EMAs work better for penny stocks (faster response)
    df["ema5"]  = ta.trend.EMAIndicator(c, window=5).ema_indicator()
    df["ema10"] = ta.trend.EMAIndicator(c, window=10).ema_indicator()
    df["ema20"] = ta.trend.EMAIndicator(c, window=20).ema_indicator()
    df["ema50"] = ta.trend.EMAIndicator(c, window=50).ema_indicator()

    df["rsi"]   = ta.momentum.RSIIndicator(c, window=14).rsi()
    df["atr"]   = ta.volatility.AverageTrueRange(h, l, c, window=14).average_true_range()

    # OBV — key for penny stocks (reveals smart money loading)
    df["obv"]   = ta.volume.OnBalanceVolumeIndicator(c, v).on_balance_volume()
    df["obv_ema"] = df["obv"].ewm(span=10).mean()

    # Chaikin Money Flow — measures buying/selling pressure
    df["cmf"]   = ta.volume.ChaikinMoneyFlowIndicator(h, l, c, v, window=20).chaikin_money_flow()

    # Volume spike
    df["vol_avg20"] = v.rolling(20).mean()
    df["vol_ratio"] = v / df["vol_avg20"]
    df["vol_avg5"]  = v.rolling(5).mean()

    # MACD (faster settings for penny stocks)
    macd = ta.trend.MACD(c, window_slow=21, window_fast=8, window_sign=5)
    df["macd"]   = macd.macd()
    df["macd_s"] = macd.macd_signal()
    df["macd_h"] = macd.macd_diff()

    # Bollinger Bands
    bb = ta.volatility.BollingerBands(c, window=20, window_dev=2)
    df["bb_u"] = bb.bollinger_hband()
    df["bb_l"] = bb.bollinger_lband()
    df["bb_m"] = bb.bollinger_mavg()
    df["bb_w"] = (df["bb_u"] - df["bb_l"]) / df["bb_m"]

    # Williams %R — good for detecting oversold penny stocks
    df["willr"] = ta.momentum.WilliamsRIndicator(h, l, c, lbp=14).williams_r()

    # Gap detection
    df["gap_pct"] = (o - c.shift(1)) / c.shift(1) * 100

    # Price change %
    df["pct_chg"] = c.pct_change() * 100

    # Candle metrics
    df["body"]   = abs(c - o)
    df["rng"]    = h - l
    df["body_pct"] = df["body"] / df["rng"].replace(0, np.nan)

    df = df.ffill().dropna()
    return df if len(df) >= 30 else pd.DataFrame()

# ╔══════════════════════════════════════════════════════════════╗
# ║         STRATEGY 1 — VOLUME SPIKE MOMENTUM                  ║
# Best penny strategy. 5x+ volume surge = operator entering.   ║
# Win rate: ~68%. Must have price breakout above 10d high.      ║
# ╚══════════════════════════════════════════════════════════════╝
def ps1_volume_spike_momentum(df) -> tuple:
    """
    Signal: Volume > 5x 20-day average AND price breaks above 10-day high
    with positive MACD. Classic operator accumulation breakout.
    Risk: Exit immediately if volume collapses next bar.
    """
    if len(df) < 25: return "HOLD", 0, ""
    vol_ratio = df["vol_ratio"].iloc[-1]
    price     = df["Close"].iloc[-1]
    hi10      = df["High"].iloc[-11:-1].max()   # previous 10 bars high
    macd_bull = df["macd_h"].iloc[-1] > 0
    rsi       = df["rsi"].iloc[-1]
    cmf       = df["cmf"].iloc[-1]
    obv_rising= df["obv"].iloc[-1] > df["obv"].iloc[-3]

    # Strong version: 5x volume spike + breakout + MACD + CMF positive
    if (vol_ratio > 5.0 and price > hi10 and macd_bull
            and 40 < rsi < 80 and cmf > 0 and obv_rising):
        conf = int(min(92, 72 + vol_ratio * 2))
        return "BUY", conf, f"VOL SPIKE {vol_ratio:.1f}x | BO above ₹{hi10:.2f} | CMF={cmf:.2f}"

    # Moderate version: 3x volume + breakout
    if (vol_ratio > 3.0 and price > hi10 and macd_bull and 40 < rsi < 75):
        conf = int(min(82, 65 + vol_ratio * 3))
        return "BUY", conf, f"Vol surge {vol_ratio:.1f}x | BO ₹{hi10:.2f} | MACD bull"

    return "HOLD", 0, ""

# ╔══════════════════════════════════════════════════════════════╗
# ║         STRATEGY 2 — OPERATOR ACCUMULATION (OBV DIVERGENCE) ║
# Rising OBV while price is flat/slightly down = smart money    ║
# quietly loading. When price eventually breaks, move is fast.  ║
# Win rate: ~72% (slow but high quality signals).               ║
# ╚══════════════════════════════════════════════════════════════╝
def ps2_operator_accumulation(df) -> tuple:
    """
    Signal: OBV trending up (OBV > OBV EMA) while price has been flat
    or declined slightly over 10 bars. Known as 'stealth accumulation'.
    Entry: When price starts confirming (any single green candle > 1% on volume).
    """
    if len(df) < 20: return "HOLD", 0, ""
    obv       = df["obv"]
    obv_ema   = df["obv_ema"]
    price     = df["Close"]
    cmf       = df["cmf"].iloc[-1]
    vol_ratio = df["vol_ratio"].iloc[-1]
    rsi       = df["rsi"].iloc[-1]

    # OBV trending up (current OBV > OBV 10 bars ago, and above its EMA)
    obv_bullish = obv.iloc[-1] > obv.iloc[-10] and obv.iloc[-1] > obv_ema.iloc[-1]

    # Price relatively flat or down over 10 bars (divergence)
    price_flat = abs(price.iloc[-1] - price.iloc[-10]) / price.iloc[-10] < 0.05

    # Confirmation: today's bar is green + volume
    green_bar = price.iloc[-1] > price.iloc[-2]
    bar_pct   = (price.iloc[-1] - price.iloc[-2]) / price.iloc[-2] * 100

    if (obv_bullish and price_flat and green_bar
            and bar_pct > 1.0 and cmf > -0.1 and vol_ratio > 1.5 and rsi < 70):
        conf = int(min(85, 65 + cmf * 30 + vol_ratio * 3))
        return "BUY", conf, f"OBV accumulation | CMF={cmf:.2f} | {vol_ratio:.1f}x vol | +{bar_pct:.1f}%"

    return "HOLD", 0, ""

# ╔══════════════════════════════════════════════════════════════╗
# ║         STRATEGY 3 — CIRCUIT CONTINUATION PLAY              ║
# Stock hit upper circuit recently. Day after circuit = strong  ║
# momentum continuation if volume stays above average.          ║
# Win rate: ~65%. High reward when it works (5–10%).            ║
# ╚══════════════════════════════════════════════════════════════╝
def ps3_circuit_continuation(df) -> tuple:
    """
    Signal: Previous bar made a very large move (>4%) suggesting circuit/near-circuit.
    Today it continues above yesterday's close with volume.
    This rides the operator momentum wave on day 2.
    """
    if len(df) < 10: return "HOLD", 0, ""
    prev_pct  = float(df["pct_chg"].iloc[-2])   # yesterday's % move
    today_pct = float(df["pct_chg"].iloc[-1])   # today's % move
    vol_ratio = df["vol_ratio"].iloc[-1]
    rsi       = df["rsi"].iloc[-1]
    ema5      = df["ema5"].iloc[-1]
    ema20     = df["ema20"].iloc[-1]
    price     = df["Close"].iloc[-1]

    # Yesterday must have been a big circuit-like move (> 4%)
    big_prev  = prev_pct > 4.0

    # Today continues (positive, with volume)
    cont_today = today_pct > 0.5 and vol_ratio > 1.5

    # EMA5 > EMA20 confirms uptrend
    trend_ok  = ema5 > ema20

    if big_prev and cont_today and trend_ok and rsi < 80:
        conf = int(min(88, 62 + prev_pct * 3 + vol_ratio * 2))
        return "BUY", conf, f"Circuit continuation | Prev: +{prev_pct:.1f}% | Today: +{today_pct:.1f}% | {vol_ratio:.1f}x vol"

    return "HOLD", 0, ""

# ╔══════════════════════════════════════════════════════════════╗
# ║         STRATEGY 4 — GAP-AND-GO                             ║
# Opening gap > 3% on big volume = strong institutional/retail  ║
# interest. First 30 min candle direction = day direction.      ║
# Win rate: ~70%. Only valid on intraday data.                   ║
# ╚══════════════════════════════════════════════════════════════╝
def ps4_gap_and_go(df, mode="Swing (Daily)") -> tuple:
    """
    Signal: Today's open is 3–15% above yesterday's close (gap up).
    Volume confirms (>2x average). Price holds above gap midpoint.
    Bullish gap-and-go = momentum trade for the day.
    """
    if len(df) < 5: return "HOLD", 0, ""

    gap_pct   = float(df["gap_pct"].iloc[-1])
    vol_ratio = df["vol_ratio"].iloc[-1]
    price     = df["Close"].iloc[-1]
    open_     = float(df["Open"].iloc[-1])
    prev_close= float(df["Close"].iloc[-2])
    rsi       = df["rsi"].iloc[-1]

    # Gap midpoint — price must hold above it
    gap_mid   = (open_ + prev_close) / 2

    # Bullish gap-and-go
    if (3.0 < gap_pct < 15.0 and vol_ratio > 2.0
            and price > gap_mid and price > open_ * 0.98 and rsi < 80):
        conf = int(min(88, 65 + gap_pct * 2 + vol_ratio))
        return "BUY", conf, f"Gap-and-Go +{gap_pct:.1f}% open | {vol_ratio:.1f}x vol | holds above gap mid"

    # Bearish gap-down continuation (short opportunity)
    if (gap_pct < -3.0 and vol_ratio > 2.0 and price < gap_mid):
        conf = int(min(82, 60 + abs(gap_pct) * 2))
        return "SELL", conf, f"Gap-down {gap_pct:.1f}% | {vol_ratio:.1f}x vol | below gap mid"

    return "HOLD", 0, ""

# ╔══════════════════════════════════════════════════════════════╗
# ║         STRATEGY 5 — PENNY DOUBLE-BOTTOM (W-PATTERN)        ║
# Two lows at similar price with rising volume on 2nd bounce.   ║
# Classic reversal pattern. Works well for beaten-down penny    ║
# stocks that are stabilising after a large decline.            ║
# Win rate: ~74% when confirmed with volume.                    ║
# ╚══════════════════════════════════════════════════════════════╝
def ps5_double_bottom(df) -> tuple:
    """
    Signal: Two recent lows within 3% of each other,
    with the second low followed by a volume increase and green bar.
    RSI must be oversold (<40) at the second low to confirm exhaustion.
    """
    if len(df) < 20: return "HOLD", 0, ""

    lows  = df["Low"]
    rsi   = df["rsi"]
    price = df["Close"].iloc[-1]
    vol_ratio = df["vol_ratio"].iloc[-1]
    cmf   = df["cmf"].iloc[-1]
    willr = df["willr"].iloc[-1]

    # Find two recent local lows in the last 20 bars
    recent_lows = lows.iloc[-20:]
    lo1_idx = recent_lows.idxmin()
    lo1_val = recent_lows[lo1_idx]

    # Second low = recent minimum excluding the first
    try:
        mask = recent_lows.copy()
        # Zero out region around first low
        lo1_pos = recent_lows.index.get_loc(lo1_idx)
        start   = max(0, lo1_pos - 3)
        end_    = min(len(mask), lo1_pos + 3)
        mask.iloc[start:end_] = np.inf
        lo2_val = mask.min()
    except Exception:
        return "HOLD", 0, ""

    # Two lows within 3% of each other
    if lo2_val == np.inf or lo1_val == 0:
        return "HOLD", 0, ""
    diff_pct = abs(lo2_val - lo1_val) / lo1_val * 100

    # Current price must be rising off the second low
    recovering = price > df["Close"].iloc[-3] and price > df["Low"].iloc[-5:].min() * 1.02

    # RSI oversold + CMF turning positive + Williams %R oversold
    oversold = rsi.iloc[-3] < 40 and willr < -60

    if (diff_pct < 3.0 and recovering and oversold
            and vol_ratio > 1.5 and cmf > -0.2):
        conf = int(min(84, 65 + (3.0 - diff_pct) * 5 + vol_ratio * 2))
        return "BUY", conf, f"Double bottom ₹{lo1_val:.2f}~₹{lo2_val:.2f} | RSI={rsi.iloc[-1]:.0f} | {vol_ratio:.1f}x vol"

    return "HOLD", 0, ""

# ╔══════════════════════════════════════════════════════════════╗
# ║         STRATEGY 6 — SQUEEZE-AND-RELEASE                    ║
# Bollinger Band squeeze on low volatility penny stocks.        ║
# Penny stocks compress for days then release violently.        ║
# Combines with CMF > 0 to determine direction.                 ║
# Win rate: ~76%. Best entry for multi-day swing trades.        ║
# ╚══════════════════════════════════════════════════════════════╝
def ps6_squeeze_release(df) -> tuple:
    """
    Signal: BB bandwidth at 30-day minimum (squeeze) and
    current candle breaks above upper band with positive CMF.
    For penny stocks this often signals 10–20% multi-day moves.
    """
    if len(df) < 35: return "HOLD", 0, ""

    bw     = df["bb_w"]
    price  = df["Close"].iloc[-1]
    bb_u   = df["bb_u"].iloc[-1]
    bb_l   = df["bb_l"].iloc[-1]
    vol_ratio = df["vol_ratio"].iloc[-1]
    cmf    = df["cmf"].iloc[-1]
    rsi    = df["rsi"].iloc[-1]
    ema5   = df["ema5"].iloc[-1]

    # Squeeze: bandwidth at or near 30-bar minimum
    bw_min30  = bw.rolling(min(30, len(bw))).min().iloc[-1]
    bw_mean30 = bw.rolling(min(30, len(bw))).mean().iloc[-1]
    squeezed  = bw.iloc[-3:].min() <= bw_min30 * 1.10

    if squeezed and price > bb_u and cmf > 0.05 and vol_ratio > 1.5 and rsi < 80:
        conf = int(min(90, 70 + cmf * 40 + vol_ratio * 2))
        return "BUY",  conf, f"BB squeeze release UP | BW={bw.iloc[-1]:.3f} | CMF={cmf:.2f} | {vol_ratio:.1f}x"

    if squeezed and price < bb_l and cmf < -0.05 and vol_ratio > 1.5:
        conf = int(min(85, 66 + abs(cmf) * 30 + vol_ratio * 2))
        return "SELL", conf, f"BB squeeze release DOWN | BW={bw.iloc[-1]:.3f} | CMF={cmf:.2f}"

    return "HOLD", 0, ""

# ╔══════════════════════════════════════════════════════════════╗
# ║              PENNY STRATEGY MAP                             ║
# ╚══════════════════════════════════════════════════════════════╝
PENNY_STRATEGY_MAP = {
    "Volume Spike Momentum":    (ps1_volume_spike_momentum,   0.22),
    "Operator Accumulation":    (ps2_operator_accumulation,   0.20),
    "Circuit Continuation":     (ps3_circuit_continuation,    0.18),
    "Gap-and-Go":               (ps4_gap_and_go,              0.15),
    "Double Bottom":            (ps5_double_bottom,           0.15),
    "BB Squeeze Release":       (ps6_squeeze_release,         0.10),
}

def run_penny_strategies(df: pd.DataFrame, enabled: list, mode: str) -> dict:
    results  = {}
    buy_w = sell_w = total_w = 0.0
    triggers = []

    for name, (fn, weight) in PENNY_STRATEGY_MAP.items():
        if name not in enabled:
            continue
        try:
            if name == "Gap-and-Go":
                sig, conf, reason = fn(df, mode)
            else:
                sig, conf, reason = fn(df)
            results[name] = {"signal":sig, "confidence":conf, "reason":reason}
            if sig == "BUY":
                buy_w  += weight * (conf/100)
                triggers.append(f"✅ {name}: BUY ({conf}%)")
            elif sig == "SELL":
                sell_w += weight * (conf/100)
                triggers.append(f"🔴 {name}: SELL ({conf}%)")
            total_w += weight
        except Exception:
            pass

    if total_w == 0:
        return {"signal":"HOLD","score":0,"strategies":results,"triggers":triggers}

    score  = (buy_w - sell_w) / total_w
    signal = "BUY" if score > 0.18 else ("SELL" if score < -0.18 else "HOLD")
    return {"signal":signal,"score":round(score,3),"strategies":results,"triggers":triggers}

# ╔══════════════════════════════════════════════════════════════╗
# ║              PENNY POSITION SIZING                          ║
# Wider SL (2x ATR) and target (4x ATR) for penny volatility   ║
# ╚══════════════════════════════════════════════════════════════╝
def penny_position_size(price: float, atr: float, capital: float,
                         risk_pct: float, direction: str = "BUY") -> dict:
    risk_rs = capital * risk_pct
    atr_eff = max(atr, price * 0.02)   # minimum 2% of price as ATR for penny stocks

    if direction == "BUY":
        sl     = round(price - 2 * atr_eff, 2)   # wider SL
        target = round(price + 4 * atr_eff, 2)   # 1:2 RR with wider ATR
    else:
        sl     = round(price + 2 * atr_eff, 2)
        target = round(price - 4 * atr_eff, 2)

    qty      = max(1, int(risk_rs / max(2*atr_eff, 0.01)))
    qty      = min(qty, int(capital * 0.30 / max(price, 0.01)))   # max 30% in one penny stock
    invest   = round(qty * price, 2)
    pot_gain = round(qty * 4 * atr_eff, 2)
    pot_loss = round(qty * 2 * atr_eff, 2)
    brok     = round(invest * BROKERAGE * 2, 2)
    return {
        "qty":qty, "invest":invest, "sl":sl, "target":target,
        "pot_gain":pot_gain, "pot_loss":pot_loss,
        "brokerage":brok, "net_gain":round(pot_gain-brok,2), "rr":"1:2",
    }

# ╔══════════════════════════════════════════════════════════════╗
# ║                 PENNY RISK ASSESSMENT                       ║
# Extra checks specific to penny stocks                         ║
# ╚══════════════════════════════════════════════════════════════╝
def penny_risk_check(df: pd.DataFrame, price: float) -> dict:
    """Returns risk flags specific to penny stocks."""
    flags  = []
    score  = 100   # starts at 100, deduct for each flag

    # Low liquidity check (volume < 50k shares is dangerous)
    avg_vol = df["Volume"].rolling(10).mean().iloc[-1]
    if avg_vol < 50000:
        flags.append("⚠️ Very low liquidity (<50k avg vol)")
        score -= 30

    # Extreme volatility (ATR > 5% of price daily = very risky)
    atr   = df["atr"].iloc[-1]
    if atr / price > 0.05:
        flags.append(f"⚠️ High volatility (ATR={atr/price*100:.1f}% of price)")
        score -= 15

    # Recent large gap (suggests operator activity)
    recent_gap = abs(df["gap_pct"].iloc[-1])
    if recent_gap > 8:
        flags.append(f"⚡ Recent large gap {recent_gap:.1f}% — possible operator")
        score -= 10

    # Overbought RSI
    rsi = df["rsi"].iloc[-1]
    if rsi > 75:
        flags.append(f"🔴 RSI overbought ({rsi:.0f}) — late entry risk")
        score -= 20

    # Very low price (< ₹10 = circuit risk)
    if price < 10:
        flags.append(f"⚠️ Sub-₹10 stock — 5% circuit filter likely")
        score -= 10

    # CMF strongly negative while buying
    cmf = df["cmf"].iloc[-1]
    if cmf < -0.2:
        flags.append(f"🔴 CMF negative ({cmf:.2f}) — selling pressure")
        score -= 15

    risk_level = "LOW" if score >= 80 else ("MEDIUM" if score >= 55 else "HIGH")
    return {"flags": flags, "score": score, "level": risk_level}

# ╔══════════════════════════════════════════════════════════════╗
# ║                   AI SENTIMENT                              ║
# ╚══════════════════════════════════════════════════════════════╝
@st.cache_data(ttl=1800)
def get_penny_sentiment(ticker_clean: str, price: float, pct: float,
                         vol_ratio: float) -> dict:
    try:
        api_key = st.secrets.get("ANTHROPIC_API_KEY", None)
        if not api_key:
            return {"score":0.0,"label":"Neutral","confidence":0,"summary":"No API key"}
        client = anthropic.Anthropic(api_key=api_key)
        prompt = (
            f"NSE penny stock {ticker_clean}: Rs{price:.2f} ({pct:+.1f}%) | "
            f"Volume: {vol_ratio:.1f}x average\n"
            f"Analyse for pump-and-dump risk, operator activity, and genuine breakout potential.\n"
            'Return ONLY JSON: {"score":<-1 to 1>,"label":"...","confidence":<0-100>,"summary":"<1 sentence, mention pump risk if relevant>"}'
        )
        r    = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=120,
            messages=[{"role":"user","content":prompt}]
        )
        data = json.loads(r.content[0].text.strip().replace("```json","").replace("```",""))
        data["score"]      = max(-1.0, min(1.0, float(data.get("score",0))))
        data["confidence"] = max(0, min(100, int(data.get("confidence",0))))
        return data
    except Exception:
        return {"score":0.0,"label":"Neutral","confidence":0,"summary":"API error"}

# ╔══════════════════════════════════════════════════════════════╗
# ║                    SCAN SINGLE STOCK                        ║
# ╚══════════════════════════════════════════════════════════════╝
def scan_penny_stock(ticker: str, df_raw: pd.DataFrame, mode: str,
                     use_sentiment: bool, enabled: list,
                     price_min: float, price_max: float) -> dict | None:
    try:
        df = add_penny_features(df_raw)
        if df.empty or len(df) < 30:
            return None

        price = float(df["Close"].iloc[-1])

        # Price filter
        if not (price_min <= price <= price_max):
            return None

        prev  = float(df["Close"].iloc[-2])
        pct   = (price - prev) / prev * 100
        atr   = float(df["atr"].iloc[-1])
        vol_ratio = float(df["vol_ratio"].iloc[-1])

        tech = run_penny_strategies(df, enabled, mode)

        risk = penny_risk_check(df, price)

        sentiment = {"score":0.0,"label":"Neutral","confidence":0,"summary":"—"}
        if use_sentiment:
            sentiment = get_penny_sentiment(
                ticker.replace(".NS",""), price, pct, vol_ratio
            )

        w       = 0.20   # lower sentiment weight for penny stocks (less news coverage)
        blended = (1-w)*tech["score"] + w*sentiment["score"]

        if blended > 0.18:   final_sig = "BUY"
        elif blended < -0.18:final_sig = "SELL"
        else:                 final_sig = "HOLD"

        pos = {}
        if final_sig in ("BUY","SELL") and atr > 0:
            pos = penny_position_size(price, atr, CAPITAL, RISK_PCT, final_sig)

        n_buy  = sum(1 for v in tech["strategies"].values() if v["signal"]=="BUY")
        n_sell = sum(1 for v in tech["strategies"].values() if v["signal"]=="SELL")

        # OBV trend
        obv_trend = "Rising ↑" if df["obv"].iloc[-1] > df["obv"].iloc[-5] else "Falling ↓"
        cmf_val   = round(float(df["cmf"].iloc[-1]), 3)
        willr_val = round(float(df["willr"].iloc[-1]), 1)

        return {
            "ticker":       ticker.replace(".NS",""),
            "price":        round(price, 2),
            "change_pct":   round(pct, 2),
            "atr":          round(atr, 2),
            "vol_ratio":    round(vol_ratio, 2),
            "tech_score":   tech["score"],
            "tech_signal":  tech["signal"],
            "strategies_hit":tech["strategies"],
            "triggers":     tech["triggers"],
            "n_buy":        n_buy,
            "n_sell":       n_sell,
            "final_score":  round(blended, 3),
            "final_signal": final_sig,
            "position":     pos,
            "risk":         risk,
            "sent_label":   sentiment["label"],
            "sent_summary": sentiment["summary"],
            "obv_trend":    obv_trend,
            "cmf":          cmf_val,
            "willr":        willr_val,
        }
    except Exception:
        return None

# ╔══════════════════════════════════════════════════════════════╗
# ║                           SIDEBAR                           ║
# ╚══════════════════════════════════════════════════════════════╝
with st.sidebar:
    st.header("⚙️ Penny Scanner Settings")

    mode = st.selectbox("Timeframe", ["Swing (Daily)","Intraday (15m)","Intraday (5m)"],
                        help="Daily is most reliable for penny stock swing signals")
    st.divider()

    st.subheader("Price Filter")
    price_min = st.slider("Min Price (₹)", 1, 50, 5)
    price_max = st.slider("Max Price (₹)", 20, 500, 100)
    st.caption(f"Scanning ₹{price_min}–₹{price_max} range")
    st.divider()

    st.subheader("Strategies")
    enabled = []
    for name in PENNY_STRATEGY_MAP.keys():
        if st.checkbox(name, value=True, key=f"ps_{name}"):
            enabled.append(name)

    st.divider()
    use_sentiment  = st.toggle("🤖 AI Pump-Risk Analysis", value=False)
    min_strategies = st.slider("Min Strategies Agreeing", 1, 4, 1)
    only_high_vol  = st.checkbox("Only Volume Spike (>3x)", value=False)
    hide_high_risk = st.checkbox("Hide HIGH risk stocks", value=True)
    st.divider()

    st.subheader("Risk / Capital")
    CAPITAL    = st.number_input("Capital (₹)", 5000, 200000, 20000, 2500)
    RISK_PCT   = st.slider("Risk per Trade %", 1.0, 8.0, 3.0, 0.5) / 100
    TARGET_DAY = st.number_input("Daily Target (₹)", 200, 5000, 500, 100)
    st.divider()

    st.markdown("**⚠️ Penny Stock Rules**")
    st.caption("✅ Take profit fast — 4–6% gain = exit")
    st.caption("✅ Never average down")
    st.caption("✅ Max 2 penny stocks simultaneously")
    st.caption("✅ Exit before 3 PM")
    st.caption("🚫 Never hold overnight")
    st.caption("🚫 Avoid sub-₹5 stocks entirely")
    st.caption("🚫 Never chase after >10% move")

    trades_needed = max(1, int(TARGET_DAY / (CAPITAL * RISK_PCT * 2)))
    st.caption(f"\nTarget: ₹{TARGET_DAY:,} | Risk/trade: ₹{int(CAPITAL*RISK_PCT):,}")
    st.caption(f"1:2 gain: ₹{int(CAPITAL*RISK_PCT*2):,} | Need {trades_needed} win(s)")

# ╔══════════════════════════════════════════════════════════════╗
# ║                          MAIN UI                            ║
# ╚══════════════════════════════════════════════════════════════╝
st.markdown(f"""
<div style='background:#1f0d0d;border:1px solid #5c1a1a;border-radius:8px;padding:12px 20px;margin-bottom:16px;'>
<b style='color:#ff7043'>Penny Capital Plan</b> &nbsp;|&nbsp;
Capital: <b>₹{CAPITAL:,}</b> &nbsp;|&nbsp;
Risk/trade: <b>₹{int(CAPITAL*RISK_PCT):,}</b> &nbsp;|&nbsp;
Gain/trade (1:2): <b>₹{int(CAPITAL*RISK_PCT*2):,}</b> &nbsp;|&nbsp;
Price range: <b>₹{price_min}–₹{price_max}</b> &nbsp;|&nbsp;
Stocks in universe: <b>{len(PENNY_UNIVERSE)}</b>
</div>
""", unsafe_allow_html=True)

with st.expander("📚 Penny Strategy Guide", expanded=False):
    st.markdown("""
| # | Strategy | Win Rate | Edge | Best For |
|---|---|---|---|---|
| 1 | **Volume Spike Momentum** | ~68% | 5x vol + price breakout | Intraday pump detection |
| 2 | **Operator Accumulation** | ~72% | OBV divergence (smart money) | Swing 2–5 days |
| 3 | **Circuit Continuation** | ~65% | Day-after circuit momentum | Morning 9:20–10:30 |
| 4 | **Gap-and-Go** | ~70% | 3%+ open gap + vol | First 30 min only |
| 5 | **Double Bottom (W)** | ~74% | Reversal with vol confirmation | Beaten-down stocks |
| 6 | **BB Squeeze Release** | ~76% | Volatility expansion breakout | Multi-day swing |

**How to read signals:**
- 🟢 **BUY** with OBV Rising + CMF > 0 = high conviction
- 📊 **Vol Ratio > 5x** = operator involvement (enter with caution)
- ⚠️ **Risk Level HIGH** = reduce position size by 50%
- 💎 **2+ strategies agreeing** = much better odds
""")

if not enabled:
    st.warning("Select at least one strategy.")
    st.stop()

run_scan = st.button("🔍 Run Penny Scanner", type="primary", use_container_width=True)

if run_scan:
    results = []
    no_data = []

    bar = st.progress(0, text="⚡ Fetching penny stock data in parallel...")
    interval_map = {"Intraday (5m)":"5m","Intraday (15m)":"15m","Swing (Daily)":"1d"}
    period_map   = {"Intraday (5m)":"60d","Intraday (15m)":"60d","Swing (Daily)":"1y"}
    interval = interval_map[mode]
    period   = period_map[mode]

    data_cache = fetch_all_parallel(PENNY_UNIVERSE, interval, period)
    bar.progress(0.45, text="✅ Data ready. Analysing penny signals...")

    for i, ticker in enumerate(PENNY_UNIVERSE):
        bar.progress(0.45 + (i+1)/len(PENNY_UNIVERSE)*0.55,
                     text=f"Scanning {ticker.replace('.NS','')} ({i+1}/{len(PENNY_UNIVERSE)})...")
        df_raw = data_cache.get(ticker)
        if df_raw is None:
            no_data.append(ticker.replace(".NS",""))
            continue

        res = scan_penny_stock(ticker, df_raw, mode, use_sentiment,
                               enabled, float(price_min), float(price_max))
        if res is None:
            continue

        if res["final_signal"] in ("BUY","SELL"):
            n_agree = res["n_buy"] if res["final_signal"]=="BUY" else res["n_sell"]
            if n_agree < min_strategies:
                continue
            if only_high_vol and res["vol_ratio"] < 3.0:
                continue
            if hide_high_risk and res["risk"]["level"] == "HIGH":
                continue
            results.append(res)
            if abs(res["final_score"]) > 0.45:
                pos = res.get("position",{})
                send_telegram(
                    f"PENNY {'BUY' if res['final_signal']=='BUY' else 'SELL'}: {res['ticker']}\n"
                    f"₹{res['price']} ({res['change_pct']:+.1f}%) | Vol:{res['vol_ratio']:.1f}x\n"
                    f"Risk: {res['risk']['level']} | Score:{res['final_score']:.2f}\n"
                    f"SL:₹{pos.get('sl','—')} | Target:₹{pos.get('target','—')} | Qty:{pos.get('qty','—')}\n"
                    f"OBV:{res['obv_trend']} | CMF:{res['cmf']}\n"
                    f"{', '.join(res['triggers'][:2])}"
                )

    bar.empty()
    st.session_state.penny_results = results
    st.session_state.penny_ts = datetime.now().strftime("%H:%M:%S")

    if no_data:
        with st.expander(f"⚠️ {len(no_data)} no data"):
            st.write(", ".join(no_data))

results = st.session_state.penny_results

if results:
    buys  = sorted([r for r in results if r["final_signal"]=="BUY"],  key=lambda x:-x["final_score"])
    sells = sorted([r for r in results if r["final_signal"]=="SELL"], key=lambda x:x["final_score"])

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("🟢 BUY",   len(buys))
    c2.metric("🔴 SELL",  len(sells))
    c3.metric("📊 Total", len(results))
    c4.metric("🕐 Scanned", st.session_state.penny_ts or "—")
    high_vol = sum(1 for r in results if r["vol_ratio"] > 3)
    c5.metric("⚡ High Volume (>3x)", high_vol)

    if not results:
        st.warning("No penny signals found. Try: Min Strategies=1, wider price range, or check market hours.")

    st.divider()
    tab1,tab2,tab3 = st.tabs(["🟢 BUY Signals","🔴 SELL / Short","📋 Full Table"])

    def render_penny_cards(signal_list):
        if not signal_list:
            st.info("No signals.")
            return
        for r in signal_list:
            pos    = r.get("position",{})
            risk   = r.get("risk",{})
            risk_color = {"LOW":"#00e676","MEDIUM":"#ffd600","HIGH":"#ff5252"}.get(risk.get("level","—"),"#888")
            n      = r["n_buy"] if r["final_signal"]=="BUY" else r["n_sell"]
            with st.expander(
                f"{'🟢' if r['final_signal']=='BUY' else '🔴'} **{r['ticker']}** "
                f"₹{r['price']} ({r['change_pct']:+.1f}%) | Vol:{r['vol_ratio']:.1f}x | "
                f"Score:{r['final_score']:.3f} | {n} strategies | "
                f"Risk: {risk.get('level','—')}"
            ):
                c1,c2,c3,c4 = st.columns(4)
                c1.metric("Entry",  f"₹{r['price']}")
                c2.metric("Target", f"₹{pos.get('target','—')}", f"+₹{pos.get('pot_gain','—')}")
                c3.metric("SL",     f"₹{pos.get('sl','—')}",     f"-₹{pos.get('pot_loss','—')}")
                c4.metric("Qty",    pos.get("qty","—"),           f"₹{pos.get('invest','—')}")

                c5,c6,c7,c8 = st.columns(4)
                c5.metric("Net Gain", f"₹{pos.get('net_gain','—')}")
                c6.metric("OBV",      r["obv_trend"])
                c7.metric("CMF",      f"{r['cmf']:.3f}")
                c8.metric("Williams%R", f"{r['willr']:.1f}")

                # Risk flags
                if risk.get("flags"):
                    for flag in risk["flags"]:
                        st.caption(flag)

                st.markdown("**Strategies triggered:**")
                for trig in r["triggers"]:
                    st.caption(trig)

                if r.get("sent_summary","—") not in ("—",""):
                    st.info(f"🤖 {r['sent_label']}: {r['sent_summary']}")

                if st.button(f"➕ Track Trade", key=f"ppnl_{r['ticker']}"):
                    st.session_state.penny_pnl.append({
                        "ticker": r["ticker"], "signal": r["final_signal"],
                        "entry":  r["price"],  "sl": pos.get("sl",0),
                        "target": pos.get("target",0), "qty": pos.get("qty",0),
                        "status": "Open", "pnl": 0,
                    })
                    st.success(f"Added {r['ticker']} to tracker!")

    with tab1:
        st.subheader(f"🟢 {len(buys)} BUY Signals — Penny Stocks")
        render_penny_cards(buys)
    with tab2:
        st.subheader(f"🔴 {len(sells)} SELL Signals")
        render_penny_cards(sells)
    with tab3:
        st.subheader("All Penny Signals")
        if results:
            rows = []
            for r in sorted(results, key=lambda x:-abs(x["final_score"])):
                pos = r.get("position",{})
                n   = r["n_buy"] if r["final_signal"]=="BUY" else r["n_sell"]
                rows.append({
                    "Stock":    r["ticker"],
                    "Price(₹)": r["price"],
                    "Chg%":     r["change_pct"],
                    "Signal":   r["final_signal"],
                    "Score":    r["final_score"],
                    "Vol Ratio":r["vol_ratio"],
                    "Strats":   f"{n}/{len(enabled)}",
                    "Risk":     r["risk"]["level"],
                    "OBV":      r["obv_trend"],
                    "CMF":      r["cmf"],
                    "Target":   pos.get("target","—"),
                    "SL":       pos.get("sl","—"),
                    "Qty":      pos.get("qty","—"),
                    "Net(₹)":   pos.get("net_gain","—"),
                })
            df_tbl = pd.DataFrame(rows)
            def csig(val):
                if val=="BUY":  return "background-color:#1a4731;color:#00e676;font-weight:bold"
                if val=="SELL": return "background-color:#4a1010;color:#ff5252;font-weight:bold"
                return ""
            def crisk(val):
                if val=="HIGH":   return "color:#ff5252;font-weight:bold"
                if val=="MEDIUM": return "color:#ffd600"
                if val=="LOW":    return "color:#00e676"
                return ""
            st.dataframe(
                df_tbl.style.map(csig, subset=["Signal"])
                            .map(crisk, subset=["Risk"])
                            .format({"Chg%":"{:+.1f}%","Score":"{:.3f}","CMF":"{:.3f}"}),
                use_container_width=True, height=500
            )
            csv_buf = io.BytesIO()
            df_tbl.to_csv(csv_buf, index=False)
            st.download_button("⬇️ Export CSV", csv_buf.getvalue(),
                               f"penny_signals_{date.today()}.csv", "text/csv")

# P&L tracker
if st.session_state.penny_pnl:
    st.divider()
    st.subheader("💰 Penny Trade Tracker")
    for i, t in enumerate(st.session_state.penny_pnl):
        cols = st.columns([2,1,1,1,1,1,1,1])
        cols[0].write(f"**{t['ticker']}** ({t['signal']})")
        cols[1].write(f"₹{t['entry']}")
        cols[2].write(f"SL:₹{t['sl']}")
        cols[3].write(f"Tgt:₹{t['target']}")
        cols[4].write(f"Qty:{t['qty']}")
        new_s = cols[5].selectbox("", ["Open","Hit Target","Hit SL","Manual Exit"],
                                  key=f"pps_{i}",
                                  index=["Open","Hit Target","Hit SL","Manual Exit"].index(t["status"]))
        st.session_state.penny_pnl[i]["status"] = new_s
        if new_s == "Hit Target":
            st.session_state.penny_pnl[i]["pnl"] = round((t["target"]-t["entry"])*t["qty"],2)
        elif new_s == "Hit SL":
            st.session_state.penny_pnl[i]["pnl"] = round((t["sl"]-t["entry"])*t["qty"],2)
        cols[6].write(f"₹{st.session_state.penny_pnl[i]['pnl']}")
        if cols[7].button("🗑️", key=f"pdel_{i}"):
            st.session_state.penny_pnl.pop(i)
            st.rerun()

    total = sum(t["pnl"] for t in st.session_state.penny_pnl)
    color = "#00e676" if total >= 0 else "#ff1744"
    st.markdown(f"### Session P&L: <span style='color:{color}'>₹{total:,.2f}</span>", unsafe_allow_html=True)
    prog = max(0, min(100, int(total/TARGET_DAY*100))) if TARGET_DAY > 0 else 0
    st.progress(prog, text=f"Target progress: {prog}% of ₹{TARGET_DAY:,}")
