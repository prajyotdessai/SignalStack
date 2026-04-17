import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import ta
import requests
import time

st.set_page_config(layout="wide")
st.title("🚀 PRO MULTI-STRATEGY TRADING SYSTEM")

# ================= CONFIG =================
NIFTY50 = [
    "RELIANCE.NS","TCS.NS","INFY.NS","HDFCBANK.NS","ICICIBANK.NS",
    "SBIN.NS","LT.NS","ITC.NS","AXISBANK.NS","KOTAKBANK.NS"
]

CAPITAL = 100000
RISK = 0.02
BROKERAGE = 0.0005

# ================= TELEGRAM =================
def send_telegram(msg):
    try:
        token = st.secrets["TELEGRAM_TOKEN"]
        chat_id = st.secrets["TELEGRAM_CHAT_ID"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, data={"chat_id": chat_id, "text": msg})
    except:
        pass

# ================= DATA =================
@st.cache_data
def get_data(ticker, mode):
    if mode == "Intraday":
        return yf.download(ticker, period="5d", interval="5m")
    return yf.download(ticker, period="1y", interval="1d")

# ================= FEATURES =================
def add_features(df):
    df['ema200'] = ta.trend.EMAIndicator(df['Close'], 200).ema_indicator()
    df['atr'] = ta.volatility.AverageTrueRange(
        df['High'], df['Low'], df['Close']).average_true_range()

    df['vwap'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()

    return df.dropna()

# ================= STRATEGIES =================

def macd_adx(df):
    macd = ta.trend.MACD(df['Close'])
    adx = ta.trend.ADXIndicator(df['High'], df['Low'], df['Close']).adx()

    if macd.macd().iloc[-1] > macd.macd_signal().iloc[-1] and adx.iloc[-1] > 25:
        return 1
    elif macd.macd().iloc[-1] < macd.macd_signal().iloc[-1]:
        return -1
    return 0

def ema_rsi(df):
    ema9 = ta.trend.EMAIndicator(df['Close'], 9).ema_indicator()
    ema21 = ta.trend.EMAIndicator(df['Close'], 21).ema_indicator()
    rsi = ta.momentum.RSIIndicator(df['Close']).rsi()

    if ema9.iloc[-1] > ema21.iloc[-1] and 40 < rsi.iloc[-1] < 70:
        return 1
    elif ema9.iloc[-1] < ema21.iloc[-1]:
        return -1
    return 0

def vwap_bounce(df):
    if df['Close'].iloc[-1] > df['vwap'].iloc[-1]:
        return 1
    elif df['Close'].iloc[-1] < df['vwap'].iloc[-1]:
        return -1
    return 0

def supertrend(df):
    atr = df['atr']
    hl2 = (df['High'] + df['Low']) / 2
    upper = hl2 + 2*atr
    lower = hl2 - 2*atr

    if df['Close'].iloc[-1] > upper.iloc[-1]:
        return 1
    elif df['Close'].iloc[-1] < lower.iloc[-1]:
        return -1
    return 0

def bollinger(df):
    bb = ta.volatility.BollingerBands(df['Close'])

    if df['Close'].iloc[-1] > bb.bollinger_hband().iloc[-1]:
        return 1
    elif df['Close'].iloc[-1] < bb.bollinger_lband().iloc[-1]:
        return -1
    return 0

# ================= ENSEMBLE =================
def strategy_score(df):

    signals = [
        macd_adx(df),
        ema_rsi(df),
        vwap_bounce(df),
        supertrend(df),
        bollinger(df)
    ]

    score = sum(signals)/len(signals)

    if score > 0.4:
        return "BUY", score
    elif score < -0.4:
        return "SELL", score
    else:
        return "HOLD", score

# ================= BACKTEST =================
def backtest(df):

    capital = CAPITAL
    position = 0
    entry = 0
    trades = []

    for i in range(len(df)-1):

        signal, _ = strategy_score(df.iloc[:i+1])
        price = df['Close'].iloc[i]
        next_price = df['Close'].iloc[i+1]
        atr = df['atr'].iloc[i]

        if signal == "BUY" and position == 0:
            qty = (capital * RISK) / atr
            entry = price
            position = qty

        elif position != 0:

            sl = entry - atr
            tp = entry + 2*atr

            if next_price <= sl or next_price >= tp or signal == "SELL":

                pnl = (next_price - entry) * position
                pnl -= abs(pnl) * BROKERAGE

                capital += pnl
                trades.append(pnl)
                position = 0

    return capital, trades

# ================= METRICS =================
def metrics(trades):
    if not trades:
        return 0,0

    wins = [t for t in trades if t > 0]
    losses = [t for t in trades if t < 0]

    win_rate = len(wins)/len(trades)
    pf = sum(wins)/abs(sum(losses)) if losses else 0

    return win_rate, pf

# ================= SCANNER =================
def scan(ticker, mode):

    df = get_data(ticker, mode)
    df = add_features(df)

    signal, score = strategy_score(df)

    final_cap, trades = backtest(df)
    win_rate, pf = metrics(trades)

    return {
        "Stock": ticker,
        "Signal": signal,
        "Score": round(score,2),
        "WinRate": round(win_rate,2),
        "PF": round(pf,2),
        "Capital": int(final_cap)
    }

# ================= UI =================
mode = st.sidebar.selectbox("Mode", ["Swing","Intraday"])
auto = st.sidebar.checkbox("Auto Refresh (5 min)")

if st.button("Run Scanner"):

    results = []
    progress = st.progress(0)

    for i, stock in enumerate(NIFTY50):

        try:
            res = scan(stock, mode)
            results.append(res)

            if res["Signal"] == "BUY" and res["Score"] > 0.6:
                send_telegram(f"🚀 BUY {stock} | Score: {res['Score']}")

        except:
            pass

        progress.progress((i+1)/len(NIFTY50))

    df = pd.DataFrame(results).sort_values(by="Score", ascending=False)

    st.subheader("📊 Signals")
    st.dataframe(df)

# ================= AUTO REFRESH =================
if auto:
    time.sleep(300)
    st.rerun()
