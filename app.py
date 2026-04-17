import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import ta
from xgboost import XGBClassifier
import requests
import time

st.set_page_config(layout="wide")
st.title("🚀 PRO AI TRADING SYSTEM (India)")

# =========================
# CONFIG
# =========================
NIFTY50 = [
    "RELIANCE.NS","TCS.NS","INFY.NS","HDFCBANK.NS","ICICIBANK.NS",
    "SBIN.NS","LT.NS","ITC.NS","AXISBANK.NS","KOTAKBANK.NS"
]

CAPITAL = 100000
RISK_PER_TRADE = 0.02
BROKERAGE = 0.0005  # 0.05%

# =========================
# TELEGRAM
# =========================
def send_telegram(msg):
    try:
        token = st.secrets["TELEGRAM_TOKEN"]
        chat_id = st.secrets["TELEGRAM_CHAT_ID"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, data={"chat_id": chat_id, "text": msg})
    except:
        pass

# =========================
# DATA
# =========================
@st.cache_data
def get_data(ticker, mode):
    if mode == "Intraday":
        return yf.download(ticker, period="5d", interval="5m")
    else:
        return yf.download(ticker, period="1y", interval="1d")

# =========================
# FEATURES
# =========================
def add_features(df):

    df['rsi'] = ta.momentum.RSIIndicator(df['Close']).rsi()
    df['ema50'] = ta.trend.EMAIndicator(df['Close'], 50).ema_indicator()
    df['ema200'] = ta.trend.EMAIndicator(df['Close'], 200).ema_indicator()

    # ATR for stop loss
    df['atr'] = ta.volatility.AverageTrueRange(
        df['High'], df['Low'], df['Close']).average_true_range()

    # VWAP (intraday)
    df['vwap'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()

    df['vol_avg'] = df['Volume'].rolling(20).mean()
    df['vol_spike'] = df['Volume'] / df['vol_avg']

    df['returns_5'] = df['Close'].pct_change(5)
    df['returns_10'] = df['Close'].pct_change(10)

    df['target'] = (df['Close'].shift(-1) > df['Close']).astype(int)

    df.dropna(inplace=True)
    return df

# =========================
# MODEL
# =========================
def train_model(df):
    features = ['rsi','ema50','ema200','vol_spike','returns_5','returns_10']

    X = df[features]
    y = df['target']

    model = XGBClassifier(n_estimators=150, max_depth=5)
    model.fit(X, y)

    return model

# =========================
# SIGNAL ENGINE (IMPROVED)
# =========================
def generate_signal(row, prob, mode):

    # Market regime
    regime = 1 if row['Close'] > row['ema200'] else 0

    # Intraday filter
    if mode == "Intraday":
        vwap_cond = row['Close'] > row['vwap']
    else:
        vwap_cond = True

    tech_score = 1 if regime and vwap_cond else 0

    score = 0.5*prob + 0.5*tech_score

    if score > 0.65:
        return "BUY", score
    elif score < 0.35:
        return "SELL", score
    else:
        return "HOLD", score

# =========================
# BACKTEST (REALISTIC)
# =========================
def backtest(df, model, mode):

    capital = CAPITAL
    position = 0
    entry_price = 0
    trades = []

    features = ['rsi','ema50','ema200','vol_spike','returns_5','returns_10']

    for i in range(len(df)-1):

        row = df.iloc[i]
        next_price = df['Close'].iloc[i+1]

        prob = model.predict_proba(df.iloc[i:i+1][features])[0][1]
        signal, _ = generate_signal(row, prob, mode)

        atr = row['atr']

        if signal == "BUY" and position == 0:

            risk_amount = capital * RISK_PER_TRADE
            qty = risk_amount / atr

            entry_price = row['Close']
            position = qty

        elif position != 0:

            # SL / TP logic
            sl = entry_price - atr
            tp = entry_price + 2*atr

            exit_price = next_price

            if exit_price <= sl or exit_price >= tp or signal == "SELL":

                pnl = (exit_price - entry_price) * position
                pnl -= abs(pnl) * BROKERAGE

                capital += pnl
                trades.append(pnl)

                position = 0

    return capital, trades

# =========================
# METRICS
# =========================
def metrics(trades):

    if len(trades) == 0:
        return 0,0,0

    wins = [t for t in trades if t > 0]
    losses = [t for t in trades if t < 0]

    win_rate = len(wins)/len(trades)
    profit_factor = sum(wins)/abs(sum(losses)) if losses else 0
    avg_trade = np.mean(trades)

    return win_rate, profit_factor, avg_trade

# =========================
# SCANNER
# =========================
def scan(ticker, mode):

    df = get_data(ticker, mode)
    df = add_features(df)

    model = train_model(df)

    latest = df.iloc[-1]
    features = ['rsi','ema50','ema200','vol_spike','returns_5','returns_10']

    prob = model.predict_proba(df.iloc[-1:][features])[0][1]

    signal, score = generate_signal(latest, prob, mode)

    final_capital, trades = backtest(df, model, mode)
    win_rate, pf, avg = metrics(trades)

    return {
        "ticker": ticker,
        "signal": signal,
        "score": round(score,2),
        "win_rate": round(win_rate,2),
        "pf": round(pf,2),
        "capital": int(final_capital)
    }

# =========================
# UI
# =========================
mode = st.sidebar.selectbox("Mode", ["Swing", "Intraday"])

auto = st.sidebar.checkbox("Auto Refresh (5 min)")

if st.button("Run Scan"):

    results = []
    progress = st.progress(0)

    for i, ticker in enumerate(NIFTY50):

        try:
            res = scan(ticker, mode)
            results.append(res)

            # Telegram alert
            if res['signal'] == "BUY" and res['score'] > 0.7:
                send_telegram(f"🚀 BUY {ticker}\nScore: {res['score']}")

        except:
            pass

        progress.progress((i+1)/len(NIFTY50))

    df = pd.DataFrame(results).sort_values(by="score", ascending=False)

    st.subheader("📊 Signals")
    st.dataframe(df)

# =========================
# AUTO REFRESH
# =========================
if auto:
    time.sleep(300)
    st.rerun()
