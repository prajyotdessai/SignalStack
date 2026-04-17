import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import ta
from xgboost import XGBClassifier

st.set_page_config(layout="wide")
st.title("🚀 AI Trading System (India Markets)")

# -----------------------------
# CONFIG
# -----------------------------
NIFTY50 = [
    "RELIANCE.NS","TCS.NS","INFY.NS","HDFCBANK.NS","ICICIBANK.NS",
    "SBIN.NS","LT.NS","ITC.NS","AXISBANK.NS","KOTAKBANK.NS"
]

# -----------------------------
# DATA
# -----------------------------
@st.cache_data
def get_data(ticker, period="1y", interval="1d"):
    df = yf.download(ticker, period=period, interval=interval)
    df.dropna(inplace=True)
    return df

# -----------------------------
# FEATURES
# -----------------------------
def add_features(df):
    df['rsi'] = ta.momentum.RSIIndicator(df['Close']).rsi()
    df['ema50'] = ta.trend.EMAIndicator(df['Close'], 50).ema_indicator()
    df['ema200'] = ta.trend.EMAIndicator(df['Close'], 200).ema_indicator()
    df['vol_avg'] = df['Volume'].rolling(20).mean()
    df['vol_spike'] = df['Volume'] / df['vol_avg']
    df['returns_5'] = df['Close'].pct_change(5)
    df['returns_10'] = df['Close'].pct_change(10)
    df['target'] = (df['Close'].shift(-1) > df['Close']).astype(int)
    df.dropna(inplace=True)
    return df

# -----------------------------
# MODEL
# -----------------------------
def train_model(df):
    features = ['rsi','ema50','ema200','vol_spike','returns_5','returns_10']
    X = df[features]
    y = df['target']

    model = XGBClassifier(n_estimators=150, max_depth=5)
    model.fit(X, y)
    return model

# -----------------------------
# SIGNAL GENERATION
# -----------------------------
def generate_signals(df, model):
    signals = []
    features = ['rsi','ema50','ema200','vol_spike','returns_5','returns_10']

    for i in range(len(df)):
        row = df.iloc[i:i+1]
        prob = model.predict_proba(row[features])[0][1]

        tech = 1 if row['Close'].values[0] > row['ema200'].values[0] else 0
        score = 0.7*prob + 0.3*tech

        if score > 0.65:
            signals.append("BUY")
        elif score < 0.35:
            signals.append("SELL")
        else:
            signals.append("HOLD")

    return signals

# -----------------------------
# BACKTEST
# -----------------------------
def backtest(df, signals, capital=100000):
    position = 0
    entry_price = 0
    trades = []

    for i in range(1, len(df)):

        if signals[i] == "BUY" and position == 0:
            position = capital / df['Close'][i]
            entry_price = df['Close'][i]

        elif signals[i] == "SELL" and position != 0:
            exit_price = df['Close'][i]
            pnl = (exit_price - entry_price) * position
            capital += pnl
            trades.append(pnl)
            position = 0

    return capital, trades

# -----------------------------
# METRICS
# -----------------------------
def performance(trades):
    if len(trades) == 0:
        return 0,0,0

    wins = [t for t in trades if t > 0]
    losses = [t for t in trades if t < 0]

    win_rate = len(wins)/len(trades) if trades else 0
    profit_factor = (sum(wins)/abs(sum(losses))) if losses else 0
    avg_trade = sum(trades)/len(trades)

    return win_rate, profit_factor, avg_trade

# -----------------------------
# UI
# -----------------------------
tab1, tab2 = st.tabs(["📊 Scanner", "📈 Backtest"])

# -----------------------------
# SCANNER TAB
# -----------------------------
with tab1:
    st.subheader("🔍 Multi-Stock Scanner")

    if st.button("Run Scanner"):

        results = []

        for ticker in NIFTY50:
            try:
                df = get_data(ticker, "6mo")
                df = add_features(df)
                model = train_model(df)

                latest = df.iloc[-1:]
                features = ['rsi','ema50','ema200','vol_spike','returns_5','returns_10']

                prob = model.predict_proba(latest[features])[0][1]
                tech = 1 if latest['Close'].values[0] > latest['ema200'].values[0] else 0
                score = 0.7*prob + 0.3*tech

                signal = "BUY" if score > 0.65 else "SELL" if score < 0.35 else "HOLD"

                results.append({
                    "Stock": ticker,
                    "AI Prob": round(prob,2),
                    "Score": round(score,2),
                    "Signal": signal
                })

            except:
                pass

        df_results = pd.DataFrame(results).sort_values(by="Score", ascending=False)

        st.dataframe(df_results)

# -----------------------------
# BACKTEST TAB
# -----------------------------
with tab2:
    st.subheader("📈 Strategy Backtest")

    ticker = st.text_input("Enter Stock", "RELIANCE.NS")

    if st.button("Run Backtest"):

        df = get_data(ticker, "2y")
        df = add_features(df)
        model = train_model(df)

        signals = generate_signals(df, model)

        final_capital, trades = backtest(df, signals)
        win_rate, pf, avg = performance(trades)

        col1, col2, col3 = st.columns(3)

        col1.metric("Final Capital", f"₹{final_capital:.0f}")
        col2.metric("Win Rate", f"{win_rate:.2f}")
        col3.metric("Profit Factor", f"{pf:.2f}")

        st.line_chart(df['Close'])
        st.write(f"Total Trades: {len(trades)}")
