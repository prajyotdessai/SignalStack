import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import ta
import requests
import time
import anthropic
import json
from datetime import datetime, timedelta

st.set_page_config(layout="wide", page_title="PRO Trading System + AI Sentiment")
st.title("🚀 PRO MULTI-STRATEGY TRADING SYSTEM + AI SENTIMENT")

# ================= CONFIG =================
NIFTY50 = [
    "RELIANCE.NS", "TCS.NS",       "INFY.NS",      "HDFCBANK.NS", "ICICIBANK.NS",
    "SBIN.NS",     "LT.NS",        "ITC.NS",        "AXISBANK.NS", "KOTAKBANK.NS",
    "WIPRO.NS",    "HCLTECH.NS",   "TATAMOTORS.NS", "BAJFINANCE.NS","ADANIPORTS.NS",
]

CAPITAL   = 100000
RISK      = 0.02
BROKERAGE = 0.0005

# Sentiment weight in final score: 0.0 = ignore, 1.0 = only sentiment
SENTIMENT_WEIGHT = 0.30

# ================= TELEGRAM =================
def send_telegram(msg):
    try:
        token   = st.secrets["TELEGRAM_TOKEN"]
        chat_id = st.secrets["TELEGRAM_CHAT_ID"]
        url     = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, data={"chat_id": chat_id, "text": msg}, timeout=5)
    except Exception:
        pass

# ================= DATA =================
@st.cache_data(ttl=300)
def get_data(ticker, mode):
    if mode == "Intraday":
        return yf.download(ticker, period="5d", interval="5m", auto_adjust=True)
    return yf.download(ticker, period="1y", interval="1d", auto_adjust=True)

@st.cache_data(ttl=3600)
def get_news_headlines(ticker_clean: str) -> list[str]:
    """
    Fetch recent Yahoo Finance news headlines for a ticker.
    Falls back to empty list silently if unavailable.
    """
    try:
        t     = yf.Ticker(ticker_clean + ".NS")
        news  = t.news or []
        titles = [n.get("title", "") for n in news[:8] if n.get("title")]
        return titles
    except Exception:
        return []

# ================= FEATURES =================
def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema200"] = ta.trend.EMAIndicator(df["Close"], window=200).ema_indicator()
    df["atr"]    = ta.volatility.AverageTrueRange(
        df["High"], df["Low"], df["Close"], window=14
    ).average_true_range()
    df["vwap"]   = (df["Close"] * df["Volume"]).cumsum() / df["Volume"].cumsum()
    return df.dropna()

# ================= TECHNICAL STRATEGIES =================
def macd_adx(df: pd.DataFrame) -> float:
    macd = ta.trend.MACD(df["Close"])
    adx  = ta.trend.ADXIndicator(df["High"], df["Low"], df["Close"]).adx()
    if macd.macd().iloc[-1] > macd.macd_signal().iloc[-1] and adx.iloc[-1] > 25:
        return 1.0
    elif macd.macd().iloc[-1] < macd.macd_signal().iloc[-1]:
        return -1.0
    return 0.0

def ema_rsi(df: pd.DataFrame) -> float:
    ema9  = ta.trend.EMAIndicator(df["Close"], window=9).ema_indicator()
    ema21 = ta.trend.EMAIndicator(df["Close"], window=21).ema_indicator()
    rsi   = ta.momentum.RSIIndicator(df["Close"]).rsi()
    if ema9.iloc[-1] > ema21.iloc[-1] and 40 < rsi.iloc[-1] < 70:
        return 1.0
    elif ema9.iloc[-1] < ema21.iloc[-1]:
        return -1.0
    return 0.0

def vwap_bounce(df: pd.DataFrame) -> float:
    if df["Close"].iloc[-1] > df["vwap"].iloc[-1]:
        return 1.0
    elif df["Close"].iloc[-1] < df["vwap"].iloc[-1]:
        return -1.0
    return 0.0

def supertrend(df: pd.DataFrame) -> float:
    atr   = df["atr"]
    hl2   = (df["High"] + df["Low"]) / 2
    upper = hl2 + 2 * atr
    lower = hl2 - 2 * atr
    if df["Close"].iloc[-1] > upper.iloc[-1]:
        return 1.0
    elif df["Close"].iloc[-1] < lower.iloc[-1]:
        return -1.0
    return 0.0

def bollinger(df: pd.DataFrame) -> float:
    bb = ta.volatility.BollingerBands(df["Close"])
    if df["Close"].iloc[-1] > bb.bollinger_hband().iloc[-1]:
        return 1.0
    elif df["Close"].iloc[-1] < bb.bollinger_lband().iloc[-1]:
        return -1.0
    return 0.0

# ================= AI SENTIMENT ENGINE =================
@st.cache_data(ttl=1800)   # cache 30 min per ticker
def get_ai_sentiment(ticker_clean: str, headlines: list[str],
                     price: float, pct_change: float) -> dict:
    """
    Call Claude to analyse news headlines + price action and return:
    {
      "score":     float in [-1, +1],   # -1 = very bearish, +1 = very bullish
      "label":     "Bullish" | "Neutral" | "Bearish",
      "confidence": int 0–100,
      "summary":   str  (1-2 sentence rationale)
    }
    """
    try:
        client = anthropic.Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"])
    except Exception:
        return _neutral_sentiment("ANTHROPIC_API_KEY not set in secrets.")

    headlines_text = "\n".join(f"- {h}" for h in headlines) if headlines else "No recent headlines available."

    prompt = f"""You are a senior analyst specialising in Indian equity markets (NSE/BSE).

Analyse the following data for {ticker_clean} (NSE) and return a JSON sentiment score.

**Current price:** ₹{price:.2f}
**Today's change:** {pct_change:+.2f}%
**Recent news headlines:**
{headlines_text}

Return ONLY valid JSON with these exact keys (no markdown, no explanation):
{{
  "score": <float between -1.0 (very bearish) and +1.0 (very bullish)>,
  "label": "<Strongly Bullish | Bullish | Neutral | Bearish | Strongly Bearish>",
  "confidence": <integer 0 to 100>,
  "summary": "<1-2 sentence analyst rationale specific to Indian market context>"
}}

Consider:
- News sentiment and relevance to Indian markets
- Price momentum and change
- Macro factors (RBI policy, FII/DII flows, Nifty trend) if inferable
- Sector-specific Indian market dynamics"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        raw  = response.content[0].text.strip()
        data = json.loads(raw.replace("```json", "").replace("```", "").strip())
        # Clamp score
        data["score"] = max(-1.0, min(1.0, float(data["score"])))
        data["confidence"] = max(0, min(100, int(data["confidence"])))
        return data
    except json.JSONDecodeError:
        return _neutral_sentiment("Could not parse AI response.")
    except Exception as e:
        return _neutral_sentiment(str(e))

def _neutral_sentiment(reason: str = "") -> dict:
    return {
        "score":      0.0,
        "label":      "Neutral",
        "confidence": 0,
        "summary":    reason or "Sentiment unavailable.",
    }

# ================= ENSEMBLE SCORER =================
def strategy_score(df: pd.DataFrame) -> tuple[str, float]:
    """Pure technical score — returns (signal_label, score in [-1,+1])."""
    signals = [
        macd_adx(df),
        ema_rsi(df),
        vwap_bounce(df),
        supertrend(df),
        bollinger(df),
    ]
    score = sum(signals) / len(signals)
    if score > 0.4:
        return "BUY", score
    elif score < -0.4:
        return "SELL", score
    return "HOLD", score

def combined_score(tech_score: float, sentiment: dict) -> tuple[str, float]:
    """
    Blend technical score with AI sentiment score.
    combined = (1 - w) * tech + w * sentiment
    where w = SENTIMENT_WEIGHT (default 0.30)
    """
    w   = SENTIMENT_WEIGHT
    s   = sentiment.get("score", 0.0)
    blended = (1 - w) * tech_score + w * s

    if blended > 0.35:
        label = "BUY"
    elif blended < -0.35:
        label = "SELL"
    else:
        label = "HOLD"

    return label, round(blended, 3)

# ================= BACKTEST =================
def backtest(df: pd.DataFrame) -> tuple[float, list[float]]:
    capital  = CAPITAL
    position = 0.0
    entry    = 0.0
    trades   = []

    for i in range(30, len(df) - 1):   # start after indicators warm up
        slice_df         = df.iloc[: i + 1]
        signal, _        = strategy_score(slice_df)
        price            = float(df["Close"].iloc[i])
        next_price       = float(df["Close"].iloc[i + 1])
        atr              = float(df["atr"].iloc[i])
        if atr == 0:
            continue

        if signal == "BUY" and position == 0:
            qty      = (capital * RISK) / atr
            entry    = price
            position = qty

        elif position != 0:
            sl = entry - atr
            tp = entry + 2 * atr
            if next_price <= sl or next_price >= tp or signal == "SELL":
                pnl      = (next_price - entry) * position
                pnl     -= abs(pnl) * BROKERAGE
                capital += pnl
                trades.append(pnl)
                position = 0.0

    return capital, trades

# ================= METRICS =================
def metrics(trades: list[float]) -> tuple[float, float, float]:
    if not trades:
        return 0.0, 0.0, 0.0
    wins   = [t for t in trades if t > 0]
    losses = [t for t in trades if t < 0]
    win_rate = len(wins) / len(trades)
    pf       = (sum(wins) / abs(sum(losses))) if losses else float("inf")
    avg_pnl  = sum(trades) / len(trades)
    return round(win_rate, 3), round(pf, 3), round(avg_pnl, 2)

# ================= FULL SCANNER =================
def scan(ticker: str, mode: str, use_sentiment: bool) -> dict | None:
    try:
        df = get_data(ticker, mode)
        if df is None or len(df) < 40:
            return None
        df = add_features(df)

        # Technical signal
        signal_tech, score_tech = strategy_score(df)

        # Price info for sentiment
        price       = float(df["Close"].iloc[-1])
        prev_close  = float(df["Close"].iloc[-2]) if len(df) > 1 else price
        pct_change  = ((price - prev_close) / prev_close) * 100

        # Sentiment
        ticker_clean = ticker.replace(".NS", "")
        sentiment    = _neutral_sentiment()

        if use_sentiment:
            headlines = get_news_headlines(ticker_clean)
            sentiment = get_ai_sentiment(ticker_clean, headlines, price, pct_change)

        # Blended signal
        final_signal, final_score = combined_score(score_tech, sentiment)

        # Backtest
        final_cap, trades          = backtest(df)
        win_rate, pf, avg_pnl      = metrics(trades)

        return {
            "Stock":          ticker_clean,
            "Price":          round(price, 2),
            "Change%":        round(pct_change, 2),
            # Technical
            "Tech Signal":    signal_tech,
            "Tech Score":     round(score_tech, 3),
            # AI Sentiment
            "Sentiment":      sentiment["label"],
            "Sent Score":     round(sentiment["score"], 3),
            "Sent Conf%":     sentiment["confidence"],
            "AI Summary":     sentiment["summary"],
            # Combined
            "Final Signal":   final_signal,
            "Final Score":    final_score,
            # Backtest
            "Win Rate":       win_rate,
            "Profit Factor":  pf,
            "Avg P&L (₹)":    avg_pnl,
            "Final Capital":  int(final_cap),
            "Total Trades":   len(trades),
        }
    except Exception as e:
        st.warning(f"Error scanning {ticker}: {e}")
        return None

# ================= SIGNAL COLOUR HELPERS =================
def _colour_signal(val: str) -> str:
    if val == "BUY":
        return "background-color:#1a4731;color:#00e5a0;font-weight:600;"
    if val == "SELL":
        return "background-color:#4a1020;color:#ff4d6d;font-weight:600;"
    return "color:#f5b731;"

def _colour_sent(val: str) -> str:
    if "Bullish" in val:
        return "color:#00e5a0;"
    if "Bearish" in val:
        return "color:#ff4d6d;"
    return "color:#f5b731;"

# ================= UI =================
with st.sidebar:
    st.header("⚙️ Settings")
    mode          = st.selectbox("Trading Mode", ["Swing", "Intraday"])
    use_sentiment = st.toggle("🤖 Enable AI Sentiment", value=True,
                              help="Uses Claude to analyse news headlines and price action")
    sent_weight   = st.slider("Sentiment Weight in Score", 0.0, 0.8, SENTIMENT_WEIGHT, 0.05,
                              help="How much AI sentiment influences the final signal")
    SENTIMENT_WEIGHT = sent_weight

    st.divider()
    st.caption("**Signal thresholds**")
    st.caption("BUY  → blended score > 0.35")
    st.caption("SELL → blended score < -0.35")
    st.caption("HOLD → everything else")
    st.divider()
    auto = st.checkbox("⏱️ Auto Refresh (5 min)")
    st.caption("Sentiment cached 30 min | Data cached 5 min")

# ── Main area ──────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("Capital",          f"₹{CAPITAL:,}")
col2.metric("Risk/Trade",       f"{RISK*100:.0f}%")
col3.metric("Sentiment Weight", f"{SENTIMENT_WEIGHT*100:.0f}%")
col4.metric("Stocks in Scanner", str(len(NIFTY50)))

st.divider()

# ── HOW IT WORKS expander ─────────────────────────────────────
with st.expander("ℹ️ How AI Sentiment is Integrated", expanded=False):
    st.markdown(f"""
**Pipeline for each stock:**

1. **Technical Score** — average of 5 strategies (MACD+ADX, EMA+RSI, VWAP, SuperTrend, Bollinger) → range **[-1, +1]**
2. **AI Sentiment Score** — Claude analyses:
   - Up to 8 recent Yahoo Finance news headlines
   - Current price and today's % change
   - Indian market context (RBI, FII/DII, sector dynamics)
   → returns score in **[-1, +1]** with confidence %
3. **Blended Score** = `(1 - {SENTIMENT_WEIGHT:.0%}) × tech_score + {SENTIMENT_WEIGHT:.0%} × sentiment_score`
4. **Final Signal**: BUY if > 0.35 · SELL if < -0.35 · HOLD otherwise
5. **Telegram alert** sent for strong BUY signals (score > 0.6)

> Sentiment is cached 30 min per ticker. Technical data is cached 5 min.
""")

# ── RUN BUTTON ────────────────────────────────────────────────
if st.button("▶ Run Scanner", type="primary", use_container_width=True):

    results     = []
    progress    = st.progress(0, text="Scanning stocks...")
    status_area = st.empty()

    for i, stock in enumerate(NIFTY50):
        pct = (i + 1) / len(NIFTY50)
        progress.progress(pct, text=f"Scanning {stock} ({i+1}/{len(NIFTY50)})...")

        if use_sentiment:
            status_area.info(f"🤖 Getting AI sentiment for {stock}...")

        res = scan(stock, mode, use_sentiment)
        if res:
            results.append(res)

            # Telegram alert for strong combined BUY
            if res["Final Signal"] == "BUY" and res["Final Score"] > 0.6:
                sent_str = f" | Sentiment: {res['Sentiment']} ({res['Sent Conf%']}%)" if use_sentiment else ""
                send_telegram(
                    f"🚀 BUY {res['Stock']}\n"
                    f"Price: ₹{res['Price']} ({res['Change%']:+.2f}%)\n"
                    f"Score: {res['Final Score']}{sent_str}\n"
                    f"Win Rate: {res['Win Rate']*100:.0f}% | PF: {res['Profit Factor']:.2f}\n"
                    f"AI: {res['AI Summary']}"
                )

    progress.empty()
    status_area.empty()

    if not results:
        st.error("No results returned. Check your data connection.")
        st.stop()

    df_results = pd.DataFrame(results).sort_values("Final Score", ascending=False)

    # ── SUMMARY METRICS ────────────────────────────────────────
    buys  = (df_results["Final Signal"] == "BUY").sum()
    sells = (df_results["Final Signal"] == "SELL").sum()
    holds = (df_results["Final Signal"] == "HOLD").sum()
    avg_wr = df_results["Win Rate"].mean() * 100

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🟢 BUY Signals",  buys)
    c2.metric("🔴 SELL Signals", sells)
    c3.metric("🟡 HOLD Signals", holds)
    c4.metric("📊 Avg Win Rate", f"{avg_wr:.1f}%")

    st.divider()

    # ── TABS ───────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs(["📋 Signal Table", "🤖 AI Sentiment Detail", "📈 Backtest Summary"])

    with tab1:
        st.subheader("All Signals — Sorted by Final Score")
        display_cols = [
            "Stock","Price","Change%",
            "Tech Signal","Tech Score",
            "Sentiment","Sent Score","Sent Conf%",
            "Final Signal","Final Score",
            "Win Rate","Profit Factor","Total Trades"
        ]
        disp = df_results[display_cols].copy()

        def style_row(row):
            styles = [""] * len(row)
            for j, col in enumerate(row.index):
                if col == "Final Signal":
                    styles[j] = _colour_signal(row[col])
                elif col == "Tech Signal":
                    styles[j] = _colour_signal(row[col])
                elif col == "Sentiment":
                    styles[j] = _colour_sent(row[col])
                elif col == "Change%":
                    styles[j] = "color:#00e5a0;" if row[col] > 0 else "color:#ff4d6d;"
            return styles

        st.dataframe(
            disp.style.apply(style_row, axis=1).format({
                "Price":          "₹{:.2f}",
                "Change%":        "{:+.2f}%",
                "Tech Score":     "{:.3f}",
                "Sent Score":     "{:.3f}",
                "Sent Conf%":     "{:.0f}%",
                "Final Score":    "{:.3f}",
                "Win Rate":       "{:.1%}",
                "Profit Factor":  "{:.2f}",
            }),
            use_container_width=True,
            height=420,
        )

    with tab2:
        st.subheader("AI Sentiment Breakdown")
        if not use_sentiment:
            st.info("Enable AI Sentiment in the sidebar to see this panel.")
        else:
            for _, row in df_results.iterrows():
                signal_col = (
                    "🟢" if row["Final Signal"] == "BUY"
                    else "🔴" if row["Final Signal"] == "SELL"
                    else "🟡"
                )
                with st.expander(
                    f"{signal_col} **{row['Stock']}** — {row['Sentiment']} "
                    f"(Sent: {row['Sent Score']:+.2f} | Conf: {row['Sent Conf%']}%) "
                    f"| Final: {row['Final Signal']} ({row['Final Score']:+.3f})"
                ):
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Technical Score", f"{row['Tech Score']:+.3f}", row["Tech Signal"])
                    c2.metric("Sentiment Score", f"{row['Sent Score']:+.3f}", row["Sentiment"])
                    c3.metric("Blended Score",   f"{row['Final Score']:+.3f}", row["Final Signal"])
                    st.info(f"🤖 **AI Analysis:** {row['AI Summary']}")

    with tab3:
        st.subheader("Backtest Performance")
        bt_cols = ["Stock","Final Signal","Win Rate","Profit Factor","Avg P&L (₹)","Final Capital","Total Trades"]
        bt_df   = df_results[bt_cols].copy()
        st.dataframe(
            bt_df.style.format({
                "Win Rate":      "{:.1%}",
                "Profit Factor": "{:.2f}",
                "Avg P&L (₹)":   "₹{:.2f}",
                "Final Capital": "₹{:,}",
            }),
            use_container_width=True,
            height=400,
        )

        # Summary bars
        st.divider()
        st.markdown("**Capital outcome distribution**")
        bt_chart = df_results[["Stock","Final Capital"]].set_index("Stock")
        st.bar_chart(bt_chart)

# ── AUTO REFRESH ──────────────────────────────────────────────
if auto:
    st.toast("Auto-refreshing in 5 minutes…")
    time.sleep(300)
    st.rerun()
