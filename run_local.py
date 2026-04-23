#!/usr/bin/env python3
"""
run_local.py — One-click launcher for NSE Pro Trader
=====================================================
Works on Windows, Mac, Linux.
Run:  python run_local.py

What it does:
  1. Checks Python version
  2. Installs all dependencies
  3. Creates .streamlit/secrets.toml template if missing
  4. Launches the Streamlit app in your browser
"""
import sys, os, subprocess, platform

print("\n" + "="*60)
print("  NSE PRO TRADER — Local Launcher")
print("="*60)

# 1. Python version check
if sys.version_info < (3, 9):
    print(f"\n❌ Python 3.9+ required. You have {sys.version}")
    print("   Download: https://www.python.org/downloads/")
    sys.exit(1)
print(f"✅ Python {sys.version.split()[0]}")

# 2. Install dependencies
print("\n📦 Installing dependencies...")
result = subprocess.run(
    [sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "-q"],
    capture_output=True, text=True
)
if result.returncode != 0:
    print(f"❌ pip install failed:\n{result.stderr}")
    sys.exit(1)
print("✅ All packages installed")

# 3. Create secrets template
secrets_dir  = os.path.join(".streamlit")
secrets_path = os.path.join(secrets_dir, "secrets.toml")
os.makedirs(secrets_dir, exist_ok=True)

if not os.path.exists(secrets_path):
    with open(secrets_path, "w") as f:
        f.write("""# NSE Pro Trader — Secrets
# Fill in your keys. This file is never committed to git.

# ── Zerodha Kite Connect (FREE Personal API) ──────────────────
# Get from: https://developers.kite.trade → Create App → Personal
KITE_API_KEY    = "your_api_key_here"
KITE_API_SECRET = "your_api_secret_here"

# ── Claude AI Sentiment (optional) ───────────────────────────
# Get from: https://console.anthropic.com → API Keys
ANTHROPIC_API_KEY = "sk-ant-..."

# ── Telegram Alerts (optional) ───────────────────────────────
# Create bot via @BotFather on Telegram
TELEGRAM_TOKEN   = "your_bot_token"
TELEGRAM_CHAT_ID = "your_chat_id"
""")
    print(f"\n📝 Created {secrets_path}")
    print("   → Fill in your API keys before running for full functionality")
else:
    print(f"✅ Secrets file exists: {secrets_path}")

# 4. Test data connectivity
print("\n🌐 Testing data connection...")
try:
    import yfinance as yf
    df = yf.download("RELIANCE.NS", period="5d", interval="1d",
                     auto_adjust=True, progress=False)
    if len(df) > 0:
        print(f"✅ Yahoo Finance works — RELIANCE.NS last close: ₹{df['Close'].iloc[-1]:.2f}")
    else:
        print("⚠️  Yahoo Finance returned empty data (may need VPN or Kite connection)")
except Exception as e:
    print(f"⚠️  Yahoo Finance error: {e}")
    print("   → App will still work via Zerodha Kite API once you connect")

# 5. Launch
print("\n🚀 Launching NSE Pro Trader...")
print("   → App will open at: http://localhost:8501")
print("   → Press Ctrl+C to stop\n")
os.execvp(sys.executable, [sys.executable, "-m", "streamlit", "run",
                            "pro_trading_system.py",
                            "--server.headless=false",
                            "--server.port=8501",
                            "--browser.gatherUsageStats=false"])
