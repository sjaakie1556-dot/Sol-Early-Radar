# Solana Early Radar (Streamlit)

## Deploy
1. Upload `app.py` and `requirements.txt` to a new GitHub repository.
2. Open Streamlit Community Cloud and choose **New app**.
3. Select your repository, branch, and `app.py`, then deploy.
4. Open the Streamlit app on your iPhone. Keep the page open for live updates.

## What it does
- Connects to Shrine's public Socket.IO feed from the Amsterdam endpoint.
- Subscribes to new-token events across Pump.fun/PumpSwap, Meteora, Raydium, Orca, Bonk, and StonkFun.
- Shows new launches, token mints, chart and RugCheck links, and market data when available.

## Important
The feed may not provide market cap or liquidity for every launch immediately. The scanner shows `Onbekend` when the data is not present rather than inventing values. It is a monitoring tool, not financial advice or a guarantee of catching a pump.
