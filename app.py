import threading
import time
from datetime import datetime, timezone

import pandas as pd
import requests
import socketio
import streamlit as st

st.set_page_config(page_title="Solana Early Radar", page_icon="🚨", layout="wide")

FEED_URL = "https://sol-ams.shrine.trade"
PROTOCOLS = ["PUMPFUN", "PUMPSWAP", "METEORA", "RAYDIUM", "ORCA", "BONK", "STONKFUN"]
MAX_ROWS = 250

def now_utc():
    return datetime.now(timezone.utc)

def parse_time(value):
    if value is None:
        return now_utc()
    try:
        if isinstance(value, (int, float)):
            # Accept Unix seconds or milliseconds.
            if value > 10_000_000_000:
                value = value / 1000
            return datetime.fromtimestamp(value, tz=timezone.utc)
        s = str(value).strip()
        if s.isdigit():
            return parse_time(int(s))
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return now_utc()

def first(d, *keys, default=None):
    for key in keys:
        if isinstance(d, dict) and d.get(key) is not None:
            return d.get(key)
    return default

def ensure_state():
    if "radar_data" not in st.session_state:
        st.session_state.radar_data = {"tokens": {}, "status": "Starting live feed…", "last_event": None}
    return st.session_state.radar_data

# Shared server-side state survives Streamlit reruns.
@st.cache_resource
def shared_store():
    return {"lock": threading.Lock(), "tokens": {}, "status": "Connecting to live launch feed…",
            "last_event": None, "started": False, "errors": []}

def update_token(store, mint, **updates):
    if not mint:
        return
    with store["lock"]:
        row = store["tokens"].get(mint, {})
        row.update(updates)
        row["mint"] = mint
        row["last_seen"] = now_utc()
        if "first_seen" not in row:
            row["first_seen"] = now_utc()
        store["tokens"][mint] = row
        # Keep newest items and cap memory use.
        if len(store["tokens"]) > MAX_ROWS:
            ordered = sorted(store["tokens"].items(),
                             key=lambda kv: kv[1].get("first_seen", now_utc()), reverse=True)
            store["tokens"] = dict(ordered[:MAX_ROWS])

def background_listener(store):
    sio = socketio.Client(reconnection=True, logger=False, engineio_logger=False)

    @sio.event
    def connect():
        with store["lock"]:
            store["status"] = "LIVE — connected to Shrine feed"
            store["errors"] = []
        try:
            sio.emit("subscribe_new_tokens", {"protocols": PROTOCOLS})
        except Exception as e:
            with store["lock"]:
                store["errors"].append("Subscribe error: " + str(e))
        try:
            sio.emit("subscribe_stream", {"protocols": PROTOCOLS,
                                          "actions": ["create", "buy", "sell", "migrate",
                                                      "liquidity_add", "liquidity_remove"]})
        except Exception:
            pass

    @sio.event
    def disconnect():
        with store["lock"]:
            store["status"] = "Reconnecting to live feed…"

    @sio.on("new_token")
    def on_new_token(data):
        if not isinstance(data, dict):
            return
        mint = first(data, "mint", "tokenMint", "token_mint", "address")
        if not mint:
            return
        update_token(
            store, str(mint),
            name=first(data, "name", "tokenName", "token_name", default=""),
            symbol=first(data, "symbol", "ticker", default=""),
            protocol=str(first(data, "protocol", "source", default="Unknown")).upper(),
            pool=first(data, "pool", "poolAddress", "pool_address", default=""),
            launch_time=parse_time(first(data, "timestamp", "createdAt", "created_at")),
            launch_seen=now_utc(),
            launch_event=data,
        )
        with store["lock"]:
            store["last_event"] = now_utc()

    @sio.on("stream")
    def on_stream(data):
        if not isinstance(data, dict):
            return
        mint = first(data, "mint", "tokenMint", "token_mint", "baseMint", "base_mint")
        if not mint:
            return
        action = str(first(data, "action", "type", "event", default="")).lower()
        updates = {"last_action": action or "activity", "last_activity": now_utc()}
        if action == "buy":
            updates["buys_delta"] = 1
        elif action == "sell":
            updates["sells_delta"] = 1
        for key in ("priceUSD", "price_usd", "mcapUSD", "marketCapUSD", "market_cap_usd",
                    "liquidityUSD", "liquidity_usd", "volumeUSD", "volume_usd"):
            if data.get(key) is not None:
                norm = {"priceUSD":"price_usd", "price_usd":"price_usd",
                        "mcapUSD":"mcap_usd", "marketCapUSD":"mcap_usd",
                        "market_cap_usd":"mcap_usd", "liquidityUSD":"liquidity_usd",
                        "liquidity_usd":"liquidity_usd", "volumeUSD":"volume_usd",
                        "volume_usd":"volume_usd"}[key]
                updates[norm] = data[key]
        update_token(store, str(mint), **updates)
        with store["lock"]:
            store["last_event"] = now_utc()

    @sio.on("token_update")
    def on_token_update(data):
        if not isinstance(data, dict):
            return
        mint = first(data, "mint", "tokenMint", "token_mint", "address")
        if not mint:
            return
        updates = {}
        mapping = {
            "name": ["name"], "symbol": ["symbol", "ticker"],
            "price_usd": ["priceUSD", "price_usd"],
            "mcap_usd": ["mcapUSD", "marketCapUSD", "market_cap_usd"],
            "liquidity_usd": ["liquidityUSD", "liquidity_usd"],
            "volume_usd": ["volumeUSD", "volume_usd"],
        }
        for target, keys in mapping.items():
            value = first(data, *keys)
            if value is not None:
                updates[target] = value
        update_token(store, str(mint), **updates)
        with store["lock"]:
            store["last_event"] = now_utc()

    while True:
        try:
            with store["lock"]:
                store["status"] = "Connecting to live launch feed…"
            sio.connect(FEED_URL, transports=["websocket", "polling"], wait_timeout=20)
            sio.wait()
        except Exception as e:
            with store["lock"]:
                store["status"] = "Feed connection retrying…"
                store["errors"] = [str(e)[:250]]
            time.sleep(5)
        finally:
            try:
                if sio.connected:
                    sio.disconnect()
            except Exception:
                pass

store = shared_store()
if not store["started"]:
    store["started"] = True
    threading.Thread(target=background_listener, args=(store,), daemon=True).start()

st.title("🚨 Solana Early Radar")
st.caption("Nieuwe Solana-token launches • automatisch bijgewerkt • bedoeld om vroeg te signaleren, niet als garantie op winst.")
with store["lock"]:
    status = store["status"]
    rows = [dict(v) for v in store["tokens"].values()]
    last_event = store["last_event"]
    errors = list(store["errors"])

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    st.markdown(f"**Feedstatus:** {status}")
with col2:
    st.metric("Tokens gezien", len(rows))
with col3:
    st.metric("Laatste feed-event", last_event.strftime("%H:%M:%S UTC") if last_event else "Nog geen")

st.divider()
left, right = st.columns([1, 2])
with left:
    min_cap = st.number_input("Min. market cap ($)", min_value=0, max_value=100000000, value=1000, step=500)
    max_cap = st.number_input("Max. market cap ($)", min_value=0, max_value=1000000000, value=5000, step=500)
    show_unknown = st.checkbox("Toon tokens zonder bekende market cap", value=True)
    protocols_selected = st.multiselect("Launch-platforms", PROTOCOLS, default=PROTOCOLS)
    st.caption("Market cap/liquiditeit zijn alleen zichtbaar als de feed die gegevens levert. Een nieuwe launch kan eerst nog geen betrouwbare marktdata hebben.")
with right:
    st.info("Gebruik de mint- en chartlinks om zelf te controleren. Nieuwe tokens zijn vaak extreem riskant; dit scherm voert geen trades uit.")

def fmt_money(x):
    try:
        n = float(x)
        if n >= 1_000_000:
            return f"${n/1_000_000:.2f}M"
        if n >= 1000:
            return f"${n/1000:.1f}K"
        return f"${n:.2f}"
    except Exception:
        return "—"

def age_text(dt):
    try:
        secs = max(0, int((now_utc() - dt).total_seconds()))
        if secs < 60: return f"{secs}s"
        if secs < 3600: return f"{secs//60}m"
        if secs < 86400: return f"{secs//3600}h"
        return f"{secs//86400}d"
    except Exception:
        return "—"

filtered = []
for r in rows:
    protocol = str(r.get("protocol", "Unknown")).upper()
    if protocols_selected and protocol not in protocols_selected:
        continue
    cap = r.get("mcap_usd")
    try:
        cap = float(cap) if cap is not None else None
    except Exception:
        cap = None
    if cap is None:
        if not show_unknown:
            continue
    elif cap < min_cap or cap > max_cap:
        continue
    filtered.append(r)

filtered.sort(key=lambda r: r.get("launch_seen", r.get("first_seen", now_utc())), reverse=True)
st.subheader(f"🆕 Nieuwe launches ({len(filtered)})")
if not filtered:
    st.write("Nog geen matches. Laat deze pagina open; nieuwe launches verschijnen zodra de live feed ze doorstuurt.")
else:
    for r in filtered[:100]:
        symbol = str(r.get("symbol") or "").strip()
        name = str(r.get("name") or "").strip()
        title = (f"${symbol} — {name}" if symbol and name else f"${symbol}" if symbol else name or str(r.get("mint", ""))[:12] + "…")
        launch_time = r.get("launch_time") or r.get("launch_seen") or r.get("first_seen") or now_utc()
        cap = r.get("mcap_usd")
        mint = str(r.get("mint", ""))
        chart = f"https://dexscreener.com/solana/{mint}"
        rug = f"https://rugcheck.xyz/tokens/{mint}"
        with st.container(border=True):
            a, b, c, d = st.columns([3, 1, 1, 1])
            with a:
                st.markdown(f"**{title}**")
                st.caption(f"{r.get('protocol','Unknown')} · leeftijd {age_text(launch_time)} · mint `{mint[:8]}…{mint[-6:]}`")
            with b:
                st.metric("Market cap", fmt_money(cap) if cap is not None else "Onbekend")
            with c:
                st.metric("Liquiditeit", fmt_money(r.get("liquidity_usd")))
            with d:
                st.markdown(f"[📈 Chart]({chart})")
                st.markdown(f"[🛡️ Rugcheck]({rug})")
            st.caption(f"Laatste activiteit: {r.get('last_action','—')} · Prijs: {fmt_money(r.get('price_usd'))} · Pool: {str(r.get('pool') or 'nog onbekend')[:18]}")

if errors:
    with st.expander("Verbindingsdetails"):
        for err in errors:
            st.write(err)

st.caption("Feedbron: Shrine live launch/stream API. Data kan ontbreken of vertraagd zijn; controleer contract, liquiditeit, holders en verkoopbaarheid voordat je risico neemt.")
time.sleep(2)
st.rerun()
