# import sys
# import os
# import websocket
import json
import time
import hmac
import hashlib
# from concurrent.futures import ThreadPoolExecutor
# import threading
# from collections import defaultdict
from datetime import datetime, timezone, timedelta


import hmac
import hashlib
import json
import time
import requests
import time
from datetime import datetime

from binance import get_binance_funding,dcx_to_binance_symbol,get_binance_funding_safe

# আপনার API Key এবং Secret এখানে দিন
# API_KEY = "YOUR_API_KEY"  # CoinDCX থেকে API Key নিন
# API_SECRET = "YOUR_API_SECRET"  # CoinDCX থেকে API Secret নিন

  # Enter your API Key and Secret here. If you don't have one, you can generate it from the website.
API_KEY = "88b0853aa09e2bc5a0fab1917c8bf176783ee459e5e9cc8c"
API_SECRET = "fce4b5da22a410a0b5fcddd6de1ae26202cc8dc91980cd51e7a4d1df4c1efabb"
BASE_URL = "https://api.coindcx.com"
# Secret-কে bytes-এ convert করুন
secret_bytes = bytes(API_SECRET, encoding='utf-8')

# Timestamp generate করুন (milliseconds-এ)
def get_timestamp():
    return int(round(time.time() * 1000))

# Signature generate করার function
def generate_signature(body):
    json_body = json.dumps(body, separators=(',', ':'))
    return hmac.new(secret_bytes, json_body.encode(), hashlib.sha256).hexdigest()

# API headers তৈরি করুন
def get_headers(body):
    return {
        'Content-Type': 'application/json',
        'X-AUTH-APIKEY': API_KEY,
        'X-AUTH-SIGNATURE': generate_signature(body)
    }






def get_balance_dcx():

    body = {"timestamp": get_timestamp()}
    json_body = json.dumps(body, separators=(',', ':'))
    url = "https://api.coindcx.com/exchange/v1/users/balances"

    response = requests.post(url, data=json_body, headers=get_headers(body))
    data = response.json()
    print(data)
    return data

def get_futures_balance_dcx():
    body = {"timestamp": get_timestamp()}
    json_body = json.dumps(body, separators=(',', ':'))
    # url = "https://api.coindcx.com/exchange/v1/derivatives/futures/user/balances"
    # url = "https://api.coindcx.com/exchange/v1/derivatives/futures/wallets"
    # url = "https://api.coindcx.com/exchange/v1/derivatives/futures/user/balances"

    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/wallets"
    

    response = requests.get(url, data=json_body, headers=get_headers(body))
    data = response.json()
    print(data)
    return data

# Timestamp convert (milliseconds to human-readable)
def ms_to_datetime(ms):
    return datetime.fromtimestamp(ms / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

# 1. Current Funding Rate চেক করা (Get Active Instruments)
# 💰 Main function
def get_current_funding_rate(sym):

    symbol = normalize_pair(sym)
    print("symbol :",symbol)
    
    """
    Example:
      get_current_funding_rate("B-BTC_USDT")
      get_current_funding_rate("BTCUSDT")
    দুটোই কাজ করবে ✅
    """
    url = "https://public.coindcx.com/market_data/v3/current_prices/futures/rt"
    body = {"timestamp": get_timestamp()}
    
    try:
        response = requests.get(url, headers=get_headers(body))
        response.raise_for_status()
        data = response.json()
        prices = data.get("prices", {})

        # ✅ Symbol normalization logic
        normalized_symbol = symbol
        print("normalized_symbol :",normalized_symbol)
        if not symbol.startswith("B-"):
            # BTCUSDT বা BTC_USDT বা btcusdt — সব ধরার জন্য normalize
            for key in prices.keys():
                if key.replace("-", "").replace("_", "").lower() == ("B-" + symbol).replace("-", "").replace("_", "").lower():
                    normalized_symbol = key
                    break
                elif key.replace("-", "").replace("_", "").lower() == ("B-" + symbol).lower():
                    normalized_symbol = key
                    break

        btc_data = prices.get(normalized_symbol, {})
        if not btc_data:
            print(f"❌ Symbol '{symbol}' not found!")
            return None
        print("btc_data",btc_data)
        print(f"\n📊 {btc_data.get('mkt', 'N/A')} Perpetual Futures Details:")
        print(f"Funding Rate: {btc_data.get('fr', 'N/A')} ({btc_data.get('fr', 0) * 100:.4f}%)")
        print(f"Effective Funding Rate: {btc_data.get('efr', 'N/A')} ({btc_data.get('efr', 0) * 100:.4f}%)")
        print(f"Last Price: {btc_data.get('ls', 'N/A')} USDT")
        print(f"Mark Price: {btc_data.get('mp', 'N/A')} USDT")
        print(f"24h High: {btc_data.get('h', 'N/A')} USDT")
        print(f"24h Low: {btc_data.get('l', 'N/A')} USDT")
        print(f"24h Price Change: {btc_data.get('pc', 'N/A')}%")
        print(f"24h Volume: {btc_data.get('v', 'N/A')} contracts")
        print(f"Market Skew: {btc_data.get('skw', 'N/A')} ({'Long bias' if btc_data.get('skw', 0) > 0 else 'Short bias' if btc_data.get('skw', 0) < 0 else 'Neutral'})")
        print(f"Market: {btc_data.get('mkt', 'N/A')}")
        print(f"Bid Timestamp: {ms_to_datetime(btc_data.get('btST', 0))}")
        print(f"Contract Timestamp: {ms_to_datetime(btc_data.get('ctRT', 0))}")

        return btc_data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching funding rate: {e}")
        return None


def format_time_remaining(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02}:{minutes:02}:{secs:02}"



def parse_dcx_funding_data(data):
    """
    Convert CoinDCX funding data to Delta-like readable dict
    """

    # print(data)

    # -------- funding rate --------
    funding_rate = data.get("fr", 0)
    effective_funding_rate = data.get("efr", funding_rate)

    # -------- next funding time --------
    next_funding_ms = data.get("bmST")  # milliseconds
    if next_funding_ms:
        next_funding_ts = int(next_funding_ms / 1000)
        now_ts = int(time.time())

        # DCX funding is every 8h → roll forward if expired
        while next_funding_ts <= now_ts:
            next_funding_ts += 8 * 3600

        seconds_remaining = max(0, next_funding_ts - now_ts)
        next_funding_in = format_time_remaining(seconds_remaining)
    else:
        next_funding_in = "--"

    # -------- timestamps --------
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc + timedelta(hours=5, minutes=30)

    return {
        "symbol": data.get("market") or data.get("symbol"),
        # "funding_rate": funding_rate,
        "funding_rate": f"{data.get('efr', 'N/A')} ({data.get('efr', 0) * 100:.4f}%)",
        "funding_rate_8h": funding_rate * 8,
        "predicted_funding_rate": f"{data.get('efr', 'N/A')} ({data.get('efr', 0) * 100:.4f}%)",
        "predicted_funding_rate_8h": effective_funding_rate * 8,
        "next_funding_in": next_funding_in,
        "mark_price": data.get("mp"),
        "timestamp_utc": now_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "timestamp_ist": now_ist.strftime("%Y-%m-%d %H:%M:%S"),
    }        


# ✅ মূল function: funding_list return করবে
def get_dcx_funding_rate():
    url = "https://public.coindcx.com/market_data/v3/current_prices/futures/rt"
    body = {"timestamp": get_timestamp()}
    
    try:
        response = requests.get(url, headers=get_headers(body))
        response.raise_for_status()
        data = response.json()

        funding_list = []
        for symbol, info in data.get("prices", {}).items():
            fr_raw = info.get("fr", 0)                 # আসল funding rate
            fr_percent = fr_raw * 100                  # % তে রূপান্তর
            funding_list.append({
            "symbol": symbol,
            "market": info.get("mkt", "N/A"),
            "funding_rate_percent": fr_percent,
            "abs_rate": abs(fr_percent),            # sorting এর জন্য আলাদা key
             **info
            })

    # ২️⃣ বড় থেকে ছোট (descending order) ক্রমে সাজানো — abs() দিয়ে
        sorted_funding = sorted(funding_list, key=lambda x: x["abs_rate"], reverse=True)

        # ৩️⃣ প্রিন্ট করা (optional)
        # print("📊 Funding Rate (Highest → Lowest):\n")
        # for i, item in enumerate(sorted_funding[:100], 1):  # শুধু Top 10 দেখানো
        #     print(f"{i}. {item['market']} → {item['funding_rate_percent']:.4f}%")

        # total_markets = len(data.get("prices", {}))
        # print("\nTotal markets found:", total_markets)

        # ✅ এখন return করবে পুরো sorted list
        return sorted_funding

    except requests.exceptions.RequestException as e:
        print(f"Error fetching funding rate: {e}")
        return []        
    






# ================= CONFIG =================
FUNDING_DIFF_THRESHOLD = 0.0005  # <-- CHANGE THIS

# ================= HELPERS =================
def normalize_symbol(sym: str):
    sym = sym.replace("-USDT-SWAP", "USD")
    sym = sym.replace("USDT", "USD")
    return sym

def dcx_map_builder(dcx_data):
    m = {}
    for d in dcx_data:
        sym = normalize_symbol(d.get("market") or d.get("symbol"))
        m[sym] = {
            # "funding": d.get("fr", 0) * 100,  # convert to percentage
            "funding":d.get('efr', 0) * 100,
            # "funding_percent": (f"{d.get('fr', 'N/A')} ({d.get('fr', 0) * 100:.4f}%)"),
            "funding_percent":(f"{d.get('efr', 'N/A')} ({d.get('efr', 0) * 100:.4f}%)"),
            "mark_price": d.get("mp", 0),
            "funding_time_ms": d.get("bmST")
        }
    return m

# def countdown_from_ms(ms):
#     if not ms:
#         return "--"
#     now = datetime.now(timezone.utc).timestamp()
#     diff = int(ms/1000 - now)
#     if diff <= 0:
#         return "00:00:00"
#     h = diff // 3600
#     m = (diff % 3600) // 60
#     s = diff % 60
#     return f"{h:02}:{m:02}:{s:02}"

# def countdown_from_ms(ms):
#     # print("ms",ms)
#     if not ms:
#         return "--"

#     now = int(datetime.now(timezone.utc).timestamp())
#     base = int(ms / 1000)

#     while base <= now:
#         base += 8 * 3600   # add next funding cycle

#     diff = base - now
#     h = diff // 3600
#     m = (diff % 3600) // 60
#     s = diff % 60
#     return f"{h:02}:{m:02}:{s:02}"


def countdown_from_ms(ms):
    if not ms:
        return "--"

    # 🔥 যদি আগেই formatted string আসে
    if isinstance(ms, str):
        return ms

    try:
        ms = int(ms)
    except (ValueError, TypeError):
        return "--"

    now = int(datetime.now(timezone.utc).timestamp())
    base = ms // 1000

    while base <= now:
        base += 8 * 3600   # Binance funding cycle = 8h

    diff = base - now
    h = diff // 3600
    m = (diff % 3600) // 60
    s = diff % 60
    return f"{h:02}:{m:02}:{s:02}"



def arbitrage_signal(ex_f, dcx_f):
    # print("ex_f",ex_f)
    # print("dcx_f",dcx_f)
    diff = ex_f - dcx_f
    if diff > FUNDING_DIFF_THRESHOLD:
        return "SHORT EX / LONG DCX", diff
    if diff < -FUNDING_DIFF_THRESHOLD:
        return "LONG EX / SHORT DCX", diff
    return "HOLD", diff
# ================= CONFIG =================





# ==============================
# MAIN FUNCTION
# ==============================


def normalize_pair(pair: str) -> str:
    pair = pair.upper()

    # remove separators
    clean = pair.replace("-", "").replace("_", "")

    # force USDT market
    if clean.endswith("USDT"):
        base = clean[:-4]   # remove USDT
    elif clean.endswith("USD"):
        base = clean[:-3]   # remove USD
    else:
        # fallback: assume last 4 is USDT
        base = clean.replace("USDT", "").replace("USD", "")

    return f"B-{base}_USDT"


def get_all_dcx_futures_symbols():
    url = "https://public.coindcx.com/market_data/v3/current_prices/futures/rt"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()

        symbols = []
        for _, info in data.get("prices", {}).items():
            mkt = info.get("mkt")
            if mkt:
                # BTC-USDT → BTCUSD
                symbols.append(normalize_symbol(mkt))

        return sorted(set(symbols))

    except Exception as e:
        print("Error fetching futures symbols:", e)
        return []



def get_futures_instrument_data_multi(symbols=None, margin="USDT"):
    """
    symbols:
      None / []        → ALL futures instruments
      ["BTCUSD", ...]  → filtered
    """

    # 🔹 normalize & clean symbols list
    if symbols:
        symbols = [s for s in symbols if s and s.strip()]

    # 🔥 symbols empty হলে ALL নাও
    if not symbols:
        # symbols = get_all_dcx_futures_symbols()
        # print(symbols)
        symbols = ""
        return {}

    result = {}

    for sym in symbols:
        print(sym)
        resp = get_futures_instrument_data(pair=sym, margin=margin)

        if resp.get("success"):
            key = normalize_symbol(sym)
            result[key] = resp["parsed"]
        else:
            result[sym] = {
                "error": resp.get("error", "unknown error")
            }

    return result






def get_futures_instrument_data(pair="B-SOPH_USDT", margin="USDT" ):
    
    # ✅ Symbol normalization logic
    normalized_symbol = normalize_pair(pair)

    # print("normalized_symbol :",normalized_symbol)        



    url = f"{BASE_URL}/exchange/v1/derivatives/futures/data/instrument"
    body = {
        "pair": normalized_symbol,
        "margin_currency_short_name": margin,
        "timestamp": get_timestamp()
    }

    try:
        response = requests.get(url, headers=get_headers(body), params=body, timeout=10)
        response.raise_for_status()
        raw_data = response.json()

        instrument = raw_data.get("instrument", {})

        # ==============================
        # SAFE EXTRACTION
        # ==============================
        parsed_data = {
            "pair": instrument.get("pair"),
            "status": instrument.get("status"),
            "kind": instrument.get("kind"),
            "margin_currency": instrument.get("margin_currency_short_name"),

            # Funding
            "funding_frequency_hours": instrument.get("funding_frequency"),

            # Leverage
            "max_leverage_long": instrument.get("max_leverage_long"),
            "max_leverage_short": instrument.get("max_leverage_short"),
            "dynamic_leverage": instrument.get("dynamic_position_leverage_details"),

            # Order rules
            "min_qty": instrument.get("min_quantity"),
            "max_qty": instrument.get("max_quantity"),
            "qty_step": instrument.get("quantity_increment"),
            "price_step": instrument.get("price_increment"),
            "min_notional": instrument.get("min_notional"),

            # Fees
            "maker_fee": instrument.get("maker_fee"),
            "taker_fee": instrument.get("taker_fee"),

            # Risk
            "liquidation_fee": instrument.get("liquidation_fee"),
            "safety_margin": instrument.get("dynamic_safety_margin_details"),

            # Contract
            "is_inverse": instrument.get("is_inverse"),
            "is_quanto": instrument.get("is_quanto"),
            "contract_value": instrument.get("unit_contract_value"),
            

            # Time
            "expiry_time": instrument.get("expiry_time"),
            "exit_only": instrument.get("exit_only"),
        }

        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "raw": raw_data,          # full raw data
            "parsed": parsed_data     # cleaned useful data
        }

    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "error": str(e)
        }

def get_get_data():
    # url = "https://api.coindcx.com/exchange/v1/derivatives/futures/funding_rate"
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/instrument?pair=B-SOPH_USDT&margin_currency_short_name=USDT"
    # url = "https://api.coindcx.com/exchange/v1/funding/settle"
    
    body = {"timestamp": get_timestamp()}
    
    try:
        response = requests.get(url, headers=get_headers(body))
        response.raise_for_status()
        data = response.json()

        print("data :",data)


    except requests.exceptions.RequestException as e:
        print(f"Error fetching funding rate: {e}")
        # return []        
    






import time
import json
import hmac
import hashlib
import requests


BASE_URL = "https://api.coindcx.com"


def _sign_payload(api_secret, payload):
    json_body = json.dumps(payload, separators=(",", ":"))
    signature = hmac.new(
        api_secret.encode("utf-8"),
        json_body.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return json_body, signature


# def smart_close_order_or_position(api_key, api_secret, order_id, pair):
    # """
    # Smart close logic:
    # - If order is OPEN -> cancel order
    # - If order is FILLED -> exit position
    # """

    # timestamp = int(time.time() * 1000)

    # # -------------------------------
    # # 1️⃣ FETCH ORDER STATUS
    # # -------------------------------
    # list_order_payload = {
    #     "timestamp": timestamp,
    #     "status": "open,filled,partially_filled,untriggered",
    #     "side": "buy,sell",
    #     "page": "1",
    #     "size": "50",
    #     "margin_currency_short_name": ["INR"]
    # }

    # body, signature = _sign_payload(api_secret, list_order_payload)

    # headers = {
    #     "Content-Type": "application/json",
    #     "X-AUTH-APIKEY": api_key,
    #     "X-AUTH-SIGNATURE": signature
    # }

    # resp = requests.post(
    #     f"{BASE_URL}/exchange/v1/derivatives/futures/orders",
    #     headers=headers,
    #     data=body
    # )

    # orders = resp.json()
    # target_order = next((o for o in orders if o["id"] == order_id), None)

    # if not target_order:
    #     return {"error": "Order not found"}

    # order_status = target_order["status"].lower()

    # # --------------------------------
    # # 2️⃣ IF ORDER IS OPEN → CANCEL
    # # --------------------------------
    # if order_status in ["open", "partially_filled", "untriggered"]:
    #     cancel_payload = {
    #         "timestamp": int(time.time() * 1000),
    #         "id": order_id
    #     }

    #     body, signature = _sign_payload(api_secret, cancel_payload)

    #     resp = requests.post(
    #         f"{BASE_URL}/exchange/v1/derivatives/futures/orders/cancel",
    #         headers=headers,
    #         data=body
    #     )

    #     return {
    #         "action": "order_cancelled",
    #         "response": resp.json()
    #     }

    # # --------------------------------
    # # 3️⃣ IF ORDER IS FILLED → EXIT POSITION
    # # --------------------------------
    # if order_status == "filled":
    #     pos_payload = {
    #         "timestamp": int(time.time() * 1000),
    #         "page": "1",
    #         "size": "20",
    #         "pairs": pair,
    #         "margin_currency_short_name": ["INR"]
    #     }

    #     body, signature = _sign_payload(api_secret, pos_payload)

    #     resp = requests.post(
    #         f"{BASE_URL}/exchange/v1/derivatives/futures/positions",
    #         headers=headers,
    #         data=body
    #     )

    #     positions = resp.json()
    #     active_position = next(
    #         (p for p in positions if abs(p["active_pos"]) > 0),
    #         None
    #     )

    #     if not active_position:
    #         return {"error": "No active position found"}

    #     exit_payload = {
    #         "timestamp": int(time.time() * 1000),
    #         "id": active_position["id"]
    #     }

    #     body, signature = _sign_payload(api_secret, exit_payload)

    #     resp = requests.post(
    #         f"{BASE_URL}/exchange/v1/derivatives/futures/positions/exit",
    #         headers=headers,
    #         data=body
    #     )

    #     return {
    #         "action": "position_exited",
    #         "position_id": active_position["id"],
    #         "response": resp.json()
    #     }

    # return {"error": f"Unhandled order status: {order_status}"}




def smart_close_order_or_position(api_key, api_secret, order_id, pair):

    timestamp = int(time.time() * 1000)

    list_payload = {
        "timestamp": timestamp,
        "status": "initial,open,filled,partially_filled,untriggered",
        "side": "buy,sell",
        "page": "1",
        "size": "50",
        "margin_currency_short_name": ["INR", "USDT"]
        
    }

    body, sig = _sign_payload(api_secret, list_payload)

    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": api_key,
        "X-AUTH-SIGNATURE": sig
    }

    resp = requests.post(
        f"{BASE_URL}/exchange/v1/derivatives/futures/orders",
        headers=headers,
        data=body
    )

    orders_data = resp.json()

    # 🔥 SAFE PARSING
    if isinstance(orders_data, dict):
        orders = orders_data.get("orders", [])
    elif isinstance(orders_data, list):
        orders = orders_data
    else:
        orders = []

    target = next((o for o in orders if o.get("id") == order_id), None)
    if not target:
        return {"error": "Order not found", "orders_seen": len(orders)}

    status = target.get("status", "").lower()

    # ---------- CANCEL ----------
    if status in ["initial", "open", "partially_filled", "untriggered"]:
        cancel_payload = {
            "timestamp": int(time.time() * 1000),
            "id": order_id
        }

        body, sig = _sign_payload(api_secret, cancel_payload)
        headers["X-AUTH-SIGNATURE"] = sig

        resp = requests.post(
            f"{BASE_URL}/exchange/v1/derivatives/futures/orders/cancel",
            headers=headers,
            data=body
        )

        return {
            "action": "order_cancelled",
            "order_status": status,
            "response": resp.json()
        }

    # ---------- EXIT ----------
    if status == "filled":
        return {
            "action": "filled",
            "note": "Position exit logic needed separately"
        }

    return {"error": f"Unhandled status {status}"}


def ts():
    return datetime.now().strftime("%H:%M:%S.%f")



def place_futures_order(
    api_key: str,
    api_secret: str,
    side: str,
    pair: str,
    order_type: str,
    quantity: float,
    leverage: int = None,
    price: float = None,
    stop_price: float = None,
    take_profit_price: float = None,
    stop_loss_price: float = None,
    notification: str = "email_notification",
    time_in_force: str = None,
    margin_currency: str = "INR",
    position_margin_type: str = "isolated"
):
    """
    Places a CoinDCX Futures order

    order_type:
      market_order
      limit_order
      stop_market
      stop_limit
      take_profit_market
      take_profit_limit
    """

    print(f"dcx place order : {ts()}")

    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/orders/create"

    timestamp = int(time.time() * 1000)

    order = {
        "side": side,
        "pair": pair,
        "order_type": order_type,
        "total_quantity": quantity,
        "notification": notification,
        "margin_currency_short_name": margin_currency,
        "position_margin_type": position_margin_type
    }

    # Optional fields
    if leverage is not None:
        order["leverage"] = leverage

    if price is not None and order_type == "limit_order":
        order["price"] = price

    if stop_price is not None:
        order["stop_price"] = stop_price

    if take_profit_price is not None:
        order["take_profit_price"] = take_profit_price

    if stop_loss_price is not None:
        order["stop_loss_price"] = stop_loss_price

    # time_in_force should NOT be sent for market orders
    if time_in_force is not None and order_type != "market_order":
        order["time_in_force"] = time_in_force

    payload = {
        "timestamp": timestamp,
        "order": order
    }

    json_payload = json.dumps(payload, separators=(",", ":"))

    signature = hmac.new(
        api_secret.encode("utf-8"),
        json_payload.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": api_key,
        "X-AUTH-SIGNATURE": signature
    }
    print(f"json_payload : {ts()}",json_payload)

    response = requests.post(url, headers=headers, data=json_payload, timeout=10)

    return response.status_code, response.json()



def get_live_dcx_funding(symbols=None):
    """
    symbols = None or ["all"]  → সব symbol
    symbols = ["BTCUSDT", "ETHUSDT"] → filtered
    """

    symbols = symbols or ["all"]

    # 1️⃣ DCX raw funding list
    funding_list = get_dcx_funding_rate()   # list of dicts

    # 2️⃣ Build map: {symbol: parsed_data}
    funding_map = {}

    for item in funding_list:
        sym = item.get("market") or item.get("symbol")
        if not sym:
            continue
        key = normalize_symbol(sym)   # 🔥 IMPORTANT
        funding_map[key] = parse_dcx_funding_data(item)

    # 3️⃣ Return all
    if symbols == ["all"]:
        return funding_map

    # 4️⃣ Return only requested symbols
    # return {
    #     s: funding_map[s]
    #     for s in symbols
    #     if s in funding_map
    # }


      # 3️⃣ Normalize requested symbols too
    result = {}
    for s in symbols:
        k = normalize_symbol(s)
        if k in funding_map:
            result[k] = funding_map[k]
            
    return result     



def get_all_futures_orders(api_key, api_secret):

    payload = {
        "timestamp": int(time.time() * 1000),
        "status": "open,filled,partially_filled,untriggered,partially_cancelled",
        "side": "buy,sell",
        "page": "1",
        "size": "100",
        "margin_currency_short_name": ["INR", "USDT"]
    }

    body, sig = _sign_payload(api_secret, payload)

    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": api_key,
        "X-AUTH-SIGNATURE": sig
    }

    resp = requests.post(
        f"{BASE_URL}/exchange/v1/derivatives/futures/orders",
        headers=headers,
        data=body
    )

    data = resp.json()
    return data if isinstance(data, list) else data.get("orders", [])




def get_orders_by_pair(api_key, api_secret, pair):
    orders = get_all_futures_orders(api_key, api_secret)
    return [o for o in orders if o.get("pair") == pair]



def smart_close_by_pair(api_key, api_secret, pair):
    """
    Close everything related to a pair:
    - Cancel all active orders
    - Exit active position (if any)
    """

    results = {
        "pair": pair,
        "cancelled_orders": [],
        "position_exit": None,
        "errors": []
    }

    ts = int(time.time() * 1000)

    # ---------------------------
    # 1️⃣ FETCH ALL ORDERS
    # ---------------------------
    payload = {
        "timestamp": ts,
        "status": "open,partially_filled,untriggered,filled",
        "side": "buy,sell",
        "page": "1",
        "size": "100",
        "margin_currency_short_name": ["INR", "USDT"]
    }

    body, sig = _sign_payload(api_secret, payload)

    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": api_key,
        "X-AUTH-SIGNATURE": sig
    }

    resp = requests.post(
        f"{BASE_URL}/exchange/v1/derivatives/futures/orders",
        headers=headers,
        data=body
    )

    orders_data = resp.json()

    if isinstance(orders_data, dict):
        orders = orders_data.get("orders", [])
    elif isinstance(orders_data, list):
        orders = orders_data
    else:
        orders = []

    pair_orders = [o for o in orders if o.get("pair") == pair]

    # ---------------------------
    # 2️⃣ CANCEL ACTIVE ORDERS
    # ---------------------------
    for o in pair_orders:
        status = o.get("status", "").lower()

        if status in ["initial", "open", "partially_filled", "untriggered"]:
            cancel_payload = {
                "timestamp": int(time.time() * 1000),
                "id": o["id"]
            }

            body, sig = _sign_payload(api_secret, cancel_payload)
            headers["X-AUTH-SIGNATURE"] = sig

            r = requests.post(
                f"{BASE_URL}/exchange/v1/derivatives/futures/orders/cancel",
                headers=headers,
                data=body
            )

            results["cancelled_orders"].append({
                "order_id": o["id"],
                "status": status,
                "response": r.json()
            })

    # ---------------------------
    # 3️⃣ CHECK & EXIT POSITION
    # ---------------------------
    pos_payload = {
        "timestamp": int(time.time() * 1000),
        "page": "1",
        "size": "20",
        "pairs": pair,
        "margin_currency_short_name": ["INR", "USDT"]
    }

    body, sig = _sign_payload(api_secret, pos_payload)
    headers["X-AUTH-SIGNATURE"] = sig

    pos_resp = requests.post(
        f"{BASE_URL}/exchange/v1/derivatives/futures/positions",
        headers=headers,
        data=body
    )

    positions = pos_resp.json()

    if isinstance(positions, list):
        active_pos = next(
            (p for p in positions if abs(p.get("active_pos", 0)) > 0),
            None
        )

        if active_pos:
            exit_payload = {
                "timestamp": int(time.time() * 1000),
                "id": active_pos["id"]
            }

            body, sig = _sign_payload(api_secret, exit_payload)
            headers["X-AUTH-SIGNATURE"] = sig

            exit_resp = requests.post(
                f"{BASE_URL}/exchange/v1/derivatives/futures/positions/exit",
                headers=headers,
                data=body
            )

            results["position_exit"] = exit_resp.json()

    return results




# Main execution
# if __name__ == "__main__":

#     get_balance_dcx()
#     get_futures_balance_dcx()




    # status, data = place_futures_order(
    #     api_key=API_KEY,
    #     api_secret=API_SECRET,
    #     side="buy",
    #     pair="B-ADA_USDT",
    #     order_type="limit_order",
    #     price=0.2,
    #     quantity=30,
    #     leverage=30,
    #     time_in_force="good_till_cancel"
    # )

    # print(status)
    # print(data)


    # result = smart_close_by_pair(
    #     api_key=API_KEY,
    #     api_secret=API_SECRET,
    #     pair="B-ADA_USDT"
    # )

    # print(json.dumps(result, indent=2))

    # data = normalize_pair("BTCUSD")
    # print("data :",data)

    


    # result = smart_close_order_or_position(
    # api_key=API_KEY,
    # api_secret=API_SECRET,
    # order_id="3a710805-78b7-432f-823f-8c3857a68b5f",
    # pair="B-ADA_USDT"
    # )

    # print(result)



    # data = get_orders_by_pair(
    #     api_key=API_KEY,
    #     api_secret=API_SECRET,
    #     pair="B-ADA_USDT"
    # )
    # print(data)




    # symbols = ['0GUSD', '1000000MOGUSD', '1000BONKUSD', '1000CATUSD', '1000CHEEMSUSD', '1000FLOKIUSD', '1000LUNCUSD', '1000PEPEUSD', '1000RATSUSD', '1000SATSUSD', '1000SHIBUSD', '1000WHYUSD', '1000XECUSD', '1000XUSD', '1INCHUSD', '1MBABYDOGEUSD', '2ZUSD', 'A2ZUSD', 'AAVEUSD', 'ACEUSD', 'ACHUSD', 'ACTUSD', 'ACXUSD', 'ADAUSD', 'AERGOUSD', 'AEROUSD', 'AEVOUSD', 'AGIXUSD', 'AGLDUSD', 'AI16ZUSD', 'AIUSD', 'AIXBTUSD', 'AKTUSD', 'ALCHUSD', 'ALGOUSD', 'ALICEUSD', 'ALLOUSD', 'ALPACAUSD', 'ALPHAUSD', 'ALPINEUSD', 'ALTUSD', 'AMBUSD', 'ANIMEUSD', 'ANKRUSD', 'APEUSD', 'API3USD', 'APTUSD', 'ARBUSD', 'ARCUSD', 'ARKMUSD', 'ARKUSD', 'ARPAUSD', 'ARUSD', 'ASRUSD', 'ASTERUSD', 'ASTRUSD', 'ATAUSD', 'ATHUSD', 'ATOMUSD', 'AUCTIONUSD', 'AUDIOUSD', 'AUSD', 'AVAAIUSD', 'AVAUSD', 'AVAXUSD', 'AVNTUSD', 'AWEUSD', 'AXLUSD', 'AXSUSD', 'B3USD', 'BABYUSD', 'BADGERUSD', 'BAKEUSD', 'BALUSD', 'BANANAS31USD', 'BANANAUSD', 'BANDUSD', 'BANUSD', 'BARDUSD', 'BATUSD', 'BBUSD', 'BCHUSD', 'BEAMXUSD', 'BELUSD', 'BERAUSD', 'BICOUSD', 'BIGTIMEUSD', 'BIOUSD', 'BLURUSD', 'BLZUSD', 'BMTUSD', 'BNBUSD', 'BNTUSD', 'BNXUSD', 'BOMEUSD', 'BONDUSD', 'BRETTUSD', 'BROCCOLI714USD', 'BSVUSD', 'BSWUSD', 'BTC-USD', 'BTC-USD-SWAP', 'BTCBUSD', 'BTCDOMUSD', 'BTCUSD', 'C98USD', 'CAKEUSD', 'CATIUSD', 'CCUSD', 'CELOUSD', 'CELRUSD', 'CETUSUSD', 'CFXUSD', 'CGPTUSD', 'CHESSUSD', 'CHILLGUYUSD', 'CHRUSD', 'CHZUSD', 'CKBUSD', 'CLANKERUSD', 'COMBOUSD', 'COMPUSD', 'COOKIEUSD', 'COSUSD', 'COTIUSD', 'COWUSD', 'CRVUSD', 'CTKUSD', 'CTSIUSD', 'CUSD', 'CVXUSD', 'CYBERUSD', 'DARUSD', 'DASHUSD', 'DEFIUSD', 'DEGENUSD', 'DEGOUSD', 'DENTUSD', 'DEXEUSD', 'DFUSD', 'DIAUSD', 'DODOXUSD', 'DOGEUSD', 'DOGSUSD', 'DOLOUSD', 'DOTUSD', 'DRIFTUSD', 'DUSD', 'DUSKUSD', 'DYDXUSD', 'DYMUSD', 'EDENUSD', 'EDUUSD', 'EGLDUSD', 'EIGENUSD', 'ENAUSD', 'ENJUSD', 'ENSOUSD', 'ENSUSD', 'EOSUSD', 'EPICUSD', 'ERAUSD', 'ETCUSD', 'ETH-USD', 'ETH-USD-SWAP', 'ETHBUSD', 'ETHFIUSD', 'ETHUSD', 'ETHWUSD', 'EULUSD', 'FARTCOINUSD', 'FETUSD', 'FFUSD', 'FIDAUSD', 'FILUSD', 'FIOUSD', 'FISUSD', 'FLMUSD', 'FLOWUSD', 'FLUXUSD', 'FORMUSD', 'FORTHUSD', 'FRONTUSD', 'FTMUSD', 'FUNUSD', 'FUSD', 'FXSUSD', 'GALAUSD', 'GALUSD', 'GASUSD', 'GHSTUSD', 'GIGGLEUSD', 'GLMRUSD', 'GLMUSD', 'GMTUSD', 'GMXUSD', 'GOATUSD', 'GPSUSD', 'GRASSUSD', 'GRIFFAINUSD', 'GRTUSD', 'GTCUSD', 'GUNUSD', 'GUSD', 'HAEDALUSD', 'HBARUSD', 'HEIUSD', 'HEMIUSD', 'HFTUSD', 'HIFIUSD', 'HIGHUSD', 'HIPPOUSD', 'HIVEUSD', 'HMSTRUSD', 'HNTUSD', 'HOLOUSD', 'HOMEUSD', 'HOOKUSD', 'HOTUSD', 'HUMAUSD', 'HYPERUSD', 'HYPEUSD', 'ICPUSD', 'ICXUSD', 'IDEXUSD', 'IDUSD', 'ILVUSD', 'IMXUSD', 'INITUSD', 'INJUSD', 'IOSTUSD', 'IOTAUSD', 'IOTXUSD', 'IOUSD', 'IPUSD', 'IRYSUSD', 'JASMYUSD', 'JOEUSD', 'JSTUSD', 'JTOUSD', 'JUPUSD', 'KAIAUSD', 'KAITOUSD', 'KASUSD', 'KAVAUSD', 'KDAUSD', 'KERNELUSD', 'KEYUSD', 'KITEUSD', 'KLAYUSD', 'KMNOUSD', 'KNCUSD', 'KOMAUSD', 'KSMUSD', 'LAUSD', 'LAYERUSD', 'LDOUSD', 'LEVERUSD', 'LINAUSD', 'LINEAUSD', 'LINKUSD', 'LISTAUSD', 'LOKAUSD', 'LOOMUSD', 'LPTUSD', 'LQTYUSD', 'LRCUSD', 'LSKUSD', 'LTCUSD', 'LUMIAUSD', 'LUNA2USD', 'MAGICUSD', 'MANAUSD', 'MANTAUSD', 'MASKUSD', 'MATICUSD', 'MAVIAUSD', 'MAVUSD', 'MBLUSD', 'MBOXUSD', 'MDTUSD', 'MELANIAUSD', 'MEMEUSD', 'METISUSD', 'MEUSD', 'MEWUSD', 'MINAUSD', 'MIRAUSD', 'MITOUSD', 'MKRUSD', 'MLNUSD', 'MMTUSD', 'MOB-USD', 'MOCAUSD', 'MONUSD', 'MOODENGUSD', 'MORPHOUSD', 'MOVEUSD', 'MOVRUSD', 'MTLUSD', 'MUBARAKUSD', 'MYROUSD', 'NEARUSD', 'NEIROETHUSD', 'NEIROUSD', 'NEOUSD', 'NEWTUSD', 'NFPUSD', 'NIGHTUSD', 'NILUSD', 'NKNUSD', 'NMRUSD', 'NOMUSD', 'NOTUSD', 'NTRNUSD', 'NULSUSD', 'NXPCUSD', 'OCEANUSD', 'OGNUSD', 'OGUSD', 'OMGUSD', 'OMNIUSD', 'OMUSD', 'ONDOUSD', 'ONEUSD', 'ONGUSD', 'ONTUSD', 'OPENUSD', 'OPUSD', 'ORBSUSD', 'ORCAUSD', 'ORDIUSD', 'OXTUSD', 'PARTIUSD', 'PAXGUSD', 'PENDLEUSD', 'PENGUUSD', 'PEOPLEUSD', 'PERPUSD', 'PHAUSD', 'PHBUSD', 'PIPPINUSD', 'PIXELUSD', 'PLUMEUSD', 'PNUTUSD', 'POLUSD', 'POLYXUSD', 'PONKEUSD', 'POPCATUSD', 'PORTALUSD', 'POWRUSD', 'PROMUSD', 'PROVEUSD', 'PUMPUSD', 'PUNDIXUSD', 'PYTHUSD', 'QNTUSD', 'QTUMUSD', 'QUICKUSD', 'RADUSD', 'RAREUSD', 'RAYSOLUSD', 'RDNTUSD', 'REDUSD', 'REEFUSD', 'REIUSD', 'RENDERUSD', 'RENUSD', 'RESOLVUSD', 'REZUSD', 'RIFUSD', 'RLCUSD', 'RNDRUSD', 'RONINUSD', 'ROSEUSD', 'RPLUSD', 'RSRUSD', 'RUNEUSD', 'RVNUSD', 'SAFEUSD', 'SAGAUSD', 'SAHARAUSD', 'SANDUSD', 'SANTOSUSD', 'SCRTUSD', 'SCRUSD', 'SEIUSD', 'SFPUSD', 'SHELLUSD', 'SIGNUSD', 'SKLUSD', 'SKYUSD', 'SLERFUSD', 'SLPUSD', 'SNTUSD', 'SNXUSD', 'SOLUSD', 'SOLVUSD', 'SOMIUSD', 'SONICUSD', 'SOPHUSD', 'SPELLUSD', 'SPKUSD', 'SPXUSD', 'SSVUSD', 'STEEMUSD', 'STGUSD', 'STMXUSD', 'STORJUSD', 'STOUSD', 'STPTUSD', 'STRAXUSD', 'STRKUSD', 'STXUSD', 'SUIUSD', 'SUNUSD', 'SUPERUSD', 'SUSD', 'SUSHIUSD', 'SWARMSUSD', 'SWELLUSD', 'SXPUSD', 'SXTUSD', 'SYNUSD', 'SYRUPUSD', 'SYSUSD', 'TAOUSD', 'THETAUSD', 'THEUSD', 'TIAUSD', 'TLMUSD', 'TNSRUSD', 'TOKENUSD', 'TOMOUSD', 'TONUSD', 'TOWNSUSD', 'TRBUSD', 'TREEUSD', 'TROYUSD', 'TRUMPUSD', 'TRUUSD', 'TRXUSD', 'TSTUSD', 'TURBOUSD', 'TURTLEUSD', 'TUSD', 'TUTUSD', 'TWTUSD', 'UMAUSD', 'UNFIUSD', 'UNIUSD', 'USDCUSD', 'USTCUSD', 'USUALUSD', 'UXLINKUSD', 'VANAUSD', 'VANRYUSD', 'VELODROMEUSD', 'VETUSD', 'VICUSD', 'VIDTUSD', 'VINEUSD', 'VIRTUALUSD', 'VOXELUSD', 'VTHOUSD', 'VVVUSD', 'WALUSD', 'WAVESUSD', 'WAXPUSD', 'WCTUSD', 'WETUSD', 'WIFUSD', 'WLDUSD', 'WLFIUSD', 'WOOUSD', 'WUSD', 'XAIUSD', 'XBTUSD', 'XCNUSD', 'XEMUSD', 'XLMUSD', 'XMRUSD', 'XPLUSD', 'XRPU19', 'XRPUSD', 'XTZUSD', 'XVGUSD', 'XVSUSD', 'YBUSD', 'YFIUSD', 'YGGUSD', 'ZBTUSD', 'ZECUSD', 'ZENUSD', 'ZEREBROUSD', 'ZETAUSD', 'ZILUSD', 'ZKCUSD', 'ZKUSD', 'ZROUSD', 'ZRXUSD']
    # symbols = [""]
    # print(get_futures_instrument_data_multi(symbols))

#     print("get_live_dcx_funding : ",get_current_funding_rate(f"{symbol}"))



    # print("Checking BTC/USDT Funding Details...")
    # funding_list = get_dcx_funding_rate()
    # parsed = [parse_dcx_funding_data(x) for x in funding_list]
    # print(parsed)

    # data = get_live_dcx_funding()
    # print(data)
    # dcx_all = get_live_dcx_funding(["DYDXUSDT"])
    # print(dcx_all)

    # print(get_live_dcx_funding(["SOPHUSDT"]))
    # print(get_current_funding_rate("SOPHUSDT"))






