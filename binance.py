import requests
from datetime import datetime, timezone


# def get_binance_funding(symbol="BTCUSDT"):
#     url = "https://fapi.binance.com/fapi/v1/premiumIndex"
#     params = {"symbol": symbol}

#     r = requests.get(url, params=params, timeout=10)
#     r.raise_for_status()
#     data = r.json()

#     # print(data)

#     return {
#         "symbol": data["symbol"],
#         "funding_rate": float(data["lastFundingRate"]),
#         "next_funding_time":data["nextFundingTime"],
#         "mark_price": float(data["markPrice"])
#     }


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




def get_binance_funding(symbol="BTCUSDT"):
    url = "https://fapi.binance.com/fapi/v1/premiumIndex"
    params = {"symbol": symbol}

    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    # ❗ Binance may return LIST if symbol invalid
    if isinstance(data, list):
        raise ValueError(f"Binance futures symbol not found: {symbol}")

    return {
        "symbol": data["symbol"],
        "funding_rate": float(data["lastFundingRate"]),
        "next_funding_time": data["nextFundingTime"],
        "mark_price": float(data["markPrice"])
    }



BINANCE_CACHE = {}

def get_binance_funding_safe(symbol):
    try:
        if symbol not in BINANCE_CACHE:
            BINANCE_CACHE[symbol] = get_binance_funding(symbol)
        return BINANCE_CACHE[symbol]
    except Exception as e:
        print(f"❌ Binance funding unavailable for {symbol} → {e}")
        return None

# def dcx_to_binance_symbol(dcx_symbol):
#     # B-BTC_USDT → BTCUSDT
#     return dcx_symbol.replace("B-", "").replace("_", "")

def dcx_to_binance_symbol(symbol: str):
    """
    Normalize CoinDCX / mixed symbols to Binance USDT-M futures symbol.

    Mapping rules:
    B-BTC_USDT -> BTCUSDT
    B-BTC_USD  -> BTCUSDT
    B-BTC_INR  -> None
    BTCUSDT    -> BTCUSDT
    BTCUSD     -> BTCUSDT
    """
    try:
        symbol = symbol.strip().upper()

        # Case 1: Already Binance-style USDT
        if symbol.endswith("USDT") and "_" not in symbol:
            return symbol

        # Case 2: Binance-style USD -> USDT
        if symbol.endswith("USD") and "_" not in symbol:
            base = symbol[:-3]
            return f"{base}USDT"

        # Case 3: CoinDCX futures format
        if symbol.startswith("B-"):
            base, quote = symbol.replace("B-", "").split("_")

            if quote in ("USDT", "USD"):
                return f"{base}USDT"

            # INR or others not supported
            return None

        return None

    except Exception:
        return None





def get_all_binance_futures_symbols():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()

    symbols = [
        s["symbol"]
        for s in data["symbols"]
        if s["contractType"] == "PERPETUAL"
        and s["quoteAsset"] == "USDT"
        and s["status"] == "TRADING"
    ]

    return symbols


def get_live_binance_funding(symbols=None):
    """
    symbols:
      None / ["all"]  → all Binance USDT-M perpetuals
      ["BTCUSDT","ETHUSDT"] → filtered
    """

    # 1️⃣ ALL symbols
    if not symbols or symbols == ["all"]:
        symbols = get_all_binance_futures_symbols()

    result = {}

    for sym in symbols:
        try:
            bn_sym = dcx_to_binance_symbol(sym)   # 🔥 FIX
            data = get_binance_funding(bn_sym)
            result[sym] = data
        except Exception as e:
            result[sym] = {
                "error": str(e)
            }

    return result




# if __name__ == "__main__":

#         sym = "SOPHUSDT"

#         binance_symbol = dcx_to_binance_symbol(sym)
#         print("sym :",binance_symbol)

# # #         # 📡 get binance funding data
#         bn_data = get_binance_funding_safe(binance_symbol)
#         print(bn_data)

        # data = get_all_binance_futures_symbols()
        # print(data)
