import math
# import queue
from flask import Flask, jsonify, render_template_string, render_template, request, abort
# from flask_cors import CORS
# import threading
# import time
# import random
# import json
# import os
from datetime import datetime, timedelta
# import mysql.connector
# import firebase_admin
# from firebase_admin import credentials, db
# import json
# import socket
# import requests
# import secrets
# import psutil
# import sys
# import random
# import string
# import logging
# import uuid
# from delta_rest_client import DeltaRestClient,OrderType,TimeInForce

# class CoinDCXClient:
#     def __init__(self, api_key, api_secret):
#         self.api_key = api_key
#         self.api_secret = api_secret.encode()
#         self.base_url = "https://api.coindcx.com"

#     def _headers(self, body):
#         payload = json.dumps(body, separators=(',', ':'))
#         signature = hmac.new(
#             self.api_secret,
#             payload.encode(),
#             hashlib.sha256
#         ).hexdigest()

#         return {
#             'X-AUTH-APIKEY': self.api_key,
#             'X-AUTH-SIGNATURE': signature,
#             'Content-Type': 'application/json'
#         }

#     def get_balances(self):
#         body = {
#             "timestamp": int(time.time() * 1000)
#         }
#         headers = self._headers(body)
#         return requests.post(
#             self.base_url + "/exchange/v1/users/balances",
#             json=body,
#             headers=headers
#         ).json()

# import os
# import sys
from dcx import get_dcx_funding_rate,dcx_map_builder,arbitrage_signal,countdown_from_ms,get_balance_dcx,get_current_funding_rate,get_futures_instrument_data,normalize_pair

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# from ex import get_api_data, get_api_dcx, get_live_funding_data, get_live_ticker

# from delta_rest_client import DeltaRestClient


# API_KEY = "pnvnILRxPPNJejTG0Bh2GXpzLLKJ5J"
# API_SECRET = "WeTk7hKMLANiTL4ZiW9Ya896McpeTomhZttQDhxw8YnhQCLG1FvBpFdMsA0G"
# # WeTk7hKMLANiTL4ZiW9Ya896McpeTomhZttQDhxw8YnhQCLG1FvBpFdMsA0G

# # API_KEY = "8HiEv5IWu67YNnO1iqUX2cIx3hCa7p"
# # API_SECRET = "O5dbzlC5IHgYEM1mfC0TUXnRSSokzzNhc5ucTcIYna7xNeNCyBaoHhw03GVB"

# BASE_URL = "https://cdn-ind.testnet.deltaex.org"
# WS_URL = "wss://socket-ind.testnet.deltaex.org"

def resolve_dcx_max_leverage(parsed):
    # 1️⃣ try direct fields
    # if parsed.get("max_leverage_long"):
    #     return int(parsed["max_leverage_long"])

    # if parsed.get("max_leverage_short"):
    #     return int(parsed["max_leverage_short"])

    # 2️⃣ fallback to dynamic leverage tiers
    dyn = parsed.get("dynamic_leverage", {})
    if dyn:
        return max(int(k) for k in dyn.keys())

    # 3️⃣ absolute fallback
    return 1



def build_arbitrage_payload(
    symbol: str,
    order_size: float,
    leverage_min: int,
    leverage_max: int,
    strategy: str,
    delta_balance_usd: float,
    dcx_balance_usd: float,
    delta_ticker: dict,
    dcx_instrument: dict
):
    """
    Returns executable arbitrage payload or rejection reason
    """

    # =========================
    # Normalize symbol
    # =========================
    normalized_symbol = normalize_pair(symbol)

    # =========================
    # Extract prices
    # =========================
    price = float(delta_ticker[symbol]["mark_price"])
    contract_value = float(delta_ticker[symbol].get("contract_value", 1))


    # =========================
    # DCX rules
    # =========================
    min_notional = float(dcx_instrument["parsed"]["min_notional"])
    qty_step = float(dcx_instrument["parsed"]["qty_step"])
    # dcx_max_leverage = int(dcx_instrument["parsed"]["max_leverage_long"])
    dcx_max_leverage = resolve_dcx_max_leverage(dcx_instrument["parsed"])


    print("dcx_max_leverage :",dcx_max_leverage)

    # =========================
    # Calculate minimum qty
    # =========================
    raw_qty = min_notional / price

    print("raw_qty :",raw_qty)

    min_qty = math.ceil(raw_qty / qty_step) * qty_step

    print("min_qty :",min_qty)

    # user requested size must be >= min qty
    qty = max(order_size, min_qty)

    # =========================
    # Leverage validation
    # =========================
    delta_allowed_leverage = int(delta_ticker[symbol].get("leverage", 1))

    print("delta_allowed_leverage :",delta_allowed_leverage)

    final_max_leverage = min(
        leverage_max,
        dcx_max_leverage,
        delta_allowed_leverage
    )

    if leverage_min > final_max_leverage:
        return {
            "success": False,
            "reason": "Leverage mismatch",
            "details": {
                "leverage_min": leverage_min,
                "allowed_max": final_max_leverage
            }
        }

    leverage = max(final_max_leverage, 1)

    # =========================
    # Margin calculation
    # =========================
    notional = qty * price
    required_margin = notional / leverage

    # =========================
    # Balance checks
    # =========================
    if delta_balance_usd < required_margin:
        return {
            "success": False,
            "reason": "Insufficient Delta balance",
            "required": required_margin,
            "available": delta_balance_usd
        }

    if dcx_balance_usd < required_margin:
        return {
            "success": False,
            "reason": "Insufficient DCX balance",
            "required": required_margin,
            "available": dcx_balance_usd
        }
    




    # =========================
    # Final Order (MARGIN BASED)
    # =========================

#     requested_margin = float(order_size)

#     # target notional from margin
#     target_notional = requested_margin * leverage

# # qty from target notional
#     raw_final_qty = target_notional / price

# # apply qty step
#     final_qty = math.floor(raw_final_qty / qty_step) * qty_step

# # enforce min qty
#     if final_qty < min_qty:
#         final_qty = min_qty

#     final_notional = final_qty * price
#     required_margin = final_notional / leverage



    # =========================
    # Final Order (MARGIN BASED + LOT AWARE)
    # =========================

    requested_margin = float(order_size)

    # target notional from margin
    target_notional = requested_margin * leverage

    raw_qty = target_notional / price

    # quantity must be multiple of contract_value
    final_qty = math.floor(raw_qty / contract_value) * contract_value

    # enforce min qty
    if final_qty < min_qty:
        final_qty = math.ceil(min_qty / contract_value) * contract_value

    final_notional = final_qty * price
    required_margin = final_notional / leverage



    # =========================
    # Final Arbitrage Payload
    # =========================
    payload = {
        "success": True,
        "strategy": strategy,
        "timestamp": datetime.utcnow().isoformat(),

        "symbol": {
            "delta": symbol,
            "dcx": normalized_symbol
        },

        "price_reference": price,

        "order": {
            "quantity": qty,
            "notional": round(notional, 4),
            "leverage": leverage
        },

        "final_order": {
            "contract_value": contract_value,
            "requested_order_size_usd": requested_margin,
            "target_notional_usd": round(target_notional, 4),
            "final_quantity": round(final_qty, 4),
            "final_notional": round(final_notional, 4),
            "required_margin_per_exchange": round(required_margin, 4)
        },

        # "final_order": {
        #     "requested_order_size_usd": requested_margin,
        #     "target_notional_usd": round(target_notional, 4),
        #     "contract_value": contract_value,
        #     "final_quantity_lots": round(final_qty, 4),
        #     "final_notional": round(final_notional, 4),
        #     "required_margin_per_exchange": round(required_margin, 4)
        # },


        "margin": {
            "required_per_exchange": round(required_margin, 4),
            "delta_balance": delta_balance_usd,
            "dcx_balance": dcx_balance_usd
        },

        "execution_plan": {
            "delta_side": "SHORT" if strategy.lower() == "arbitrage" else "LONG",
            "dcx_side": "LONG",
            "size_match": True
        }
    }

    return payload



# //////////////////////////////////////////////////




# ////////////////////////////////////////////////



# if __name__ == "__main__":

#     final_max_leverage = min(
#         30.00,
#         15.00,
#         20.00
#     )

#     print("final_max_leverage :",final_max_leverage)

#     data = 10 > final_max_leverage

#     ff =  leverage = max(final_max_leverage, 1)

#     print("ff :",ff)





#         symbol = ""
#         order_size = ""
#         leverage_min = ""
#         leverage_max = ""
#         strategy = ""
 

#         tickers_delta = manager.get_live_delta_ticker([f"{symbol}"])
#         dcx_future_instrument =  get_futures_instrument_data(f"{symbol}")

#         data = build_arbitrage_payload(str(symbol),
#         order_size=float(order_size),
#         leverage_min = int(leverage_min),
#         leverage_max = int(leverage_max),
#         strategy = str(strategy),
#         delta_balance_usd = float(delta_balance_usd),
#         dcx_balance_usd = float(dcx_balance_usd),
#         delta_ticker = dict(tickers_delta),
#         dcx_instrument = dict(dcx_future_instrument))


#         print("Arbitrage data = ",data)