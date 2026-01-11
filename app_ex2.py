from gevent import monkey
monkey.patch_all()
import gevent

import logging
from logging.handlers import TimedRotatingFileHandler




# import queue
from flask import Flask, jsonify, render_template_string, render_template, request, abort,make_response
from flask_cors import CORS
# import threading
import time

# from gevent import time as gtime
import random
import json
import os
from datetime import datetime, timedelta
# import mysql.connector
# import firebase_admin
from firebase_admin import credentials, db
import socket
import requests
import secrets
import psutil
# import sys
import string
# import logging
import uuid
from delta_rest_client import DeltaRestClient,OrderType,TimeInForce


# //////////////////////////////////

from collections import defaultdict
# from concurrent.futures import ThreadPoolExecutor

from collections import OrderedDict
from gevent.lock import Semaphore


from gevent.pool import Pool

from gevent.pywsgi import WSGIServer

import html
import re
# /////////////////////////////////////





# ================= LOGGING SETUP =================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, "error.log")

handler = TimedRotatingFileHandler(
    log_file,
    when="midnight",      # 🔹 daily log
    interval=1,
    backupCount=14,       # 🔹 last 14 days
    encoding="utf-8"
)

# formatter = logging.Formatter(
#     "%(asctime)s | %(levelname)s | %(process)d | %(threadName)s | %(message)s",
#     "%Y-%m-%d %H:%M:%S"
# )

formatter = logging.Formatter(
    "\n"  # 🔹 blank line before every error
    "%(asctime)s | %(levelname)s | %(process)d | %(threadName)s | %(message)s\n"
    "%(exc_text)s\n",
    "%Y-%m-%d %H:%M:%S"
)


handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.ERROR)
logger.addHandler(handler)

# console-এও দেখতে চাইলে
console = logging.StreamHandler()
console.setFormatter(formatter)
logger.addHandler(console)

# =================================================




# /////////////////////////////////////////////




# helper.py path add করা (data folder থেকে parent directory)
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# from ex import get_api_data, get_api_dcx, get_live_funding_data, get_live_ticker

from binance import get_binance_funding,dcx_to_binance_symbol,get_binance_funding_safe,get_live_binance_funding
from dcx import get_dcx_funding_rate,dcx_map_builder,arbitrage_signal,countdown_from_ms,get_balance_dcx,get_futures_balance_dcx,get_current_funding_rate,get_futures_instrument_data,get_futures_instrument_data_multi,get_live_dcx_funding,place_futures_order,normalize_pair,smart_close_by_pair
from ws import MultiUserManager, show_user_data, update_unrealized_pnl_for_user, data_store,data_lock
from arbitrage import build_arbitrage_payload



data_store.clear()
manager = MultiUserManager()
# manager.start_all()  # WebSocket start

# Firebase config
# cred = credentials.Certificate(
#     "air-share-be6be-firebase-adminsdk-pxekd-d21c09d722.json")  # তোমার key ফাইলের নাম
# firebase_admin.initialize_app(cred, {
#     'databaseURL': 'https://air-share-be6be-default-rtdb.firebaseio.com/'  # তোমার ডেটাবেস URL
# })

# Example: Database reference
strategies_ref = db.reference("strategies")
# Firebase থেকে সব strategies একবারে লোড
firebase_data = strategies_ref.get()


app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Allow all origins for dev








# ----------------------------
# In-Memory Storage
# ----------------------------
users = {}  # {username: {"password": "...", "id": int}}
# strategies = {}  # {user_id: {"running": False, "last_signal": "NONE", "pnl": 0.0, "positions": [], "strategy_params": {}}}
user_counter = 1
strategy_threads = {}



# Firebase থেকে strategies node থেকে সব data fetch
strategies = db.reference("strategies").get() or {}  # None হলে খালি dict


# ----------------------------
# Run Server
# ----------------------------

# for user_id in strategies.keys():
#     firebase_data = load_strategies_from_firebase(user_id)
#     if firebase_data:
#         print(f"✅ Firebase data found for user {user_id}: {firebase_data}")
#         strategies[user_id] = firebase_data
#     else:
#         print(
#             f"ℹ️ No Firebase data for user {user_id}, keeping existing: {strategies[user_id]}")



# if firebase_data:
#     # print(f"✅ Firebase raw data: {firebase_data}")

#     for idx, user_strategy in enumerate(firebase_data):
#         if user_strategy is None:
#             continue

#         user_id = str(idx)

#         # Flask / Python উদাহরণ
#         strategies = db.reference(f"strategies/{user_id}").get()


#         # Ensure all runtime keys exist
#         strategies[user_id] = {
#             "running": user_strategy.get("running", False),
#             "last_signal": user_strategy.get("last_signal", "NONE"),
#             "pnl": user_strategy.get("pnl", 0.0),
#             "positions": user_strategy.get("positions", []),
#             "strategy_params": user_strategy.get("strategy_params", [])
#         }
#         print(f"✅ Loaded strategy for user {user_id}: {strategies[user_id]}")
# else:
#     print("ℹ️ No strategies found in Firebase")


# ----------------------------
# JSON Persistence
# ----------------------------
# STRATEGY_FILE = "strategies.json"


# @app.route("/ping", methods=["GET"])
# def ping():
#     return jsonify({"status": "alive"}), 200

# @app.route("/ping", methods=["GET"])
# def ping():
#     return "ok", 200

# @app.route("/ping")
# def ping():
#     return jsonify({"status": "alive"}), 200


@app.route("/ping")
def ping():
    return "ok", 200






def format_log_html(text: str) -> str:
    text = html.escape(text)

    # timestamp
    text = re.sub(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
        r'<span class="ts">\1</span>',
        text
    )

    # ERROR level
    text = re.sub(
        r"\| ERROR \|",
        r'<span class="level-error">| ERROR |</span>',
        text
    )

    # Traceback
    text = re.sub(
        r"(Traceback \(most recent call last\):)",
        r'<span class="traceback">\1</span>',
        text
    )

    # Python file paths
    text = re.sub(
        r'(File "&quot;.*?\.py&quot;, line \d+)',
        r'<span class="file">\1</span>',
        text
    )

    # Exception names
    text = re.sub(
        r'(ZeroDivisionError|KeyError|ValueError|TypeError|Exception)',
        r'<span class="exception">\1</span>',
        text
    )

    return text








@app.route("/save_api_delta", methods=["POST"])
def save_api_delta():
    try:
        data = request.get_json()

        if not data:
            return jsonify({"success": False, "msg": "No JSON data received"})
        
        user_id = data.get("user_id")
        print("user_id",user_id)
        delta_data = data.get("delta")
        

        if not user_id or not delta_data:
            return jsonify({"success": False, "msg": "Missing user_id or delta data"})
        
        api_key = delta_data.get("api_key")
        api_secret = delta_data.get("api_secret")

        if not api_key or not api_secret:
            return jsonify({"success": False, "msg": "Missing API key or secret"})

        # 🔥 ADD THIS
        cache_key = f"{api_key}:{api_secret}"
        manager.api_type_cache.pop(cache_key, None)    
        
         # 🔍 CHECK API TYPE
        # ✅ USE GLOBAL MANAGER (NO NEW INSTANCE)
        # api_type = manager.check_api_type(api_key, api_secret, user_id)
        api_type = manager.check_api_type(api_key, api_secret, user_id, force=True)


        if api_type == "INVALID_KEY":
            return jsonify({
                "success": False,
                "msg": "Invalid API key or secret"
            })
        
         # ✅ SAVE TO FIREBASE
        # ref = db.reference(f"strategies/{user_id}/database/delta")
        # ref.update({
        #     "api_key": api_key,
        #     "api_secret": api_secret,
        #     "type": api_type
        # })


        
         # Database reference: strategies/<user_id>/database
        ref_path = f"strategies/{user_id}/database/delta"
        ref = db.reference(ref_path)

        # Check if user already exists
        existing_data = ref.get()

        save_data = {
            "api_key": api_key,
            "api_secret": api_secret,
            "type": api_type
        }

        if existing_data:
            # Update existing record
            ref.update(save_data)
            print(f"✅ Updated API data for user {user_id}")
            #  # 🔁 AUTO WS RESTART
            # restarted = manager.restart_user_ws(user_id)
            # return jsonify({
            #     "success": True, 
            #     "msg": "API data updated successfully",
            #     "account_type": api_type,
            #     "ws_restarted": restarted
            #     })
        else:
            # Create new entry
            ref.set(save_data)
            print(f"✅ Saved new API data for user {user_id}")

        gevent.sleep(0.3)  # 300 ms delay
        # ✅ CONFIRM DATABASE WRITE
        confirmed_data = ref.get()
        print("Confirmed Data:", confirmed_data.get("api_key"))

        if not confirmed_data or confirmed_data.get("api_key") != api_key:
            return jsonify({
            "success": False,
            "msg": "Database update not confirmed. Try again."
            })


        # 🔁 AUTO WS RESTART
        restarted = manager.restart_user_ws(user_id)
        if restarted:
            return jsonify({
                "success": True, 
                "msg": "API data saved successfully",
                "account_type": api_type,
                "ws_restarted": restarted
                })
         


    except Exception as e:
        print("❌ Error saving Delta API:", e)
        return jsonify({"success": False, "msg": str(e)})



@app.route("/save_api_dcx", methods=["POST"])
def save_api_dcx():
    try:
        data = request.get_json()

        if not data:
            return jsonify({"success": False, "msg": "No JSON data received"})
        
        user_id = data.get("user_id")
        print("user_id",user_id)
        delta_data = data.get("delta")
        

        if not user_id or not delta_data:
            return jsonify({"success": False, "msg": "Missing user_id or delta data"})
        
        api_key = delta_data.get("api_key")
        api_secret = delta_data.get("api_secret")

        if not api_key or not api_secret:
            return jsonify({"success": False, "msg": "Missing API key or secret"})
        
         # 🔍 CHECK API TYPE
        # ✅ USE GLOBAL MANAGER (NO NEW INSTANCE)
        # api_type = manager.check_api_type_dcx(api_key, api_secret, user_id)

        # if api_type == "INVALID_KEY":
        #     return jsonify({
        #         "success": False,
        #         "msg": "Invalid API key or secret"
        #     })
        
         # ✅ SAVE TO FIREBASE
        # ref = db.reference(f"strategies/{user_id}/database/delta")
        # ref.update({
        #     "api_key": api_key,
        #     "api_secret": api_secret,
        #     "type": api_type
        # })


        
         # Database reference: strategies/<user_id>/database
        ref_path = f"strategies/{user_id}/database/dcx"
        ref = db.reference(ref_path)

        # Check if user already exists
        existing_data = ref.get()

        save_data = {
            "api_key": api_key,
            "api_secret": api_secret,
            # "type": api_type
        }

        if existing_data:
            # Update existing record
            ref.update(save_data)
            print(f"✅ Updated API data for user {user_id}")
        else:
            # Create new entry
            ref.set(save_data)
            print(f"✅ Saved new API data for user {user_id}")

        gevent.sleep(0.3)  # 300 ms delay
        # ✅ CONFIRM DATABASE WRITE
        confirmed_data = ref.get()
        print("Confirmed Data:", confirmed_data.get("api_key"))

        if not confirmed_data or confirmed_data.get("api_key") != api_key:
            return jsonify({
            "success": False,
            "msg": "Database update not confirmed. Try again."
            })


        # 🔁 AUTO WS RESTART
        # restarted = manager.restart_user_ws(user_id)
        # if restarted:
        return jsonify({
                "success": True, 
                "msg": "API data saved successfully"
                })
         


    except Exception as e:
        print("❌ Error saving DCX API:", e)
        return jsonify({"success": False, "msg": str(e)})


# ----------------------------
# DB helper functions
# ----------------------------


@app.route("/get_user/<user_id>", methods=["GET"])
def get_user(user_id):
    try:
        user_ref = strategies_ref.child(user_id).get()
        if not user_ref:
            return jsonify({"error": "User not found"}), 404
        return jsonify(user_ref)
    except Exception as e:
        print(f"❌ Firebase fetch error: {e}")
        return jsonify({"error": "Failed to fetch user"}), 500





# exchanges_data = {
#         "delta": {
#             "api_key": "delta_api_123",
#             "secret_key": "delta_secret_123"
#         },
#         "binance": {
#             "api_key": "binance_api_456",
#             "secret_key": "binance_secret_456"
#         }
#     }


# def save_strategies_to_firebase(user_id, data):
#     """User এর পুরো strategy Firebase এ সেভ করো"""
#     # print(strategies[user_id])
#     # print("data",data)
#        # user database info strategies dict থেকে নাও
#     user_db = strategies[user_id]["database"]
#     try:
#         user_id = str(user_id)
#         full_state = {
#             "database": {
#                 "name": user_db.get("name", ""),
#                 "phone": user_db.get("phone", ""),
#                 "token": user_db.get("token", ""),
#                 "email": user_db.get("email", ""),
#                 "username": user_db.get("username", ""),
#                 "password": user_db.get("password", ""),
#                 "exchanges": []
#             },
#             "running": strategies[user_id]["running"],
#             "pnl": strategies[user_id]["pnl"],
#             "last_signal": strategies[user_id]["last_signal"],
#             "executed_signals": strategies[user_id].get("executed_signals", {}),
#             "positions": strategies[user_id]["positions"],
#             "strategy_params": data
#         }
#         strategies_ref.child(user_id).set(full_state)
#         print(f"✅ Strategy saved for {user_id}")
#     except Exception as e:
#         print(f"❌ Firebase Save Error: {e}")
#         raise


def format_custom_datetime(updated_at):
    if not updated_at:
        return None

    # Convert ISO timestamp to datetime (UTC)
    dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))

    # Convert to Bangladesh time (UTC+6)
    bd_time = dt + timedelta(hours=6)

    # Format → 08-Dec-2025 09:38:02 AM
    return bd_time.strftime("%d-%b-%Y %I:%M:%S %p")



def save_strategies_to_firebase(user_id, full_state):
    try:
        strategies_ref.child(user_id).set(full_state)
        print(f"✅ Strategy saved for {user_id}")
    except Exception as e:
        # print(f"❌ Firebase Save Error: {e}")
        logger.error("Firebase Save Error")
        raise



def load_strategies_from_firebase(user_id):
    """User এর strategy Firebase থেকে লোড করো"""
    try:
        data = strategies_ref.child(user_id).get()
        if data:
            return data
        else:
            return {}
    except Exception as e:
        print(f"❌ Firebase Load Error: {e}")
        return {}


# for user_id in strategies.keys():
#     strategies[user_id] = load_strategies_from_firebase(
#         user_id) or strategies[user_id]


# ----------------------------
# Simulated Coin Details
# ----------------------------


def fetch_coin_details(symbol):
    return {
        "price": random.uniform(25000, 27000) if "BTC" in symbol else random.uniform(1500, 2000),
        "funding_rate": random.uniform(-0.2, 0.2),
        "next_funding_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 3600))
    }

# ----------------------------
# Strategy Runner
# ----------------------------


def safe_float(val, default=0.0):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# def strategy_runner(user_id):
#     print(f"🚀 Strategy runner started for user {user_id}")

#     # executed_signals dict to track which adv condition has been executed
#     executed_signals = {}

#     while strategies[user_id]["running"]:
#         params_list = strategies[user_id].get("strategy_params", [])

#         for leg in params_list:
#             leg_id = leg.get("legId")
#             contact = leg.get("contact", "Unknown")
#             qty = safe_float(leg.get("qty"), 0)
#             price = safe_float(leg.get("price"), 0)
#             tp = safe_float(leg.get("tp"), 0)
#             sl = safe_float(leg.get("sl"), 0)
#             advanced_conditions = leg.get("advanced", [])

#             for idx, adv in enumerate(advanced_conditions):
#                 op = adv.get("op")
#                 action_time = adv.get("aETime")
#                 signal = adv.get("opS2")  # BUY / EXIT

#                 # unique key for each leg+adv condition
#                 key = f"{leg_id}_{idx}_{action_time}_{signal}"

#                 # ✅ Only execute once
#                 if executed_signals.get(key):
#                     continue

#                 if op == "on time":
#                     current_time = datetime.now().strftime("%H:%M")
#                     if current_time == action_time:
#                         executed_signals[key] = True  # mark as executed
#                         # profit = round(tp - position["entry_price"], 2)
#                         print("pnl1")
#                         if signal == "BUY":
#                             position = {
#                                 "contact": contact,
#                                 "side": "BUY",
#                                 "qty": qty,
#                                 "entry_price": price,
#                                 "timestamp": datetime.now().isoformat()
#                             }
#                             strategies[user_id]["positions"].append(position)
#                             profit = round(tp - position["entry_price"], 2)
#                             print("pnl", profit)
#                             strategies[user_id]["pnl"] += profit
#                             strategies[user_id]["last_signal"] = "BUY"
#                             print(f"🟢 [{current_time}] {contact} BUY executed")

#                         elif signal == "EXIT":
#                             if strategies[user_id]["positions"]:
#                                 position = {
#                                     "contact": contact,
#                                     "side": "EXIT",
#                                     "qty": qty,
#                                     "entry_price": price,
#                                     "timestamp": datetime.now().isoformat()
#                                 }
#                                 strategies[user_id]["positions"].append(
#                                     position)

#                                 # entry = strategies[user_id]["positions"].pop(0)
#                                 # profit = round(tp - sl, 2)
#                                 # real profit calculation
#                                 # profit = round(tp - entry["entry_price"], 2)
#                                 # print("pnl", profit)
#                                 strategies[user_id]["pnl"] += profit
#                                 strategies[user_id]["last_signal"] = "EXIT"
#                                 print(
#                                     f"🔴 [{current_time}] {contact} EXIT executed, Profit={profit}")
#          # ✅ Save to JSON file
#         # save_strategies_background()
#         gevent.sleep(2)

#     print(f"🛑 Strategy runner stopped for user {user_id}")




def should_trigger_on_time(action_time: str, key: str, executed_signals: dict, window=2):
    """
    action_time: "HH:MM:SS"
    key: unique execution key
    executed_signals: runtime dict
    window: allowed seconds drift
    """

    print("KEY :", key)

    if executed_signals.get(key):
        return False

    now = datetime.now()
    current_sec = now.hour * 3600 + now.minute * 60 + now.second

    h, m, s = map(int, action_time.split(":"))
    action_sec = h * 3600 + m * 60 + s

    if abs(current_sec - action_sec) <= window:
        # executed_signals[key] = True
        return True

    return False





def should_trigger_on_time_strict(action_time, key, executed_signals, window=1.0):
    if executed_signals.get(key):
        return False

    now = datetime.now()

    h, m, s = map(int, action_time.split(":"))
    action_dt = now.replace(hour=h, minute=m, second=s, microsecond=0)

    diff = (now - action_dt).total_seconds()

    # ONLY allow after action_time, within window
    return 0 <= diff <= window



# def should_prepare_trade(action_time, prepared, window=3):
#     """
#     Trigger PREPARE phase before execution
#     """
#     if prepared:
#         return False

#     now = datetime.now()
#     h, m, s = map(int, action_time.split(":"))
#     action_dt = now.replace(hour=h, minute=m, second=s, microsecond=0)

#     diff = (action_dt - now).total_seconds()

#     # trigger prepare BEFORE execution
#     return 0 < diff <= window


# def should_prepare_trade(action_time, prepared, min_before=2, max_before=4):
#     """
#     Prepare ONLY between T-4s and T-2s
#     Never within last 1 second
#     """
#     if prepared:
#         return False

#     now = datetime.now()
#     h, m, s = map(int, action_time.split(":"))
#     action_dt = now.replace(hour=h, minute=m, second=s, microsecond=0)

#     diff = (action_dt - now).total_seconds()

#     return max_before >= diff >= min_before



def should_prepare_trade(action_time, prepared, min_before=1):
    """
    Prepare trade any time BEFORE (action_time - min_before)
    Never within last `min_before` seconds
    """
    if prepared:
        return False

    now = datetime.now()
    h, m, s = map(int, action_time.split(":"))
    action_dt = now.replace(hour=h, minute=m, second=s, microsecond=0)

    diff = (action_dt - now).total_seconds()

    # 🔴 block only last `min_before` seconds
    return diff > min_before







def prepare_trade(user_id, leg):
    # broker = leg.get("Brocker")
    broker = leg.get("brocker")
    contact = leg.get("contact")

    if broker == "Delta":
        leverage = int(leg.get("leverage", 1))
        product_id = leg.get("productId")

        print(
            f"🛠️ PREPARE Delta leverage {leverage}x for {contact}",
            datetime.now().strftime("%H:%M:%S.%f")
        )

        res = manager.set_laverage(
            user_id=str(user_id),
            product_id=str(product_id),
            leverage=leverage
        )

        if not res or not res.get("success"):
            # print("❌ Leverage set API failed")
            logger.error("Leverage set API failed")
            return False

        # 🔍 VERIFY
        actual = int(res.get("leverage", 0))
        if actual != leverage:
            print(f"❌ Leverage mismatch {actual} != {leverage}")
            return False

        print("✅ Leverage confirmed")
        return True





def execute_signal(user_id, item):

    # print("prepare_key :",prepare_key)

    leg = item["leg"]
    signal = item["signal"]
    broker = item["brocker"]

    contact = leg.get("contact")
    qty = safe_float(leg.get("qty"), 0)
    side = leg.get("buySell", "").lower()
    leverage = int(leg.get("leverage", 1))
    lprice = safe_float(leg.get("lPrice"), 0)
    order_type_ui = leg.get("type", "").lower()

    bestBid = leg.get("bestBid",0) 
    product_id = leg.get("productId")
    # productId

    print(
        f"▶️ START {contact} {signal} {broker}",
        datetime.now().strftime("%H:%M:%S.%f")
    )

    # ================= BUY =================
    if signal == "BUY":

        if broker == "Dcx":
            client = manager.clients[user_id]["dcx"]
            pair = normalize_pair(contact)

            order_type = "limit_order" if order_type_ui == "limit" else "market_order"

            status, data = place_futures_order(
                api_key=client.api_key,
                api_secret=client.api_secret,
                side=side,
                pair=pair,
                order_type=order_type,
                price=lprice if order_type == "limit_order" else "",
                quantity=qty,
                leverage=leverage,
                time_in_force="good_till_cancel"
            )

            return {
                "brocker": "Dcx",
                "signal": "BUY",
                "status": status,
                "data": data,
                "leg": leg
            }

        if broker == "Delta":
            
            print("product_id :",product_id)

            # if prepare_key not in prepared_trades:
            #     prepared_trades[prepare_key] = False  # reserve first

            #     set_laverage = manager.set_laverage(
            #         user_id=str(user_id),
            #         product_id=str(product_id),
            #         leverage=int(leverage)
            #     )

            #     prepared_trades[prepare_key] = set_laverage.get("success", False)
            #     print("Set Leverage Responce :",set_laverage["success"] or set_laverage["error"])

            # if not prepared_trades or prepared_trades.get(prepare_key) is False:

            #     set_laverage = manager.set_laverage(user_id=str(user_id), product_id=str(product_id), leverage=int(leverage))
            #     print("Set Leverage Responce :",set_laverage["success"])
            #     if not set_laverage["success"]:
            #         print("Leverage set failed:", set_laverage["error"])
            #     prepared_trades[prepare_key] = set_laverage.get("success", False)    
                # continue  # Skip further processing if leverage setting failed
            best_bid = None                                # price,
            if bestBid:
                    
                best_bid,product_id = manager.get_data_ticker(user_id=str(user_id), symbol=str(contact))
                print("best_bid :",best_bid," product_id :",product_id)

            client_order_id = str(int(time.time())) + uuid.uuid4().hex[:8]

            uiii = { 
                    "product_id": str(product_id),
                    "size": float(qty),
                    "side": side.lower(),
                    "client_order_id": client_order_id,
                    "time_in_force": TimeInForce.GTC,
                }
            if order_type_ui.lower() == "limit":
                    uiii["order_type"] = OrderType.LIMIT
                    limit_px = best_bid if bestBid else lprice
                    uiii["limit_price"] = float(limit_px)

                    # akhane max limit prise ar upore gale instant best bit order ececute korbe problem...

            elif order_type_ui.lower() == "market":
                uiii["order_type"] = OrderType.MARKET
                # MUST remove limit_price for market
                uiii.pop("limit_price", None)  

            # print("uiii :",uiii)                 

            res = manager.place_order(user_id=user_id, data=uiii)

            return {
                "brocker": "Delta",
                "signal": "BUY",
                "status": res.get("success"),
                "data": res,
                "leg": leg
            }

    # ================= EXIT =================
    elif signal == "EXIT":

        if broker == "Dcx":
            client = manager.clients[user_id]["dcx"]
            pair = normalize_pair(contact)

            res = smart_close_by_pair(
                api_key=client.api_key,
                api_secret=client.api_secret,
                pair=pair
            )

            return {
                "brocker": "Dcx",
                "signal": "EXIT",
                "data": res,
                "leg": leg
            }

        if broker == "Delta":
            res = manager.smart_close(
                user_id=user_id,
                symbol=contact
            )

            return {
                "brocker": "Delta",
                "signal": "EXIT",
                "data": res,
                "leg": leg
            }

    return {
        "error": "UNKNOWN_CASE",
        "leg": leg
    }


def fb_get(ref):
    gevent.sleep(0.001)   # minimum safe
    return ref.get()






def strategy_runner(user_id):
    print(f"🚀 Strategy runner started for user {user_id}")

    # executed_signals = {}
    executed_signals = OrderedDict()

    positions = []
    pnl = 0.0
    last_signal = "NONE"

    # prepared_trades = set()   # keys which are prepared
    prepared_trades = {}   # dict: prepare_key -> True / Fals

    prepare_lock = Semaphore()

    while True:
        # prune old
        MAX_EXECUTED = 2000
        while len(executed_signals) > MAX_EXECUTED:
            executed_signals.popitem(last=False)

        if len(prepared_trades) > 1000:
            prepared_trades.clear()
    

        try:
            running = fb_get(db.reference(f"strategies/{user_id}/running"))
            if not running:
                print(f"🛑 Strategy stopped for user {user_id}")
                break

            # strategy_data = db.reference(
            #     f"strategies/{user_id}/strategy_params"
            # ).get() or []

            strategy_data = fb_get(
                db.reference(f"strategies/{user_id}/strategy_params")
            ) or []


            # 🔹 collect same-time signals
            time_bucket = defaultdict(list)


            for leg in strategy_data:
                print("leg :",leg)
                print("LEG DEBUG:", leg.get("legId"), leg.get("brocker"), leg.get("contact"))  # 👈 ADD HERE

                leg_id = leg.get("legId")
                contact = leg.get("contact")
                advanced_conditions = leg.get("advanced", [])
                broker = leg.get("brocker", "Delta")

                for idx, adv in enumerate(advanced_conditions):
                    if adv.get("op") != "on time":
                        continue

                    action_time = adv.get("aETime")
                    signal = adv.get("opS2")

                    key = f"{user_id}_{leg_id}_{idx}_{action_time}_{signal}_{broker}"

                    # inside leg loop
                    prepare_key = f"{user_id}_{leg_id}_{action_time}_{broker}"

                    # 🔹 PREPARE PHASE
                    # if should_prepare_trade(action_time, prepare_key in prepared_trades):
                    #     ok = prepare_trade(user_id, leg)
                    #     # prepared_trades.add(prepare_key)
                    #     prepared_trades[prepare_key] = ok

                    if should_prepare_trade(action_time, prepare_key in prepared_trades):
                        with prepare_lock:
                            if prepare_key not in prepared_trades:
                                prepared_trades[prepare_key] = prepare_trade(user_id, leg)
    

                    if should_trigger_on_time_strict(action_time, key, executed_signals):

                        # 🔴 EXECUTION GATE (HERE)
                        if prepared_trades.get(prepare_key) is not True:
                            print(
                                f"⛔ SKIP EXECUTE | prepare failed | {prepare_key}"
                            )
                            continue   # ❗ execute_signal ডাকাই হবে না

                        time_bucket[action_time].append({
                            "key": key,
                            "leg": leg,
                            "signal": signal,
                            "brocker": broker
                        })

                        print("🪣 BUCKET ADD:",
                            action_time,
                            leg.get("contact"),
                            signal,
                            broker,
                            datetime.now().strftime("%H:%M:%S")
                        )

                        # print("🧺 TIME BUCKET SNAPSHOT:")
                        # for t, items in time_bucket.items():
                        #     print(t, "=>", [(i["leg"]["contact"], i["signal"]) for i in items])


            # 🔥 batch execute per same time
            for action_time, items in time_bucket.items():
                if not items:
                    continue

                print(
                    f"🚀 SAME TIME EXECUTION @ {action_time} | count={len(items)}"
                )

                # mark executed
                for item in items:
                    # executed_signals[item["key"]] = True
                    executed_signals[item["key"]] = time.time()


                    # /////////////////////////////////

                # # parallel execution
                # with ThreadPoolExecutor(max_workers=len(items)) as ex:
                #     futures = [
                #         ex.submit(execute_signal, user_id, item, prepared_trades, prepare_key)
                #         for item in items
                #     ]

                #     results = [f.result() for f in futures]

                # ===============================
                # gevent parallel execution
                # ===============================
                pool = Pool(len(items))

                jobs = [
                    pool.spawn(
                        execute_signal,
                        user_id,
                        item
                        # prepared_trades,
                        # f"{user_id}_{item['leg'].get('legId')}_{action_time}_{item['brocker']}"
                    )
                    for item in items
                ]

                # wait for all greenlets
                gevent.joinall(jobs)

                # collect results
                results = [job.value for job in jobs]



                    # ///////////////////////////////////

                # ✅ MAIN THREAD: update positions / pnl
                for res in results:
                    leg = res.get("leg")
                    broker = res.get("brocker")
                    signal = res.get("signal")
                    data = res.get("data")



                    # updated_at = data.get("updated_at")
                    # print("updated_at :",updated_at)

                    print("data :",data)

                    if signal == "BUY":

                        if data.get("success"):
                            order = data.get("order", {})
                            updated_at = order.get("updated_at")
                            print("updated_at :",updated_at)

                            positions.append({
                                "legId": leg.get("legId"),
                                "brocker": broker,
                                "contact": leg.get("contact"),
                                "side": "BUY",
                                "qty": leg.get("qty"),
                                "order_id": order.get("id"),
                                "state": order.get("state"),
                                "timestamp": order.get("updated_at") or datetime.now().isoformat()
                            })
                        last_signal = "BUY"

                    elif signal == "EXIT":


                        order = data if broker == "Dcx" else data.get("order", {})
                        updated_at = order.get("updated_at")
                        print("updated_at :",updated_at)

                        positions.append({
                            "legId": leg.get("legId"),
                            "brocker": broker,
                            "contact": leg.get("contact"),
                            "side": "EXIT",
                            "timestamp": order.get("updated_at") or datetime.now().isoformat()
                        })
                        last_signal = "EXIT"

                db.reference(f"strategies/{user_id}").update({
                    "pnl": pnl,
                    "last_signal": last_signal,
                    "positions": positions
                })

            # gevent.sleep(0.2)
            gevent.sleep(0.3)   # minimum safe on Windows

            time_bucket.clear()



        except Exception as e:
            # print(f"❌ Error in strategy_runner {user_id}:", e)
            logger.exception("Error in strategy_runner for user %s", user_id)
            gevent.sleep(5)

    print(f"🛑 Strategy runner fully stopped for user {user_id}")





# def strategy_runner(user_id):
#     print(f"🚀 Strategy runner started for user {user_id}")

#     # executed_signals = {}  # track executed signals
#     # Reset executed signals on new start
#     strategies[user_id]["executed_signals"] = {}

#     # Ensure positions list exists
#     if "positions" not in strategies[user_id]:
#         strategies[user_id]["positions"] = []
#         print(strategies[user_id])

#     while strategies[user_id]["running"]:
#         legs = strategies[user_id].get("strategy_params", [])

#         for leg in legs:
#             leg_id = leg.get("legId")
#             contact = leg.get("contact", "Unknown")
#             qty = safe_float(leg.get("qty"), 0)
#             price = safe_float(leg.get("price"), 0)
#             tp = safe_float(leg.get("tp"), 0)
#             sl = safe_float(leg.get("sl"), 0)
#             advanced_conditions = leg.get("advanced", [])

#             for idx, adv in enumerate(advanced_conditions):
#                 op = adv.get("op")
#                 action_time = adv.get("aETime")
#                 signal = adv.get("opS2")  # BUY / EXIT

#                 key = f"{leg_id}_{idx}_{action_time}_{signal}"
#                 # print(
#                 #     f"Checking leg {leg_id}, adv {idx}, op={adv.get('op')}, time={adv.get('aETime')}, signal={adv.get('opS2')}")

#                 # if executed_signals.get(key):
#                 #     continue  # already executed

#                 if strategies[user_id]["executed_signals"].get(key):
#                     continue  # Already executed

#                 if op == "on time":
#                     current_time = datetime.now().strftime("%H:%M")
#                     # print(current_time)
#                     # print(action_time)
#                     if current_time == action_time:
#                         # executed_signals[key] = True

#                         # Mark as executed
#                         strategies[user_id]["executed_signals"][key] = True

#                         if signal == "BUY":
#                             position = {
#                                 "legId": leg_id,
#                                 "contact": contact,
#                                 "side": "BUY",
#                                 "qty": qty,
#                                 "entry_price": price,
#                                 "timestamp": datetime.now().isoformat()
#                             }
#                             print(strategies[user_id]["positions"])

#                             strategies[user_id]["positions"].append(position)
#                             strategies[user_id]["last_signal"] = "BUY"
#                             print(
#                                 f"✅ [{current_time}] {contact} → {signal} executed")

#                         elif signal == "EXIT":
#                             # Find last BUY position for this leg
#                             buy_positions = [p for p in strategies[user_id]["positions"]
#                                              if p["side"] == "BUY" and p["legId"] == leg_id]
#                             if buy_positions:
#                                 last_buy = buy_positions[-1]
#                                 profit = round(tp - last_buy["entry_price"], 2)
#                                 strategies[user_id]["pnl"] += profit
#                                 strategies[user_id]["last_signal"] = "EXIT"

#                                 # Add EXIT position log
#                                 exit_position = {
#                                     "legId": leg_id,
#                                     "contact": contact,
#                                     "side": "EXIT",
#                                     "qty": qty,
#                                     "entry_price": price,
#                                     "timestamp": datetime.now().isoformat()
#                                 }
#                                 strategies[user_id]["positions"].append(
#                                     exit_position)
#                                 print(
#                                     f"🔴 [{current_time}] {contact} EXIT executed, Profit={profit}")

#                         # ✅ Save runtime to Firebase
#                         save_strategies_to_firebase(
#                             user_id, strategies[user_id]["strategy_params"])

#         # small sleep for next check
#         time.sleep(1)

#     print(f"🛑 Strategy runner stopped for user {user_id}")


# @app.route("/remove_leg", methods=["POST"])
# def remove_leg():
#     data = request.json
#     user_id = data.get("user_id")
#     leg_id = data.get("leg_id")

#     if not user_id or not leg_id:
#         return jsonify({"success": False, "msg": "Missing user_id or leg_id"}), 400

#     if user_id not in strategies:
#         return jsonify({"success": False, "msg": "User not found"}), 404

#     legs = strategies[user_id].get("strategy_params", [])
#     new_legs = [leg for leg in legs if leg.get("legId") != leg_id]
#     strategies[user_id]["strategy_params"] = new_legs
#     strategies[user_id]["running"] = False
#     strategies[user_id]["last_signal"] = "NONE"
#     strategies[user_id]["pnl"] = 0.0
#     #     "running": False,
#     #     "last_signal": "NONE",
#     #     "pnl": 0.0,

#     # ✅ Save to JSON file

#     # save_strategies_background()

#     save_strategies_to_firebase(
#         user_id, strategies[user_id]["strategy_params"])

#     return jsonify({"success": True, "msg": f"Leg {leg_id} removed"})








@app.route("/remove_leg", methods=["POST"])
def remove_leg():
    try:
        data = request.json
        user_id = str(data.get("user_id"))
        id = data.get("id")
        name = data.get("name")

        if not user_id or not id:
            return jsonify({"success": False, "msg": "Missing user_id or id"}), 400

        user_strategy_ref = db.reference(f"strategies/{user_id}")
        user_strategy = fb_get(db.reference(f"strategies/{user_id}"))
        # user_strategy = user_strategy_ref.get()

        if not user_strategy:
            return jsonify({"success": False, "msg": "User not found"}), 404

        legs = user_strategy.get("strategy_params", [])
        new_legs = [leg for leg in legs if leg.get("id") != id]

        updates = {
            "strategy_params": new_legs
        }

        # 🔴 যদি আর কোনো leg না থাকে → strategy stop
        if len(new_legs) == 0:
            updates.update({
                "running": False,
                "last_signal": "NONE",
                "pnl": 0.0,
                "positions": []
            })

            # stop thread
            if user_id in strategy_threads:
                # thread = strategy_threads[user_id]
                # if thread.is_alive():
                #     thread.join(timeout=1)
                # del strategy_threads[user_id]

                # g = strategy_threads[user_id]
                # g.kill(block=False)
                # del strategy_threads[user_id]

                # g = strategy_threads.get(user_id)
                # if g and not g.dead:
                #     g.kill(block=False)
                # if g and not g.dead:
                #     g.kill()
                user_strategy_ref.update({"running": False})
                strategy_threads.pop(user_id, None)
                gevent.sleep(0.001)   # minimum safe



        user_strategy_ref.update(updates)

        return jsonify({
            "success": True,
            "msg": f"Leg {name} removed"
        })

    except Exception as e:
        return jsonify({"success": False, "msg": str(e)}), 500














# @app.route("/remove_leg", methods=["POST"])
# def remove_leg():
#     data = request.json
#     user_id = str(data.get("user_id"))   # ✅ convert to string
#     leg_id = data.get("leg_id")

#     if not user_id or not leg_id:
#         return jsonify({"success": False, "msg": "Missing user_id or leg_id"}), 400

#     if user_id not in strategies:
#         return jsonify({"success": False, "msg": "User not found"}), 404

#     # Stop strategy safely
#     if strategies[user_id]["running"]:
#         strategies[user_id]["running"] = False
#         if user_id in strategy_threads:
#             strategy_threads[user_id].join(timeout=1)
#             del strategy_threads[user_id]

#     # Remove leg
#     legs = strategies[user_id].get("strategy_params", [])
#     new_legs = [leg for leg in legs if leg.get("legId") != leg_id]
#     strategies[user_id]["strategy_params"] = new_legs

#     # Reset runtime data
#     strategies[user_id]["last_signal"] = "NONE"
#     strategies[user_id]["pnl"] = 0.0
#     strategies[user_id]["positions"] = []

#     # Save updated strategy to Firebase
#     save_strategies_to_firebase(
#         user_id, strategies[user_id]["strategy_params"])

#     return jsonify({"success": True, "msg": f"Leg {leg_id} removed and strategy stopped"})


# ----------------------------
# Routes
# ----------------------------


# ----------------------------
# Routes (register/login/create/save/remove/status/control)
# ----------------------------
# @app.route("/register", methods=["POST"])
# def register():
#     global user_counter
#     data = request.json
#     username = data.get("username")
#     password = data.get("password")
#     if not username or not password:
#         return jsonify({"success": False, "msg": "Missing username or password"}), 400
#     if username in users:
#         return jsonify({"success": False, "msg": "User already exists"}), 400
#     users[username] = {"password": password, "id": user_counter}
#     strategies[user_counter] = {
#         "running": False,
#         "last_signal": "NONE",
#         "pnl": 0.0,
#         "positions": [],
#         "strategy_params": []
#     }
#     # Persist empty params row to DB
#     save_strategies_to_firebase(
#         user_counter, strategies[user_counter]["strategy_params"])
#     user_counter += 1
#     return jsonify({"success": True, "msg": "User registered", "user_id": users[username]["id"]})


def is_user_in_firebase(username):
    try:
        ref = db.reference("strategies")
        all_users = ref.get()  # list বা dict আসতে পারে
        if all_users:
            if isinstance(all_users, dict):
                # dict হলে আগের মতো
                for uid, user_data in all_users.items():
                    if "database" in user_data and user_data["database"].get("username") == username:
                        return True
            elif isinstance(all_users, list):
                # list হলে index দিয়ে
                for user_data in all_users:
                    if user_data and "database" in user_data and user_data["database"].get("username") == username:
                        return True
        return False
    except Exception as e:
        print("Firebase check error:", e)
        return False
    

def is_email_in_firebase(email):
    try:
        ref = db.reference("strategies")
        all_users = ref.get()  # list বা dict আসতে পারে
        if all_users:
            if isinstance(all_users, dict):
                # dict হলে আগের মতো
                for uid, user_data in all_users.items():
                    if "database" in user_data and user_data["email"].get("email") == email:
                        return True
            elif isinstance(all_users, list):
                # list হলে index দিয়ে
                for user_data in all_users:
                    if user_data and "database" in user_data and user_data["database"].get("email") == email:
                        return True
        return False
    except Exception as e:
        print("Firebase check error:", e)
        return False    

@app.route("/register", methods=["POST"])
def register():
    global user_counter
    data = request.json
    username = data.get("username")
    password = data.get("password")
    email = ""
    name = ""
    phone = ""
    # token = ""
    token = ''.join(random.choices(string.ascii_lowercase + string.digits, k=32))
    if data.get("name"):
        name = data.get("name")
    if data.get("phone"):
        phone = data.get("phone")
    if data.get("token"):
        token = data.get("token")
    if data.get("email"):
        email = data.get("email")

    print("email",email);    

    if not username or not password:
        return jsonify({"success": False, "msg": "Missing username or password"}), 400
    if is_user_in_firebase(username):
        return jsonify({"success": False, "msg": "User already exists"}), 400
    if is_email_in_firebase(email):
        return jsonify({"success": False, "msg": "email already exists"}), 400
    
    # Firebase push() generates unique ID automatically
    # new_user_ref = db.reference("strategies").push()
    # user_id = new_user_ref.key


    user_id = str(user_counter)  # স্ট্রিং-এ কনভার্ট
    users[username] = {"password": password, "id": user_id}
    # strategies[user_id] = {
    full_state = {
        "database": {
            "name": name,
            "phone": phone,
            "email": email,
            "token": token,
            "username": username,
            "password": password,
            "exchanges": []
        },
        "running": False,
        "last_signal": "NONE",
        "pnl": 0.0,
        "executed_signals":[],
        "positions": [],
        "strategy_params": []
    }
    save_strategies_to_firebase(
        user_id, full_state)
    user_counter += 1
    return jsonify({"success": True, "msg": "User registered", "user_id": user_id})


@app.route("/login", methods=["POST"])
def login():
    data = request.json
    username = data.get("username")
    password = data.get("password")
    if username in users and users[username]["password"] == password:
        return jsonify({"success": True, "msg": "Login success", "user_id": users[username]["id"]})
    return jsonify({"success": False, "msg": "Invalid credentials"}), 401






# @app.route("/create_strategy", methods=["POST"])
# def create_strategy():
#     data = request.json
#     user_id = str(data.get("user_id"))
#     trade_type = data.get("trade_type")
#     symbol = data.get("symbol")
#     sma_short = data.get("sma_short")
#     sma_long = data.get("sma_long")
#     funding_settings = data.get("funding_settings")

#     # সরাসরি Firebase থেকে fetch
#     user_strategy_ref = db.reference(f"strategies/{user_id}")
#     user_strategy = user_strategy_ref.get()
#     if not user_strategy:
#         return jsonify({"success": False, "msg": "User not found"}), 404

#     if not trade_type or not symbol or not sma_short or not sma_long:
#         return jsonify({"success": False, "msg": "Missing required fields"}), 400

#     # Firebase-এ সরাসরি save
#     strategy_params = {
#         "trade_type": trade_type,
#         "symbol": symbol,
#         "sma_short": sma_short,
#         "sma_long": sma_long,
#         "funding_settings": funding_settings
#     }
#     user_strategy["strategy_params"] = strategy_params

#     # Update Firebase
#     user_strategy_ref.set(user_strategy)

#     return jsonify({"success": True, "msg": "Strategy created"})


# @app.route("/create_strategy", methods=["POST"])
# def create_strategy():
#     data = request.json
#     user_id = str(data.get("user_id"))
#     trade_type = data.get("trade_type")
#     symbol = data.get("symbol")
#     sma_short = data.get("sma_short")
#     sma_long = data.get("sma_long")
#     funding_settings = data.get("funding_settings")
#     if user_id not in strategies:
#         return jsonify({"success": False, "msg": "User not found"}), 404
#     if not trade_type or not symbol or not sma_short or not sma_long:
#         return jsonify({"success": False, "msg": "Missing required fields"}), 400
#     # store as in-memory params (and persist)
#     strategies[user_id]["strategy_params"] = {
#         "trade_type": trade_type,
#         "symbol": symbol,
#         "sma_short": sma_short,
#         "sma_long": sma_long,
#         "funding_settings": funding_settings
#     }
#     save_strategies_to_firebase(
#         user_id, strategies[user_id]["strategy_params"])
#     return jsonify({"success": True, "msg": "Strategy created"})


# @app.route("/save_strategy_params", methods=["POST"])
# def save_strategy_params():
#     """Save or update leg for a single user"""
#     data = request.json
#     user_id = data.get("user_id")
#     leg_data = data.get("strategy_params")  # single leg JSON

#     if not user_id or not leg_data or "legId" not in leg_data:
#         return jsonify({"success": False, "msg": "Missing user_id or legId in strategy_params"}), 400

#     if user_id not in strategies:
#         strategies[user_id] = {
#             "running": False,
#             "last_signal": "NONE",
#             "pnl": 0.0,
#             "positions": [],
#             "strategy_params": []
#         }

#     if "strategy_params" not in strategies[user_id]:
#         strategies[user_id]["strategy_params"] = []

#     # Check if legId exists
#     existing_legs = strategies[user_id]["strategy_params"]
#     for i, leg in enumerate(existing_legs):
#         if leg.get("legId") == leg_data["legId"]:
#             # Update existing leg
#             strategies[user_id]["strategy_params"][i] = leg_data
#             save_strategies_to_firebase(
#                 user_id, strategies[user_id]["strategy_params"])
#             return jsonify({"success": True, "msg": f"Leg {leg_data['legId']} updated successfully"})

#     # If legId not found, append new leg
#     strategies[user_id]["strategy_params"].append(leg_data)
#     save_strategies_to_firebase(
#         user_id, strategies[user_id]["strategy_params"])
#     return jsonify({"success": True, "msg": f"Leg {leg_data['legId']} added successfully"})






@app.route("/save_strategy_params", methods=["POST"])
def save_strategy_params():
    try:
        data = request.json
        user_id = str(data.get("user_id"))
        leg_data = data.get("strategy_params")

        if not user_id or not leg_data or "legId" not in leg_data:
            return jsonify({
                "success": False,
                "msg": "Missing user_id or legId"
            }), 400

        user_ref = strategies_ref.child(user_id)

        # 🔥 শুধু array টা আনো
        existing_legs = user_ref.child("strategy_params").get() or []

        print("existing_legs : ",existing_legs)

        updated = False
        # for i, leg in enumerate(existing_legs):
        #     if leg.get("legId") == leg_data["legId"]:
        #         existing_legs[i] = leg_data
        #         updated = True
        #         break

        # if not updated:
        existing_legs.append(leg_data)

        # 🔥 ONLY update this key
        user_ref.update({
            "strategy_params": existing_legs
        })

        return jsonify({
            "success": True,
            "msg": f"Leg {leg_data['legId']} {'updated' if updated else 'added'}"
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "msg": str(e)
        }), 500






# @app.route("/save_strategy_params", methods=["POST"])
# def save_strategy_params():
#     """Save or update a single user's leg directly in Firebase"""
#     try:
#         data = request.json
#         user_id = str(data.get("user_id"))  # স্ট্রিং-এ কনভার্ট
#         leg_data = data.get("strategy_params")

#         if not user_id or not leg_data or "legId" not in leg_data:
#             return jsonify({"success": False, "msg": "Missing user_id or legId in strategy_params"}), 400

#         # Firebase থেকে user strategy নেওয়া
#         user_ref = strategies_ref.child(user_id)
#         user_strategy = user_ref.get()

#         if not user_strategy:
#             # যদি user না থাকে, নতুন structure তৈরি করি
#             user_strategy = {
#                 "running": False,
#                 "last_signal": "NONE",
#                 "pnl": 0.0,
#                 "positions": [],
#                 "strategy_params": []
#             }

#         # existing legs check & update
#         existing_legs = user_strategy.get("strategy_params", [])
#         updated = False
#         for i, leg in enumerate(existing_legs):
#             if leg.get("legId") == leg_data["legId"]:
#                 existing_legs[i] = leg_data
#                 updated = True
#                 break

#         if not updated:
#             existing_legs.append(leg_data)

#         # Firebase-এ save
#         user_strategy["strategy_params"] = existing_legs
#         user_ref.set(user_strategy)

#         msg = f"Leg {leg_data['legId']} {'updated' if updated else 'added'} successfully"
#         return jsonify({"success": True, "msg": msg})

#     except Exception as e:
#         return jsonify({"success": False, "msg": f"Error: {str(e)}"}), 500








# @app.route("/save_strategy_params", methods=["POST"])
# def save_strategy_params():
#     """Save or update leg for a single user"""
#     try:
#         data = request.json
#         user_id = str(data.get("user_id"))  # স্ট্রিং-এ কনভার্ট
#         leg_data = data.get("strategy_params")

#         print("user_id :",user_id)

#         if not user_id or not leg_data or "legId" not in leg_data:
#             return jsonify({"success": False, "msg": "Missing user_id or legId in strategy_params"}), 400


#         if user_id not in strategies:
#             strategies[user_id] = {
#                 "running": False,
#                 "last_signal": "NONE",
#                 "pnl": 0.0,
#                 "positions": [],
#                 "strategy_params": []
#             }

#         existing_legs = strategies[user_id]["strategy_params"]
#         for i, leg in enumerate(existing_legs):
#             if leg.get("legId") == leg_data["legId"]:
#                 strategies[user_id]["strategy_params"][i] = leg_data
#                 save_strategies_to_firebase(
#                     user_id, strategies[user_id]["strategy_params"])
#                 return jsonify({"success": True, "msg": f"Leg {leg_data['legId']} updated successfully"})

#         strategies[user_id]["strategy_params"].append(leg_data)
#         save_strategies_to_firebase(
#             user_id, strategies[user_id]["strategy_params"])
#         return jsonify({"success": True, "msg": f"Leg {leg_data['legId']} added successfully"})
#     except Exception as e:
#         return jsonify({"success": False, "msg": f"Error: {str(e)}"}), 500

# বাকি রুটগুলো অপরিবর্তিত থাকবে


# @app.route("/get_user_full_status", methods=["GET"])
# def get_user_full_status():
#     """
#     Get full strategy status for a user including:
#     - running
#     - last_signal
#     - pnl
#     - positions
#     - strategy_params (all legs)
#     Usage: GET /get_user_full_status?user_id=1
#     """
#     user_id = request.args.get("user_id")

#     if not user_id or not user_id.isdigit():
#         return jsonify({"success": False, "msg": "Invalid or missing user_id"}), 400

#     user_id = int(user_id)

#     if user_id not in strategies:
#         return jsonify({"success": False, "msg": "User not found"}), 404

#     # Full status including runtime and legs
#     full_status = {
#         "running": strategies[user_id]["running"],
#         "last_signal": strategies[user_id]["last_signal"],
#         "pnl": strategies[user_id]["pnl"],
#         "positions": strategies[user_id]["positions"],
#         "strategy_params": strategies[user_id].get("strategy_params", [])
#     }

#     return jsonify({"success": True, "user_id": user_id, "data": full_status})


@app.route("/get_user_full_status", methods=["GET"])
def get_user_full_status():
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify({"success": False, "msg": "Invalid or missing user_id"}), 400

    user_id = str(user_id)  # স্ট্রিং হিসেবে রাখো

    # সরাসরি Firebase থেকে fetch করো
    user_strategy = fb_get(db.reference(f"strategies/{user_id}"))
    if not user_strategy:
        return jsonify({"success": False, "msg": "User not found"}), 404

    full_status = {
        "running": user_strategy.get("running", False),
        "last_signal": user_strategy.get("last_signal", "NONE"),
        "pnl": user_strategy.get("pnl", 0.0),
        "positions": user_strategy.get("positions") or [],  # ✅ null হলে empty array
        "strategy_params": user_strategy.get("strategy_params") or []
    }

    return jsonify({"success": True, "user_id": user_id, "data": full_status})



@app.route("/get_coin_details", methods=["GET"])
def get_coin_details():
    symbol = request.args.get("symbol")
    if not symbol:
        return jsonify({"success": False, "msg": "Symbol required"}), 400
    details = fetch_coin_details(symbol)
    return jsonify({"success": True, "data": details})


# @app.route("/control", methods=["POST"])
# def control():
#     data = request.json
#     user_id = data.get("user_id")
#     action = data.get("action")

#     if user_id not in strategies:
#         return jsonify({"success": False, "msg": "User not found"}), 404

#     if not strategies[user_id]["strategy_params"]:
#         return jsonify({"success": False, "msg": "Create a strategy first"}), 400

#     if action == "start" and not strategies[user_id]["running"]:
#         strategies[user_id]["running"] = True
#         t = threading.Thread(target=strategy_runner, args=(user_id,))
#         t.daemon = True
#         t.start()
#         strategy_threads[user_id] = t
#         return jsonify({"success": True, "msg": "Strategy started"})

#     elif action == "stop":
#         strategies[user_id]["running"] = False
#         if user_id in strategy_threads:
#             strategy_threads[user_id].join(timeout=1)
#             del strategy_threads[user_id]
#         return jsonify({"success": True, "msg": "Strategy stopped"})

#     return jsonify({"success": False, "msg": "Invalid action"}), 400


@app.route("/deltafunding")
def delta_funding():
    user_id = request.args.get("user_id")

    if not user_id:
        return jsonify({
            "success": False,
            "msg": "Missing user_id"
        }), 400

    user_id = str(user_id)
    data = manager.get_live_delta_funding(user_id=user_id)
    return jsonify(data)


@app.route("/ticker_arbitrage")
def tickers_delta():
    user_id = request.args.get("user_id")

    if not user_id:
        return jsonify({
            "success": False,
            "msg": "Missing user_id"
        }), 400

    user_id = str(user_id)
    return manager.get_live_delta_ticker(user_id=user_id)




# @app.route("/funding_arbitrage")
# def funding_arbitrage():
#     return jsonify(manager.funding_arbitrage)

@app.route("/funding_arbitrage")
def funding_api():
    # data = request.json
    # user_id = str(data.get("user_id"))

    user_id = request.args.get("user_id")

    if not user_id:
        return jsonify({
            "success": False,
            "msg": "Missing user_id"
        }), 400

    user_id = str(user_id)

    dcx_data = get_dcx_funding_rate()
    # ex_data = get_live_funding_data()
    ex_data = manager.get_live_delta_funding(user_id=user_id)

    dcx_map = dcx_map_builder(dcx_data)
    result = []

    for sym, ex in ex_data.items():
        if sym not in dcx_map:
            continue

        dcx = dcx_map[sym]

      

        # ///////////////////////////
        # 🔁 convert symbol
        binance_symbol = dcx_to_binance_symbol(sym)
        # print("sym :",binance_symbol)

        # 📡 get binance funding data
        bn_data = get_binance_funding_safe(binance_symbol)
        # print(bn_data)
        if not bn_data:
            continue
        # //////////////////////////////


        signal, diff = arbitrage_signal(
            ex["funding_rate"],
            dcx["funding"]
        )

        result.append({
            "symbol": sym,
            "ex_funding": f"{ex['funding_rate']:.6f}",
            "dcx_funding": f"{dcx['funding']:.6f}",
            "funding_diff": round(diff, 6),
            "signal": signal,
            "dcx_price": dcx["mark_price"],

            # ✅ Binance exact funding time (used for both)
            "binance_funding_time": 
                bn_data["next_funding_time"]
                ,
            "dcx_funding_time": 
                countdown_from_ms(bn_data["next_funding_time"]),



             # ✅ HERE IS YOUR ANSWER
            # "binance_funding_time": countdown_from_ms(
            #     bn_data["next_funding_time"]
            # ),
            # "dcx_funding_time": countdown_from_ms(dcx["funding_time_ms"]),
            # "dcx_funding_time": manager.format_time_remaining(dcx["funding_time_ms"]),
            "ex_funding_time": ex["next_funding_in"]
        })

    return jsonify(result)




@app.route("/start_initialize", methods=["POST"])
def start_initialize():
    data = request.json

    id = data.get("id")

    symbol = data.get("symbol")
    order_size = data.get("order_size")
    leverage_min = data.get("leverage_min")
    leverage_max = data.get("leverage_max")
    strategy = data.get("strategy")
    delta_balance_usd = data.get("delta_balance_usd")
    # dcx_balance_usd = data.get("dcx_balance_usd")
    dcx_balance_usd = "7.00"


    print("🚀 Initialize Request Received")
    print("Symbol:", symbol)
    print("Order Size:", order_size)
    print("Leverage:", leverage_min)
    print("Strategy:", strategy)

    funding_delta = manager.get_live_delta_funding(id,[f"{symbol}"])
    tickers_delta = manager.get_live_delta_ticker(id,[f"{symbol}"])
    dcx_future_instrument =  get_futures_instrument_data(f"{symbol}")

    # print("get_live_delta_funding : ",funding_delta)
    # print("\n\nget_live_delta_ticker : ",tickers_delta)
    # print("\n\nDelta balance :",delta_balance_usd)

    # # next delta data check ..............

    # print("\n\nget_live_dcx_ticker : ",get_futures_instrument_data(f"{symbol}"))
    # print("get_live_dcx_funding : ",get_current_funding_rate(f"{symbol}"))
    # print("\n\nDCX balance :",dcx_balance_usd)


    # next Calculation lavarage, lot size , murgins orothers 
    data = build_arbitrage_payload(str(symbol),
    order_size=float(order_size),
    leverage_min = int(leverage_min),
    leverage_max = int(leverage_max),
    strategy = str(strategy),
    delta_balance_usd = float(delta_balance_usd),
    dcx_balance_usd = float(dcx_balance_usd),
    delta_ticker = dict(tickers_delta),
    dcx_instrument = dict(dcx_future_instrument))


    print("Arbitrage data = ",data)




    # 👉 Here you can:
    # - validate balance
    # - check leverage limits
    # - start arbitrage engine thread
    # - store config in DB / memory



    return jsonify({
        "success": True,
        "message": f"Strategy initialized for {symbol}"
    })





@app.route("/control", methods=["POST"])
def control():
    data = request.json
    user_id = str(data.get("user_id"))
    action = data.get("action")

    # সরাসরি Firebase থেকে user strategy load
    user_ref = db.reference(f"strategies/{user_id}")
    user_strategy = fb_get(db.reference(f"strategies/{user_id}"))
    # user_strategy = user_ref.get()
    if not user_strategy:
        return jsonify({"success": False, "msg": "User not found"}), 404

    if not user_strategy.get("strategy_params"):
        return jsonify({"success": False, "msg": "Create a strategy first"}), 400

     # ▶️ START
    # START strategy
    if action == "start" and not user_strategy.get("running", False):
        # Reset executed signals
        user_strategy["executed_signals"] = {}
        user_strategy["running"] = True

        # Update Firebase
        user_ref.update({"running": True, "executed_signals": {}})


        # ////////////////////////////////////////////////

        # # Start thread
        # t = threading.Thread(target=strategy_runner, args=(user_id,))
        # t.daemon = True
        # t.start()
        # strategy_threads[user_id] = t



        g = gevent.spawn(strategy_runner, user_id)
        strategy_threads[user_id] = g


        # //////////////////////////////////////////

        return jsonify({"success": True, "msg": "Strategy started"})

    # ⏹ STOP
    # STOP strategy
    elif action == "stop" and user_strategy.get("running", False):
        user_strategy["running"] = False
        user_ref.update({"running": False})

        # Stop thread if exists
        if user_id in strategy_threads:

            # ///////////////////////////////
            # thread = strategy_threads[user_id]
            # if thread.is_alive():
            #     thread.join(timeout=1)
            # del strategy_threads[user_id]

            # g = strategy_threads[user_id]
            # g.kill(block=False)
            # del strategy_threads[user_id]

            g = strategy_threads.get(user_id)
            # if g and not g.dead:
            #     g.kill(block=False)
            # only signal stop
            user_ref.update({"running": False})

            # DO NOT kill
            strategy_threads.pop(user_id, None)

        
            # ///////////////////////////////

        return jsonify({"success": True, "msg": "Strategy stopped"})

    return jsonify({"success": False, "msg": "Invalid action"}), 400


@app.route("/status", methods=["GET"])
def status():
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify({"success": False, "msg": "Missing user_id"}), 400
    user_id = str(user_id)

    # Direct Firebase fetch
    user_strategy = fb_get(db.reference(f"strategies/{user_id}"))
    if not user_strategy:
        return jsonify({"success": False, "msg": "User not found"}), 404

    return jsonify({"success": True, "data": user_strategy})


# @app.route("/control", methods=["POST"])
# def control():
#     data = request.json
#     user_id = str(data.get("user_id"))  # ✅ ensure user_id is string
#     action = data.get("action")

#     if user_id not in strategies:
#         return jsonify({"success": False, "msg": "User not found"}), 404

#     if not strategies[user_id]["strategy_params"]:
#         return jsonify({"success": False, "msg": "Create a strategy first"}), 400

#     # START strategy
#     if action == "start" and not strategies[user_id]["running"]:
#      # ////////////////////////////////////////////////////////////////////
#         # Stop existing thread first
#         if strategies[user_id]["running"]:
#             strategies[user_id]["running"] = False
#             if user_id in strategy_threads:
#                 thread = strategy_threads[user_id]
#                 if thread.is_alive():
#                     thread.join(timeout=1)
#                 del strategy_threads[user_id]

#         # Reset executed signals
#         strategies[user_id]["executed_signals"] = {}
#         # ////////////////////////////////////////////////////////////////////

#         strategies[user_id]["running"] = True
#         # Firebase update
#         strategies_ref.child(user_id).update({"running": True})

#         t = threading.Thread(target=strategy_runner, args=(user_id,))
#         t.daemon = True
#         t.start()
#         strategy_threads[user_id] = t
#         return jsonify({"success": True, "msg": "Strategy started"})

#     # STOP strategy
#     elif action == "stop" and strategies[user_id]["running"]:
#         strategies[user_id]["running"] = False
#         # Firebase update
#         strategies_ref.child(user_id).update({"running": False})

#         if user_id in strategy_threads:
#             thread = strategy_threads[user_id]
#             if thread.is_alive():         # ✅ Check before join
#                 thread.join(timeout=1)
#             del strategy_threads[user_id]

#         return jsonify({"success": True, "msg": "Strategy stopped"})

#     return jsonify({"success": False, "msg": "Invalid action"}), 400


# @app.route("/status", methods=["GET"])
# def status():
#     user_id = request.args.get("user_id")
#     if not user_id or user_id not in strategies:
#         return jsonify({"success": False, "msg": "User not found or invalid user_id"}), 404
#     return jsonify({"success": True, "data": strategies[user_id]})


@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({"success": False, "msg": "Method Not Allowed"}), 405





# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000, debug=True)


# Or restrict to your GitHub Pages domain
# CORS(app, resources={r"/data": {"origins": "https://contact474747-ui.github.io"}})

# -----------------------------
# Monitor data
monitor_data = {
    "counter": 0,
    "last_update": time.strftime("%H:%M:%S"),
    "public_ip": "",
    "status": "",
    "connection": ""
}

# -----------------------------
# Generate strong random token
# SECRET_TOKEN = secrets.token_hex(16)
# print("Your access token:", SECRET_TOKEN)


# TOKEN_FILE = "token.txt"
# if os.path.exists(TOKEN_FILE):
#     with open(TOKEN_FILE, "r") as f:
#         SECRET_TOKEN = f.read().strip()
# else:
#     SECRET_TOKEN = secrets.token_hex(16)
#     with open(TOKEN_FILE, "w") as f:
#         f.write(SECRET_TOKEN)
# SECRET_TOKEN = os.getenv("APP_TOKEN")
SECRET_TOKEN =  "3d2a1b4e1bc806605237d663b4e76a0e"

print("Your access token:", SECRET_TOKEN)


# -----------------------------
# Function to get public IP


def get_public_ip():
    try:
        gevent.sleep(0.001)   # minimum safe
        return requests.get("https://api.ipify.org", timeout=3).text
    except:
        return "Unavailable"



def check_connection(host="8.8.8.8", port=53, timeout=3):
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except socket.error:
        return False


def is_script_running(script_name):
    """Check if a given script is already running"""
    for proc in psutil.process_iter(['cmdline']):
        try:
            if proc.info['cmdline'] and script_name in proc.info['cmdline']:
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return False


@app.route("/file_status")
def file_status():
    if is_script_running("app_ex2.py"):
        return "✅ app.py is running"
    else:
        return "❌ app.py is NOT running"


# print(is_script_running("app.py"))  # True হলে app.py চলছে

# -----------------------------
# Background thread


def monitor_task():
    while True:
        
        gevent.sleep(2)   # ❗ time.sleep নয়
        monitor_data["counter"] += 1
        monitor_data["public_ip"] = get_public_ip()

        if is_script_running("app_ex2.py"):
            monitor_data["status"] = "active"
        else:
            monitor_data["status"] = "inactive"

        if check_connection():
            monitor_data["connection"] = "true"
        else:
            monitor_data["connection"] = "False"
        # print("Updated:", monitor_data)


# -----------------------------
# /data API with strong token auth


@app.route("/data")
def get_data():
    token = request.args.get("token")
    if token != SECRET_TOKEN:
        abort(401)  # Unauthorized

    monitor_data["last_update"] = time.strftime("%H:%M:%S")
    return jsonify(monitor_data)


# -----------------------------
# Browser live view page


# INDEX_HTML = "code2.html"


# @app.route("/")
# def index():
#     if os.path.exists(INDEX_HTML):
#         with open(INDEX_HTML, "r", encoding="utf-8") as f:  # ✅ Fix: encoding যোগ করা হলো
#             return render_template_string(f.read(), token=SECRET_TOKEN)
#     else:
#         return "app.html not found", 404


# INDEX_HTML2 = "templates/code2.html"


# @app.route("/user")
# def user():
#     if os.path.exists(INDEX_HTML2):
#         with open(INDEX_HTML2, "r", encoding="utf-8") as f:  # ✅ Fix: encoding যোগ করা হলো
#             return render_template_string(f.read(), token=SECRET_TOKEN)
#     else:
#         return "ex.html not found", 404

# @app.route("/user")
# def user():
#     return render_template("code2.html", token=SECRET_TOKEN, time=time)


@app.route("/user")
def user():
    with open("templates/code2.html", "r", encoding="utf-8") as f:
        html = f.read()

    resp = make_response(html)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp





# @app.route("/user")
# def user():
#     resp = make_response(render_template(
#         "code2.html",,
#         token=SECRET_TOKEN
#     ))
#     resp.headers["Cache-Control"] = "no-store"
#     return resp




@app.route("/favicon.ico")
def favicon():
    return "", 204


@app.route("/boom")
def boom():
    return 1 / 0


@app.errorhandler(Exception)
def handle_exception(e):
    err_id = uuid.uuid4().hex[:8]
    logger.exception(f"[ERR-{err_id}] Unhandled exception")

    return jsonify({
        "success": False,
        "error_id": err_id,
        "message": "Internal server error"
    }), 500



# @app.route("/errors")
# def list_error_files():
#     token = request.args.get("token")
#     if token != SECRET_TOKEN:
#         abort(401)

#     log_dir = "logs"
#     if not os.path.exists(log_dir):
#         return "No log directory", 404

#     files = sorted(
#         [f for f in os.listdir(log_dir) if f.startswith("error.log")],
#         reverse=True
#     )

#     html = "<h2>Error Log Files</h2><ul>"
#     for f in files:
#         html += f'<li><a href="/errors/view/{f}?token={token}">{f}</a></li>'
#     html += "</ul>"

#     return html



@app.route("/errors")
def errors_dashboard():
    token = request.args.get("token")
    if token != SECRET_TOKEN:
        abort(401)

    files = sorted(
        [f for f in os.listdir("logs") if f.startswith("error.log")],
        reverse=True
    )

    file_links = "".join(
        f'<li><a href="/errors/view/{f}?token={token}">{f}</a></li>'
        for f in files
    )

    return f"""
    <h2>⚠ Error Logs</h2>

    <div style="margin-bottom:15px;">
        <button onclick="control('start')">▶ Start</button>
        <button onclick="control('stop')">⏹ Stop</button>
        <button onclick="control('restart')">🔄 Restart</button>
        <button onclick="checkStatus()">📡 Status</button>
        <span id="status"></span>
    </div>

    <ul>{file_links}</ul>

    <script>
    const token = "{token}";

    function control(action) {{
        fetch("/admin/control?token=" + token, {{
            method: "POST",
            headers: {{ "Content-Type": "application/json" }},
            body: JSON.stringify({{ action }})
        }})
        .then(r => r.json())
        .then(d => alert(d.msg || d.error));
    }}

    function checkStatus() {{
        fetch("/admin/status?token=" + token)
        .then(r => r.json())
        .then(d => {{
            document.getElementById("status").innerText =
                " Status: " + d.status;
        }});
    }}
    </script>
    """



@app.route("/admin/control", methods=["POST"])
def admin_control():
    token = request.args.get("token")
    if token != SECRET_TOKEN:
        abort(401)

    action = request.json.get("action")

    if action == "start":
        # example
        # manager.start_all()
        return jsonify({"msg": "Server started"})

    if action == "stop":
        # graceful stop logic
        # manager.stop_all()
        return jsonify({"msg": "Server stopped"})

    if action == "restart":
        # stop + start
        # manager.stop_all()
        # manager.start_all()
        return jsonify({"msg": "Server restarted"})

    return jsonify({"error": "Invalid action"}), 400


@app.route("/admin/status")
def admin_status():
    token = request.args.get("token")
    if token != SECRET_TOKEN:
        abort(401)

    running = True  # or dynamic check

    return jsonify({
        "status": "RUNNING" if running else "STOPPED"
    })





# @app.route("/errors/view/<filename>")
# def view_error_file(filename):
#     token = request.args.get("token")
#     if token != SECRET_TOKEN:
#         abort(401)

#     # 🔐 security: path traversal block
#     if ".." in filename or "/" in filename or "\\" in filename:
#         abort(400)

#     path = os.path.join("logs", filename)

#     if not os.path.isfile(path):
#         abort(404)

#     with open(path, "r", encoding="utf-8") as f:
#         content = f.read()

#     return f"""
#     <h2>{filename}</h2>
#     <a href="/errors?token={token}">⬅ Back to list</a>
#     <pre style="background:#111;color:#0f0;padding:15px;white-space:pre-wrap;">
# {content}
#     </pre>
#     """



@app.route("/errors/view/<filename>")
def view_error_file(filename):
    token = request.args.get("token")
    if token != SECRET_TOKEN:
        abort(401)

    if ".." in filename:
        abort(400)

    path = os.path.join("logs", filename)
    if not os.path.isfile(path):
        abort(404)

    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    formatted = format_log_html(raw)

    return f"""
<!DOCTYPE html>
<html>
<head>
<style>
body {{
    background:#0d1117;
    color:#c9d1d9;
    font-family: monospace;
}}

pre {{
    background:#161b22;
    padding:15px;
    border-radius:6px;
    white-space:pre-wrap;
}}

.ts {{ color:#8b949e; }}
.level-error {{ color:#ff5555; font-weight:bold; }}
.traceback {{ color:#ffa657; }}
.file {{ color:#79c0ff; }}
.exception {{ color:#ff7b72; font-weight:bold; }}

a {{ color:#58a6ff; }}
</style>
</head>
<body>

<h3>{filename}</h3>
<a href="/errors?token={token}">⬅ Back</a>

<pre>{formatted}</pre>

</body>
</html>
"""







# @app.route("/user")
# def user():
#     return render_template("code2.html", token=SECRET_TOKEN)






# catch-all route
# @app.route('/', defaults={'path': ''})
# @app.route("/<path:path>")
@app.route('/', defaults={'path': ''}, methods=["GET", "POST"])
@app.route("/<path:path>", methods=["GET", "POST"])
def catch_all(path,user_id="1"):

    user_id = str(user_id)   # ✅ এখানেই
    # ensure_user_initialized(user_id)

    FRONTEND_ROUTES = ["user", "status", "login", "register"]

    if path == "" or path in FRONTEND_ROUTES or path.startswith("static"):
        abort(404)




    token = request.args.get("token")
    # if token != SECRET_TOKEN:
    #     return jsonify({"error": "Invalid or missing token"}), 401

    # নিশ্চিত করো path leading slash সহ
    full_path = "/" + path if not path.startswith("/") else path

    if request.method == "POST":
            print("post")
            body = request.get_json() or ""

            print(full_path,body,request.method)
            # data = get_api_data(full_path, query="", body=json.dumps(body), method="POST")
            data = manager.requast_api_data(user_id,full_path, body=json.dumps(body), method="POST",query="")
            # data = manager.get_api_datakk(user_id,full_path, body=json.dumps(body), method="POST",query={"user_id": user_id})

    # print("Path requested:", full_path)  # এখন দেখাবে: /v2/wallet/balances

    elif request.args.get("DCX") == "true":
        
        data = get_balance_dcx()
        data += get_futures_balance_dcx()

    elif request.args.get("funding_Dcx") == "true":
        data = get_live_dcx_funding()

    elif request.args.get("funding") == "true":
        # বিশেষ হ্যান্ডলিং funding_rates এর জন্য
        # সরাসরি থ্রেড-সেফ ডিকশনারি রিটার্ন করো
        # data = get_live_funding_data()
        data = manager.get_live_delta_funding(user_id=user_id)

        # print("Funding data fetched:", data)
    # elif request.args.get("ws") == "true":
    #     raw_path = request.path.lstrip('/')
    #     try:
    #         obj = json.loads(raw_path)        # JSON এ রূপান্তর
    #         print("✅ Fetching via WebSocket for object:", obj)

    #         data = get_data_ws(raw_path) # WebSocket থেকে ডেটা নাও
    #     except Exception:
    #         print("⚠️ Path not JSON:", raw_path)

    elif request.args.get("binance_funding") == "true":
        symbols_param = request.args.get("symbols")

        try:
            symbols = json.loads(symbols_param)
            if not isinstance(symbols, list):
                symbols = [symbols]
        except:
            symbols = symbols_param.split(",") if symbols_param else ["all"]

        data = get_live_binance_funding(symbols)
   

    elif request.args.get("positions") == "true":
        user_id = request.args.get("user_id")
        if not user_id:
            return jsonify({"error": "user_id is required for positions"}), 400
        
        user_id = str(user_id)
        print("Updating positions for user_id:", user_id)
        


        # 🔴 যদি WS data না থাকে → REST দিয়ে আনো
        # if not data_store[user_id]["positions"]:
        #     # print(f"[{user_id}] ⚠ No WS positions")

        #     return jsonify({
        #         "user_id": user_id,
        #         "positions": {},
        #         "orders": {},
        #         "margins": {},
        #         "message": "positions not ready yet"
        #     }), 200


        if user_id not in data_store:
            return jsonify({
                "user_id": user_id,
                "positions": {},
                "orders": {},
                "margins": {},
                "message": "user not initialized yet"
            }), 200

        if not data_store[user_id].get("positions"):
            return jsonify({
                "user_id": user_id,
                "positions": {},
                "orders": {},
                "margins": {},
                "message": "positions not ready yet"
            }), 200


        


        # data = show_user_data(user_id=user_id)
        # live_tickers = get_live_ticker()
        live_tickers = manager.get_live_delta_ticker(user_id=user_id)
        # print(live_tickers)
        # print(data.get("positions"))
        if live_tickers:
            update_unrealized_pnl_for_user(user_id, live_tickers)

        data = show_user_data(user_id=user_id)

    elif request.args.get("orders") == "true":
        user_id = request.args.get("user_id") or "1"

        data = show_user_data(user_id=user_id)    

    elif request.args.get("tickers_dcx") == "true":

        symbols_param = request.args.get("symbols")

        try:
            symbols = json.loads(symbols_param)
            if not isinstance(symbols, list):
                symbols = [symbols]
        except:
            symbols = symbols_param.split(",") if symbols_param else []

        # 🔥 multi-symbol instrument data
        data = get_futures_instrument_data_multi(
            symbols=symbols,
            margin="USDT"
        )



    elif request.args.get("tickers") == "true":
        symbols_param = request.args.get("symbols")
        # print(symbols_param)

        # 🧠 Case 1: symbols is JSON array (["BTCUSD","ETHUSD"])
        try:
            symbols = json.loads(symbols_param)
            if not isinstance(symbols, list):
                symbols = [symbols]
        except:
            # 🧠 Case 2: symbols is comma-separated string (BTCUSD,ETHUSD)
            symbols = symbols_param.split(",") if symbols_param else []

        # data = get_live_ticker(symbols=symbols)
        data = manager.get_live_delta_ticker(user_id=user_id,symbols=symbols)
        # print(data)
    else:
        print("full_path :",full_path)
        # return {}
        # if request.method == "POST":
        #     print("post")
        #     body = request.get_json() or ""

        #     # print(full_path,body,request.method)
        #     data = get_api_data(full_path, query="", body=json.dumps(body), method="POST")
        # else:
        #     print("get")
        #     data = get_api_data(full_path, query="", body="", method="GET")
    


        # print("ghghgh",request)
        # data = get_api_data(full_path, query="", body="", method="GET")
        # data = manager.get_api_datakk(user_id,full_path, body="", method="GET",query={"user_id": user_id})
        data = manager.requast_api_data(user_id,full_path, query="", body="", method="GET")
        # print("data :",data)

     # full_path পাঠাও
    # print("Data received:", data)
    return jsonify(data)


# -----------------------------




if __name__ == "__main__":
    print("✅ Starting server on Windows using gevent.pywsgi")

    # start websocket + background monitor ONLY ONCE
    manager.start_all()
    gevent.spawn(monitor_task)

    # http_server = WSGIServer(("127.0.0.1", 5000), app)
    http_server = WSGIServer(("0.0.0.0", 5000), app)
    http_server.serve_forever()



# -----------------------------
# if __name__ == "__main__":

      # 🔁 START WEBSOCKETS (ONLY ONCE)
# manager.start_all()
    # manager.start_arbitrage_bot()

# /////////////////////////
    # Start background thread
# t = threading.Thread(target=monitor_task, daemon=True)
# t.start()

# gevent.spawn(monitor_task)


# manager.start_all()
# gevent.spawn(monitor_task)

# //////////////////////////////

    # Print LAN + public IP
    # local_ip = socket.gethostbyname(socket.gethostname())
    # print("Local LAN IP:", local_ip)
    # print("Public IP:", get_public_ip())

    # # Optional HTTPS setup (for testing)
    # # For production use proper certificate from letsencrypt
    # ssl_context = None
    # cert_file = "cert.pem"
    # key_file = "key.pem"
    # if os.path.exists(cert_file) and os.path.exists(key_file):
    #     ssl_context = (cert_file, key_file)
    #     print("HTTPS enabled using self-signed certificate")


    # app.run(host="0.0.0.0", port=5000, debug=False,use_reloader=False,threaded=True, ssl_context=ssl_context)



#     pip install gunicorn gevent
# gunicorn -k gevent -w 1 app:app --bind 127.0.0.1:5000

# gunicorn -k gevent -w 1 app_ex2:app --bind 127.0.0.1:5000

# -w 2 hole  WebSocket ২ বার চলবে
#  manager.start_all() duplicate connection হবে


# 🧠 কেন এটা কাজ করবে?
# Component	Windows
# gevent	✅
# monkey.patch_all	✅
# gevent.pool.Pool	✅
# gevent.pywsgi	✅
# gunicorn	❌

# তুমি এখন ব্যবহার করছো:

# Flask + gevent + gevent.pywsgi


# ➡️ Windows-এ best possible combo

# 🔁 Linux / VPS এ গেলে কী করবে?

# একই code থাকবে, শুধু runner বদলাবে:

# gunicorn -k gevent -w 1 app_ex2:app

# 🏁 FINAL ANSWER (one line)

# Windows-এ run করতে → gevent.pywsgi ব্যবহার করো
# Gunicorn নয়

