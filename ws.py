from gevent import monkey
monkey.patch_all()

import gevent

from gevent import sleep
from gevent.lock import Semaphore

import sys
import os
import websocket
import json
import time
import hmac
import hashlib
# from concurrent.futures import ThreadPoolExecutor
import threading
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import firebase_admin
from firebase_admin import credentials, db
import signal
import uuid
import requests

#opional typing
from typing import Optional, Dict

from delta_rest_client import DeltaRestClient,OrderType,TimeInForce

from dcx_client import DcxRestClient


from dcx import get_dcx_funding_rate,dcx_map_builder,arbitrage_signal,countdown_from_ms,get_balance_dcx,get_futures_balance_dcx,get_current_funding_rate,get_futures_instrument_data,get_futures_instrument_data_multi,get_live_dcx_funding,place_futures_order,normalize_pair,smart_close_by_pair
from binance import get_binance_funding,dcx_to_binance_symbol,get_binance_funding_safe

# Thread pool with max 5 workers
# executor = ThreadPoolExecutor(max_workers=5)

# helper.py path add করা (data folder থেকে parent directory)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# from ex import get_api_data,SYMBOLS
import logging
# লগিং সেটআপ
# logging.basicConfig(filename='app.log', level=logging.INFO, 
#                     format='%(asctime)s - %(levelname)s - %(message)s')

# আপনি শুধু console-এ show করতে চান (file ছাড়া)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# ==========================
# CONFIG
# ==========================
# API_KEY = "8HiEv5IWu67YNnO1iqUX2cIx3hCa7p"
# API_SECRET = "O5dbzlC5IHgYEM1mfC0TUXnRSSokzzNhc5ucTcIYna7xNeNCyBaoHhw03GVB"
# BASE_URL = "https://cdn-ind.testnet.deltaex.org"
# WS_URL = "wss://socket-ind.testnet.deltaex.org"


# Firebase config
if not firebase_admin._apps:
    cred = credentials.Certificate(
        "air-share-be6be-firebase-adminsdk-pxekd-d21c09d722.json")  # তোমার key ফাইলের নাম
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://air-share-be6be-default-rtdb.firebaseio.com/'  # তোমার ডেটাবেস URL
    })

# Example: Database reference
# strategies_ref = db.reference("strategies")
# Firebase থেকে সব strategies একবারে লোড
# firebase_data = strategies_ref.get()



# WS_URL = "wss://socket.india.delta.exchange"
# USERS = [
#     {"user_id": "user1", "api_key": "8HiEv5IWu67YNnO1iqUX2cIx3hCa7p", "api_secret": "O5dbzlC5IHgYEM1mfC0TUXnRSSokzzNhc5ucTcIYna7xNeNCyBaoHhw03GVB"},
#     # add more users
# ]

# Thread-safe store
data_store = defaultdict(lambda: {"positions": {}, "orders": {}, "margins": {}})
# data_store = {}


# data_store = defaultdict(lambda: {
#     "positions": {},
#     "orders": {
#         "open": {},
#         "filled": {},
#         "cancelled": {},
#         "closed": {}
#     },
#     "margins": {}
# })


# data_lock = threading.Lock()
data_lock = Semaphore()

# ব্যবহারকারী এবং ক্লায়েন্ট ম্যানেজমেন্টের জন্য লক
# users_lock = threading.Lock()

# users_lock = threading.RLock()
users_lock = Semaphore()


# ==========================
# Helper: signature
# ==========================


def generate_signature(secret, message):
    message = bytes(message, 'utf-8')
    secret = bytes(secret, 'utf-8')
    hash = hmac.new(secret, message, hashlib.sha256)
    return hash.hexdigest()

# def generate_signature(secret, message):
#     timestamp = str(int(datetime.now(timezone.utc).timestamp()))
#     message = bytes(timestamp + message, 'utf-8')
#     secret = bytes(secret, 'utf-8')
#     hash = hmac.new(secret, message, hashlib.sha256)
#     return hash.hexdigest()

def send_authentication(self,ws):
    method = 'GET'
    timestamp = str(int(time.time()))
    # timestamp = str(int(datetime.now(timezone.utc).timestamp()))
    # time.sleep(0.1)  # ছোট ডিলে
    path = '/live'
    signature_data = method + timestamp + path
    signature = generate_signature(self.api_secret, signature_data)

    # Print components for debugging
    print(f"Timestamp: {timestamp}")
    print(f"Signature data: {signature_data}")
    print(f"Signature: {signature}")

    ws.send(json.dumps({
        "type": "key-auth",
        "payload": {
            "api-key": self.api_key,
            "signature": signature,
            "timestamp": timestamp
        }
    }))

# ==========================
# WebSocket per user
# ==========================
class UserWSClient:
    def __init__(self, user_id, api_key, api_secret, ws_url,manager):
        self.user_id = str(user_id)
        self.api_key = api_key
        self.api_secret = api_secret
        self.ws_url = ws_url
        self.manager = manager  # Manager reference
        self.ws = None
        self.thread = None
        self._reconnecting = True
        self._started = False



    def _on_open(self, ws):
        print(f"[{self.user_id}] WebSocket opened")
        # Auth
        # ts = str(int(time.time()))
        # msg = ts + "/live"
        # signature = generate_signature(self.api_secret, msg)
        # auth_msg = {"type": "key-auth", "payload": {"api-key": self.api_key, "signature": signature, "timestamp": ts}}
        # ws.send(json.dumps(auth_msg))

        # print(f"Socket opened")
    # api key authentication
        send_authentication(self,ws)


    def _on_message(self, ws, message):
        
        try:
            data = json.loads(message)
        except Exception as e:
            print("⚠️ invalid json:", message)
            return

    # Debug: raw message log (অন => চালাও, পরে বন্ধ করতে পারো)
        # print(f"[{self.user_id}] RAW: {data.get('type')} action={data.get('action')}")
        


        # data = json.loads(message)
        # print(data)
        msg_type = data.get("type")
        print("msg_type",msg_type)
        logging.debug("msg_type=%s", msg_type)


        if msg_type == "key-auth":
            if data.get("success"):
                print(f"[{self.user_id}] Authentication successful")
                # subscribe channels
                # self.subscribe_channel("positions", ["all"])
                # self.subscribe_channel("positions", [{"symbol": "all", "include_mark_price": True}])
                self.subscribe_channel("positions", ["all"], include_mark_price=True)

                self.subscribe_channel("orders", ["all"])
                self.subscribe_channel("margins")
            else:
                print(f"[{self.user_id}] ⚠️ Auth failed:", data)
                print("API KEY : "+self.api_key)


        
# /////////////////////////////////////////////////////////////////////////////

        # elif msg_type == "positions":
        #     with data_lock:
        #         action = data.get("action")
        #         results = data.get("result", [])

        #         print("action :",action)

            #     # live ticker data
            #     live_tickers = get_live_ticker()  # dict: {symbol: {"mark_price": ...}}

            #     for pos in results:
            #         symbol = pos.get("product_symbol") or pos.get("symbol")
            #         if not symbol:
            #             continue

            #         print(symbol)

                    

            #         # get old or new
            #         old = data_store[self.user_id]["positions"].get(symbol)
            #         if old is None:
            #             old = pos.copy()
            #         else:
            #             old = old.copy()
            #             for k, v in pos.items():
            #                 old[k] = v

            # # update data_store
            #         data_store[self.user_id]["positions"][symbol] = old

            # # ✅ update unrealized PnL immediately after storing
            #         size = float(old.get("size", 0) or 0)
            #         entry = float(old.get("entry_price", 0) or 0)
            #         mark_price = live_tickers.get(symbol, {}).get("mark_price", old.get("mark_price", 0))
            #         print(mark_price)

            #         if size != 0 and mark_price is not None:
            #             mark_price = float(mark_price)
            #             pnl = (mark_price - entry) * size if size > 0 else (entry - mark_price) * abs(size)
            #             old["mark_price"] = mark_price
            #             old["unrealized_pnl"] = pnl

            #  # remove if size == 0
            #         if int(old.get("size", 0)) == 0:
            #             data_store[self.user_id]["positions"].pop(symbol, None)
            #             print(f"[{self.user_id}] 🧹 {symbol} position closed & removed")

            #     print(f"[{self.user_id}] 🔁 Positions merged ({len(results)})")
# ///////////////////////////////////////////////////////////////////////////////////////////

                # if action == "snapshot":
                #     # snapshot = full replace for listed positions
                #     for pos in results:
                #         symbol = pos.get("product_symbol") or pos.get("symbol")
                #         if not symbol:
                #             continue
                #         # store a copy
                #         data_store[self.user_id]["positions"][symbol] = pos.copy()
                #     print(f"[{self.user_id}] ✅ Snapshot received ({len(results)} positions)")
                #     print("positions snapshot : ", data_store[self.user_id]["positions"])

                # elif action in ("update", "create"):
                #     for pos in results:
                #         symbol = pos.get("product_symbol") or pos.get("symbol")
                #         if not symbol:
                #             continue

                #         # debug print of incoming partial update
                #         print(f"[{self.user_id}] UPDATE => {symbol}: keys={list(pos.keys())}")

                #         old = data_store[self.user_id]["positions"].get(symbol, {}).copy()
                #         # only overwrite keys that arrived
                #         for k, v in pos.items():
                #             old[k] = v

                #         # remove if size == 0
                #         print(int(old.get("size", 0)))
                #         if int(old.get("size", 0)) == 0:
                #             data_store[self.user_id]["positions"].pop(symbol, None)
                #             print(f"[{self.user_id}] 🧹 {symbol} position closed & removed")
                #         else:
                #             data_store[self.user_id]["positions"][symbol] = old
                #     print(f"[{self.user_id}] 🔁 Position update merged ({len(results)})")
                #     # Subscribe only if not already subscribed (e.g., first create)
                #     # if action == "create":
                #     self.subscribe_channel("positions", ["all"], include_mark_price=True)
                #         # self._is_positions_subscribed = True  # Flag to avoid repeat
                #     print("positions : ", data_store[self.user_id]["positions"])


# ///////////////////////////////////////////////////////////////////////////////////////
        elif msg_type == "positions":
            with data_lock:
                if self.user_id not in data_store:
                    return
                action = data.get("action")
                results = data.get("result", [])

                print("action :",action)
                if action == "snapshot":
                    print(f"[{self.user_id}] ✅ Snapshot received ({len(results)} positions)")
                    for pos in results:
                        symbol = pos.get("product_symbol") or pos.get("symbol")
                        if not symbol:
                            continue
                        data_store[self.user_id]["positions"][symbol] = pos
                    print(f"[{self.user_id}] 🔁 Positions initialized: {data_store[self.user_id]['positions']}")

                elif action in ("create", "update"):
                    # ❗ create/update এ data empty হতে পারে
                    # তাই WS data বিশ্বাস না করে REST refresh করো
                    # MultiUserManager.refresh_positions_once(self.user_id)
                    gevent.spawn(self.manager.refresh_positions_once,self.user_id)
                    print(f"[{self.user_id}] 🔄 Position refresh triggered due to {action}")

                elif action == "delete":
                    # MultiUserManager.refresh_positions_once(self.user_id)
                    gevent.spawn(self.manager.refresh_positions_once,self.user_id)
                    print(f"[{self.user_id}] 🔥 Position delete → forced refresh")


                # elif action == "create":
                #     print(f"[{self.user_id}] 🎉 New position created")
                #     for pos in results:
                #         symbol = pos.get("product_symbol") or pos.get("symbol")
                #         if not symbol:
                #             continue
                #         # Debug print of incoming data
                #         print(f"[{self.user_id}] CREATE => {symbol}: keys={list(pos.keys())}")
                #         # Initialize position in data_store if not exists
                #         if symbol not in data_store[self.user_id]["positions"]:
                #             data_store[self.user_id]["positions"][symbol] = {}
                #         # Merge new data
                #         old = data_store[self.user_id]["positions"][symbol].copy()
                #         for k, v in pos.items():
                #             old[k] = v
                #         data_store[self.user_id]["positions"][symbol] = old


                #         # Run in a separate thread to avoid blocking
                #         # threading.Thread(target=refresh_positions_once, args=(self.user_id,), daemon=True).start()
                #         # print(f"[{self.user_id}] 📈 Triggered position refresh for filled order: {oid}")


                #         # Subscribe only on first create if not subscribed
                #         # if not hasattr(self, '_is_positions_subscribed') or not self._is_positions_subscribed:
                #         #     self.subscribe_channel("positions", ["all"], include_mark_price=True)
                #         #     self._is_positions_subscribed = True  # Set flag to avoid repeat
                #     print(f"[{self.user_id}] 🔁 Position created/merged ({len(results)})")
                #     print(f"positions : {data_store[self.user_id]['positions']}")

                # elif action == "update":
                #     print(f"[{self.user_id}] 🔧 Position updated")

                #     if not results:
                #         print(f"[{self.user_id}] ⚠️ Empty update — keeping existing positions")
                #         return  # কিছু না করেই বেরিয়ে যাও

                #     for pos in results:
                #         symbol = pos.get("product_symbol") or pos.get("symbol")
                #         if not symbol:
                #             continue

                #         print(f"[{self.user_id}] UPDATE => {symbol}: keys={list(pos.keys())}")
                #         old = data_store[self.user_id]["positions"].get(symbol, {}).copy()

                #         # নতুন data merge করা হচ্ছে
                #         for k, v in pos.items():
                #             old[k] = v

                #         # যদি size key থাকে এবং সত্যি 0 হয়, তবেই remove করো
                #         if "size" in pos and int(pos.get("size", 0)) == 0:
                #             data_store[self.user_id]["positions"].pop(symbol, None)
                #             print(f"[{self.user_id}] 🧹 {symbol} position closed & removed")
                #         else:
                #             data_store[self.user_id]["positions"][symbol] = old

                #     print(f"[{self.user_id}] 🔁 Position update merged ({len(results)})")
                #     print(f"positions : {data_store[self.user_id]['positions']}")


                # elif action == "delete":
                #     print(f"[{self.user_id}] ❌ Position deleted")
                #     if not results:  # যদি results খালি থাকে
                #         for symbol in list(data_store[self.user_id]["positions"].keys()):
                #             data_store[self.user_id]["positions"].pop(symbol, None)
                #             print(f"[{self.user_id}] 🧹 {symbol} position removed (manual cleanup)")
                #     else:
                #         for pos in results:
                #             symbol = pos.get("product_symbol") or pos.get("symbol")
                #             if not symbol:
                #                 continue
                #             if symbol in data_store[self.user_id]["positions"]:
                #                 data_store[self.user_id]["positions"].pop(symbol, None)
                #                 print(f"[{self.user_id}] 🧹 {symbol} position removed")
                #     print(f"[{self.user_id}] 🔁 Position deleted ({len(results)})")
                #     print(f"positions : {data_store[self.user_id]['positions']}")

# /////////////////////////////////////////////////////////////////////////////////////////////
            



        # elif msg_type == "positions":
        #     with data_lock:
        #         action = data.get("action")
        #         results = data.get("result", [])

        # # live ticker data
        #         live_tickers = get_live_ticker()  # dict: {symbol: {"mark_price": ...}}

        #         if action == "snapshot":
        #             for pos in results:
        #                 symbol = pos.get("product_symbol") or pos.get("symbol")
        #                 if not symbol:
        #                     continue

        #         # store a copy
        #                 data_store[self.user_id]["positions"][symbol] = pos.copy()

        #         # ✅ get mark_price from live ticker if available
        #                 mark_price = live_tickers.get(symbol, {}).get("mark_price", pos.get("mark_price"))
        #                 size = float(pos.get("size", 0) or 0)
        #                 entry = float(pos.get("entry_price", 0) or 0)

        #                 if size != 0 and mark_price is not None:
        #                     mark_price = float(mark_price)
        #                     pnl = (mark_price - entry) * size if size > 0 else (entry - mark_price) * abs(size)
        #                     data_store[self.user_id]["positions"][symbol]["mark_price"] = mark_price
        #                     data_store[self.user_id]["positions"][symbol]["unrealized_pnl"] = pnl

        #             print(f"[{self.user_id}] ✅ Snapshot received ({len(results)} positions)")

        # elif action in ("update", "create"):
        #     for pos in results:
        #         symbol = pos.get("product_symbol") or pos.get("symbol")
        #         if not symbol:
        #             continue

        #         old = data_store[self.user_id]["positions"].get(symbol, {}).copy()
        #         for k, v in pos.items():
        #             old[k] = v

        #         size = float(old.get("size", 0) or 0)
        #         entry = float(old.get("entry_price", 0) or 0)

        #         # ✅ update mark_price from live ticker if available
        #         mark_price = live_tickers.get(symbol, {}).get("mark_price", old.get("mark_price", 0))

        #         if size != 0 and mark_price != 0:
        #             mark_price = float(mark_price)
        #             pnl = (mark_price - entry) * size if size > 0 else (entry - mark_price) * abs(size)
        #             old["mark_price"] = mark_price
        #             old["unrealized_pnl"] = pnl

        #         if int(old.get("size", 0)) == 0:
        #             data_store[self.user_id]["positions"].pop(symbol, None)
        #             print(f"[{self.user_id}] 🧹 {symbol} position closed & removed")
        #         else:
        #             data_store[self.user_id]["positions"][symbol] = old

        #     print(f"[{self.user_id}] 🔁 Position update merged ({len(results)})")









        # elif msg_type == "orders":
        #     with data_lock:
        #         orders = data.get("result", [data])
        #         for order in orders:
        #             oid = order.get("order_id") or order.get("id")
        #             if not oid:
        #                 continue

        #     # Order closed/cancelled → remove from store
        #             if order.get("size", 0) == 0:
        #                 if oid in data_store[self.user_id]["orders"]:
        #                     data_store[self.user_id]["orders"].pop(oid)
        #                     print(f"[{self.user_id}] Order closed/cancelled: {oid}")
        #             else:
        #                 data_store[self.user_id]["orders"][oid] = order
        #         print(f"[{self.user_id}] Orders update received")




        # elif msg_type == "orders":
        #     with data_lock:
        #         orders = data.get("result", [data])
        #         for order in orders:
        #             print("order",order)

        #             oid = order.get("order_id") or order.get("id")
        #             if not oid:
        #                 continue
        #             symbol = order.get("product_symbol") or order.get("symbol")
        #             print(f"[{self.user_id}] Order Update: oid={oid}, symbol={symbol}, size={order.get('size')}, state={order.get('state')}, unfilled_size={order.get('unfilled_size')}")
        #             if order.get("state") in ["closed", "cancelled", "filled"] and order.get("reason") != "fill" or order.get("unfilled_size", 0) == 0:
        #                 if oid in data_store[self.user_id]["orders"]:
        #                     data_store[self.user_id]["orders"].pop(oid)
        #                     print(f"[{self.user_id}] Order {order.get('state')}: {oid}")
        #                     if symbol and symbol in data_store[self.user_id]["positions"]:
        #                         data_store[self.user_id]["positions"].pop(symbol, None)
        #                         print(f"[{self.user_id}] 🧹 {symbol} position closed & removed due to order")
        #             else:
        #                 data_store[self.user_id]["orders"][oid] = order
        #         print(f"[{self.user_id}] Orders update received")
        #         print("orders : ", data_store[self.user_id]["orders"])


# //////////////////////////////////////////////////////////////////////////////

        # elif msg_type == "orders":
        #     with data_lock:
        #         orders = data.get("result", [data])
        #         for order in orders:
        #             print("order :",order)

        #             oid = order.get("order_id") or order.get("id")
        #             if not oid:
        #                 continue
        #             symbol = order.get("product_symbol") or order.get("symbol")
        #             print(f"[{self.user_id}] Order Update: oid={oid}, symbol={symbol}, size={order.get('size')}, state={order.get('state')}, unfilled_size={order.get('unfilled_size')}, reason={order.get('reason')}")
        #             if order.get("state") in ["open","closed", "cancelled"] or order.get("unfilled_size", 0) == 0:
        #                     print("s1")
        #                     if order.get("reduce_only") and order.get("reduce_only") == True:
        #                         if oid in data_store[self.user_id]["orders"]:
        #                             data_store[self.user_id]["orders"].pop(oid)
        #                             print(f"[{self.user_id}] Order {order.get('state')}: {oid} removed")
        #                             if symbol and symbol in data_store[self.user_id]["positions"]:
        #                                 size = data_store[self.user_id]["positions"][symbol]["size"]
                                        
        #                                 order_size = float(order.get("size", 0))
        #                                 position_size = float(size)
        #                                 # absolute value এবং .0 বাদ দেওয়া
        #                                 order_size_abs = f"{abs(order_size):.3f}".rstrip('0').rstrip('.')
        #                                 position_size_abs = f"{abs(position_size):.3f}".rstrip('0').rstrip('.')
                                        

        #                                 if order_size_abs != position_size_abs:
        #                                     print("order size :",order_size)
        #                                     print("position size :",size)
        #                                     # print("size = 2")
        #                                     # Submit to thread pool instead of creating new thread
        #                                     executor.submit(refresh_positions_once, self.user_id)
        #                                     print(f"[{self.user_id}] 📈 Triggered position refresh for filled order: {oid}")
        #                                 else :
        #                                     print("reduce_only true")
        #                                     print(data_store[self.user_id]["positions"])
        #                                     data_store[self.user_id]["positions"].pop(symbol, None)
        #                                     print(f"[{self.user_id}] 🧹 {symbol} position closed & removed due to order")    


        # #             if order.get("state") in ["closed", "cancelled"] or order.get("unfilled_size", 0) == 0:
        # #                 if order.get("reduce_only") and order.get("reduce_only") == True:
        # # # remove from orders
        # #                     if oid in data_store[self.user_id]["orders"]:
        # #                         data_store[self.user_id]["orders"].pop(oid)
        # #                         print(f"[{self.user_id}] Order {order.get('state')}: {oid} removed")

        # #                     if symbol and symbol in data_store[self.user_id]["positions"]:
        # #                         pos = data_store[self.user_id]["positions"][symbol]
        # #                         size_before = int(pos.get("size", 0))
        # #                         filled_size = int(order.get("filled_size", 0)) or int(order.get("size", 0))

        # #                         new_size = size_before - filled_size if size_before > 0 else size_before + filled_size
        # #                         print(f"[{self.user_id}] 🧩 size_before={size_before}, filled_size={filled_size}, new_size={new_size}")

        # #                         if new_size <= 0:
        # #                             data_store[self.user_id]["positions"].pop(symbol, None)
        # #                             print(f"[{self.user_id}] 🧹 {symbol} position closed & removed due to full reduce")
        # #                         else:
        # #         # শুধু size update করো, position alive থাকবে
        # #                             pos["size"] = new_size
        # #                             data_store[self.user_id]["positions"][symbol] = pos
        # #                             print(f"[{self.user_id}] 🔄 {symbol} position partially reduced (remaining size={new_size})")           



        #                     else :
        #                         print("s2")


        #                         # Update positions from order data
        #                         if symbol not in data_store[self.user_id]["positions"]:
        #                             print("s3")
        #                             data_store[self.user_id]["positions"][symbol] = {}
        #                         print("s4")    
        #                         pos_data = data_store[self.user_id]["positions"][symbol]
        #                         pos_data.update({
        #                             "product_symbol": symbol,
        #                             "product_id": order.get("product_id"),
        #                             "user_id": self.user_id,
        #                             "size": order.get("filled_size", order.get("size", 0)),
        #                             "entry_price": order.get("average_fill_price"),
        #                             "commission": order.get("paid_commission", "0"),
        #                             "created_at": order.get("created_at"),
        #                             "updated_at": order.get("updated_at"),
        #                             "margin": float(order.get("margin", 0.0)),  # ফ্লোটে রূপান্তর
        #                             "mark_price": float(order.get("mark_price", 0.0)),  # ফ্লোটে রূপান্তর
        #                             "unrealized_pnl": float(order.get("unrealized_pnl", 0.0)),  # ফ্লোটে রূপান্তর
        #                             "default_leverage": float(order.get("product", {}).get("default_leverage", 1.0))  # নিরাপদে আপডেট  # ফ্লোট হিসেবে, ডিফল্ট 1.0
        #                         })
        #                         print(f"[{self.user_id}] 📈 Position updated from order: {symbol}")
                                
        #                         if oid in data_store[self.user_id]["orders"]:
        #                             print("s5")
        #                             data_store[self.user_id]["orders"].pop(oid)
        #                             print(f"[{self.user_id}] Order {order.get('state')}: {oid} removed")   

        #                         # Submit to thread pool instead of creating new thread
        #                             executor.submit(refresh_positions_once, self.user_id)
        #                             print(f"[{self.user_id}] 📈 Triggered position refresh for filled order: {oid}")         
        #             else:
        #                 print("s6")
        #                 data_store[self.user_id]["orders"][oid] = order
                      
        #                 # print(f"[{self.user_id}] Orders update received")
        #                 # print("orders : ", data_store[self.user_id]["orders"])
        #                 # print("positions : ", data_store[self.user_id]["positions"])


        # ////////////////////////////////////////////////////////////////////////


        elif msg_type == "orders":
            with data_lock:
                if self.user_id not in data_store:
                    return
                orders = data.get("result", [data])

                for order in orders:
                    oid = order.get("order_id") or order.get("id")
                    if not oid:
                        continue

                    symbol = order.get("product_symbol") or order.get("symbol")
                    state = order.get("state")
                    reduce_only = bool(order.get("reduce_only"))
                    unfilled = float(order.get("unfilled_size", 0) or 0)

                    print(
                        f"[{self.user_id}] Order Update: oid={oid}, symbol={symbol}, "
                        f"state={state}, reduce_only={reduce_only}, unfilled={unfilled}"
                    )

                    # 1️⃣ Always keep latest order state
                    data_store[self.user_id]["orders"][oid] = order

                    # 2️⃣ REDUCE ONLY orders
                    if reduce_only:
                        if state in ["closed", "cancelled"] or unfilled == 0:
                            data_store[self.user_id]["orders"].pop(oid, None)

                            # 🔁 ONLY refresh positions (positions WS will decide truth)
                            # MultiUserManager.refresh_positions_once(self.user_id)
                            gevent.spawn(self.manager.refresh_positions_once,self.user_id)

                            print(
                                f"[{self.user_id}] 🔻 reduce_only finished → refresh positions"
                            )

                        continue  # ⛔ reduce_only handled fully

                    # 3️⃣ NORMAL orders (non reduce_only)
                    if state in ["closed", "cancelled"] or unfilled == 0:
                        data_store[self.user_id]["orders"].pop(oid, None)

                        # MultiUserManager.refresh_positions_once(self.user_id)
                        gevent.spawn(self.manager.refresh_positions_once,self.user_id)

                        print(
                            f"[{self.user_id}] ✅ order finished → refresh positions"
                            f" and removed order: {oid}")


# ///////////////////////////////////////////////////////////////////////////////
        # elif msg_type == "orders":
        #     with data_lock:
        #         orders = data.get("result", [data])
        #         for order in orders:
        #             print("order :", order)

        #             oid = order.get("order_id") or order.get("id")
        #             if not oid:
        #                 continue

        #             symbol = order.get("product_symbol") or order.get("symbol")
        #             state = order.get("state")
        #             action = order.get("action")
        #             reduce_only = bool(order.get("reduce_only"))

        #             print(f"[{self.user_id}] Order Update: oid={oid}, symbol={symbol}, "
        #                 f"size={order.get('size')}, state={state}, "
        #                 f"unfilled_size={order.get('unfilled_size')}, reason={order.get('reason')}")

        #             # 👉 1) সবার আগে orders-এ লাস্ট স্টেট রেখে দেই
        #             data_store[self.user_id]["orders"][oid] = order

        #             # 👉 2) REDUCE ONLY অর্ডারগুলোর হ্যান্ডলিং
        #             if reduce_only:
        #                 # শুধু তখনই কাজ করব যখন অর্ডার শেষ / ক্যান্সেলড / পুরো ফিল্ড
        #                 if state in ["closed", "cancelled"] or float(order.get("unfilled_size", 0)) == 0:
        #                     print("🔻 reduce_only ব্লক")

        #                     # orders থেকে সরিয়ে দাও
        #                     if oid in data_store[self.user_id]["orders"]:
        #                         data_store[self.user_id]["orders"].pop(oid, None)
        #                         print(f"[{self.user_id}] Order {state}: {oid} removed")

        #                     # positions এ symbol থাকলে reduce/close করব
        #                     if symbol and symbol in data_store[self.user_id]["positions"]:
        #                         size = data_store[self.user_id]["positions"][symbol]["size"]

        #                         order_size = float(order.get("size", 0))
        #                         position_size = float(size)

        #                         order_size_abs = f"{abs(order_size):.3f}".rstrip('0').rstrip('.')
        #                         position_size_abs = f"{abs(position_size):.3f}".rstrip('0').rstrip('.')

                                
        #                         unfilled = float(order.get("unfilled_size",0))
        #                         # filled = order_size - unfilled

        #                         if order_size_abs != position_size_abs or unfilled > 0:
        #                             print("order size :", order_size)
        #                             print("position size :", size)
        #                             executor.submit(MultiUserManager.refresh_positions_once, self.user_id)
        #                             print(f"[{self.user_id}] 📈 Triggered position refresh for filled reduce_only: {oid}")
        #                         else:
        #                             print("reduce_only true → full close")
        #                             print(data_store[self.user_id]["positions"])
        #                             data_store[self.user_id]["positions"].pop(symbol, None)
        #                             print(f"[{self.user_id}] 🧹 {symbol} position closed & removed due to reduce_only")
        #                 else:
        #                     # reduce_only কিন্তু এখনো open → কিছু করার দরকার নাই
        #                     print("🕒 reduce_only but still open, waiting for fill/cancel")
        #                 continue  # reduce_only হ্যান্ডল শেষ, নীচের normal লজিকে নামবে না


        #             # 3️⃣ NORMAL orders (non reduce_only)
        #             if state in ["closed", "cancelled"] or unfilled == 0:
        #                 data_store[self.user_id]["orders"].pop(oid, None)

        #                 executor.submit(
        #                     MultiUserManager.refresh_positions_once,
        #                     self.user_id
        #                 )

        #                 print(
        #                     f"[{self.user_id}] ✅ order finished → refresh positions"
        #                     f" and removed order: {oid}"
        #                 )

                    # 👉 3) NORMAL অর্ডার (reduce_only না)

                    # (a) নতুন create/open অর্ডার → পজিশন বানাই/আপডেট করি (যদি তুমি এ সময়েই দেখতে চাও)
                    # if action == "create" and state == "open":
                    #     print("s2 - normal create/open → position update")

                    #     if symbol not in data_store[self.user_id]["positions"]:
                    #         print("s3")
                    #         data_store[self.user_id]["positions"][symbol] = {}

                    #     print("s4")
                    #     pos_data = data_store[self.user_id]["positions"][symbol]
                    #     pos_data.update({
                    #         "product_symbol": symbol,
                    #         "product_id": order.get("product_id"),
                    #         "user_id": self.user_id,
                    #         "size": order.get("filled_size", order.get("size", 0)),
                    #         "entry_price": order.get("average_fill_price") or order.get("limit_price"),
                    #         "commission": order.get("paid_commission", "0"),
                    #         "created_at": order.get("created_at"),
                    #         "updated_at": order.get("updated_at"),
                    #         "margin": float(order.get("margin", 0.0)),
                    #         "mark_price": float(order.get("mark_price", 0.0)),
                    #         "unrealized_pnl": float(order.get("unrealized_pnl", 0.0)),
                    #         "default_leverage": float(order.get("product", {}).get("default_leverage", 1.0))
                    #     })
                    #     print(f"[{self.user_id}] 📈 Position updated from OPEN order: {symbol}")

                    # (b) অর্ডার ফিল্ড / ক্লোজড হয়েছে → পজিশন রিফ্রেশ কর
                    # if state in ["closed"] or order.get("unfilled_size", 0) == 0:
                    #     print("✅ order closed/filled → refresh + cleanup")

                    #     if oid in data_store[self.user_id]["orders"]:
                    #         data_store[self.user_id]["orders"].pop(oid, None)
                    #         print(f"[{self.user_id}] Order {state}: {oid} removed")

                    #     executor.submit(MultiUserManager.refresh_positions_once, self.user_id)
                    #     print(f"[{self.user_id}] 📈 Triggered position refresh for filled/closed order: {oid}")

                    # # (c) অর্ডার cancelled হয়েছে কিন্তু reduce_only না → শুধু orders থেকে সরালে চাইলে
                    # if state == "cancelled":
                    #     print("🚫 order cancelled (normal)")
                    #     data_store[self.user_id]["orders"].pop(oid, None)


        


        elif msg_type == "margins":
            with data_lock:
                if self.user_id not in data_store:
                    return
        # শুধুমাত্র action=="update" থাকলে save করো
                if data.get("action") == "update":
                    asset = data.get("asset_symbol")
                    if asset:
                        data_store[self.user_id]["margins"][asset] = {
                            "balance": data.get("balance"),
                            "available_balance": data.get("available_balance"),
                            "equity": data.get("robo_trading_equity") or None,
                            "blocked_margin": data.get("blocked_margin")
                        }
                        print(f"[{self.user_id}] Margins update saved for {asset}")
                    else:
                        print(f"[{self.user_id}] Margins subscription confirm / non-update message: {data}")





        # elif msg_type == "positions":
        #     # print(data)
        #     with data_lock:
        #         if data.get("action") == "snapshot":
        #             for pos in data.get("result", []):
        #                 symbol = pos.get("product_symbol") or pos.get("symbol")
        #             if symbol:
        #                 data_store[self.user_id]["positions"][symbol] = pos
        #         else:
        #             symbol = data.get("symbol")
        #             if symbol:
        #                 data_store[self.user_id]["positions"][symbol] = data
        #     print(f"[{self.user_id}] Positions update received")
        # elif msg_type == "orders":
        #     with data_lock:
        #         for order in data.get("result", [data]):
        #             oid = order.get("order_id") or order.get("id")
        #             if oid:
        #                 data_store[self.user_id]["orders"][oid] = order
        #     print(f"[{self.user_id}] Orders update received")
       

        # elif msg_type == "margins":
        #     with data_lock:
        # # শুধুমাত্র action=="update" থাকলে save করো
        #         if data.get("action") == "update":
        #             asset = data.get("asset_symbol")
        #             if asset:
        #                 data_store[self.user_id]["margins"][asset] = data
        #                 print(f"[{self.user_id}] Margins update saved for {asset}")
        #             else:
        #                 print(f"[{self.user_id}] Margins subscription confirm / non-update message: {data}")

        elif msg_type == "ping":
            ws.send(json.dumps({"type": "pong"}))
            print(f"[{self.user_id}] ❤️ Heartbeat OK")






    # def update_unrealized_pnl(self, symbol, mark_price):
    #     with data_lock:
    #         pos = data_store[self.user_id]["positions"].get(symbol)
    #         if not pos:
    #             return None  # যদি position না থাকে

    #     # Safe float conversion
    #         try:
    #             size = float(pos.get("size", 0) or 0)
    #             entry = float(pos.get("entry_price", 0) or 0)
    #             mark = float(mark_price or 0)
    #         except (TypeError, ValueError):
    #             return None

    #     # Calculate PnL
    #         if size > 0:
    #             pnl = (mark - entry) * size
    #         else:
    #             pnl = (entry - mark) * abs(size)

    #     # Update stored data
    #         pos["mark_price"] = mark
    #         pos["unrealized_pnl"] = pnl

    #     # Print formatted
    #         print(f"[{self.user_id}] {symbol} | size={size} | entry={entry} | mark={mark} | unrealized PnL={pnl}")

    #     # ✅ Return structured data
    #         return {
    #             "user_id": self.user_id,
    #             "symbol": symbol,
    #             "size": size,
    #             "entry_price": entry,
    #             "mark_price": mark,
    #             "unrealized_pnl": pnl
    #         }


     


    def _on_error(self, ws, error):
        print(f"[{self.user_id}] WS error:", error)

    # def _on_close(self, ws, code, msg):
    #     print(f"[{self.user_id}] WS closed: {code}, {msg}")
    #     backoff = 1
    #     while True:
    #         try:
    #             print(f"[{self.user_id}] 🔄 Reconnecting in {backoff}s...")
    #             time.sleep(backoff)
    #             self._run()
    #             break
    #         except Exception as e:
    #             print(f"[{self.user_id}] reconnect failed: {e}")
    #             backoff = min(backoff * 2, 60)


    # def _on_close(self, ws, code, msg):
    #     if not getattr(self, "_reconnecting", True):
    #         return

    #     backoff = 1
    #     while backoff <= 60:
    #         time.sleep(backoff)
    #         try:
    #             self._run()
    #             return
    #         except:
    #             backoff *= 2

    # def _on_close(self, ws, code, msg):
    #     if not self._reconnecting:
    #         return

    #     def reconnect():
    #         backoff = 1
    #         while backoff <= 60 and self._reconnecting:
    #             # time.sleep(backoff)
    #             sleep(backoff)
    #             try:
    #                 self._run()
    #                 return
    #             except:
    #                 backoff = min(backoff * 2, 60)

    #     threading.Thread(target=reconnect, daemon=True).start()


    def _on_close(self, ws, code, msg):
        if not self._reconnecting:
            return

        self._started = False   # 🔥 IMPORTANT

        def reconnect():
            backoff = 1
            while backoff <= 60 and self._reconnecting:
                sleep(backoff)
                try:
                    self._run()
                    return
                except:
                    backoff = min(backoff * 2, 60)

        gevent.spawn(reconnect)


# ///////////////////////////////////////////////////


    # def subscribe_channel(self, name, symbols=None):
    #     ch = {"name": name}
    #     if symbols:
    #         ch["symbols"] = symbols
    #     ws_msg = {"type": "subscribe", "payload": {"channels": [ch]}}
    #     self.ws.send(json.dumps(ws_msg))
    #     print(f"[{self.user_id}] Subscribed to {name} with {symbols}")


    def subscribe_channel(self, name, symbols=None, include_mark_price=False):
        ch = {"name": name}
        if symbols is not None:
            ch["symbols"] = symbols
        if include_mark_price:
        # Delta এ যদি accept করে এমন payload চাইলে যোগ করো
            ch["include_mark_price"] = True
        ws_msg = {"type": "subscribe", "payload": {"channels": [ch]}}
        self.ws.send(json.dumps(ws_msg))
        print(f"[{self.user_id}] Subscribed to {name} with {symbols} include_mark_price={include_mark_price}")
    

    # def start(self):
    #     self.thread = threading.Thread(target=self._run, daemon=True)
    #     self.thread.start()

    # def start(self):
    #     if self._started:
    #         return
    #     self._started = True
    #     self.thread = threading.Thread(target=self._run, daemon=True)
    #     self.thread.start()


    def start(self):
        if self._started:
            return
        self._started = True
        gevent.spawn(self._run)    


    def _run(self):
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
         # ping_interval ensures periodic pings to keep connection alive
        self.ws.run_forever(ping_interval=20, ping_timeout=10)
        # self.ws.run_forever()


# ==============================
# Public ws clint
# ==============================

class PublicWSClient:
    def __init__(self, ws_url, symbols=None):
        self.ws_url = ws_url
        self.symbols = symbols or ["all"]

        self.ticker = {}
        self.funding = {}

        # self.lock = threading.Lock()
        self.ws = None
        # self.thread = None
        self.running = False

    def on_open(self, ws):
        print("🟢 Public WebSocket connected")

        sub_msg = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "v2/ticker", "symbols": self.symbols},
                    {"name": "funding_rate", "symbols": self.symbols}
                ]
            }
        }
        ws.send(json.dumps(sub_msg))
        print("✅ Subscribed to public channels")

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
        except Exception:
            return

        if not isinstance(data, dict):
            return

        msg_type = data.get("type")
        if msg_type not in ("v2/ticker", "funding_rate"):
            return
        

        symbol = data.get("symbol")
        if not symbol:
            return

        # with self.lock:
        if msg_type == "v2/ticker":
            self.ticker[symbol] = data
        elif msg_type == "funding_rate":
            self.funding[symbol] = data

    def on_error(self, ws, error):
        print("❌ Public WS error:", error)




    # def on_close(self, ws, code, msg):
    #     if not self.running:
    #         return

    #     self.running = False      # 🔥 reset first
    #     sleep(2)
    #     self.start()

    def on_close(self, ws, code, msg):
        if not self.running:
            return

        self.running = False

        def reconnect():
            sleep(2)
            if not self.running:
                self.start()

        gevent.spawn(reconnect)

        

    def _reconnect(self):
        if self.running:
            return
        self.start()


    def start(self):
        if self.running:
            return

        self.running = True

        def _run():
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            self.ws.run_forever(ping_interval=20, ping_timeout=10)

        gevent.spawn(_run)   # 🔥 NOT direct call
    
        
    

    # def start(self):
    #     if self.running:
    #         return
    #     if self.thread and self.thread.is_alive():
    #         return

    #     self.running = True

    #     self.ws = websocket.WebSocketApp(
    #         self.ws_url,
    #         on_open=self.on_open,
    #         on_message=self.on_message,
    #         on_error=self.on_error,
    #         on_close=self.on_close
    #     )

    #     self.thread = threading.Thread(
    #         target=lambda: self.ws.run_forever(
    #             ping_interval=20,
    #             ping_timeout=10
    #         ),
    #         daemon=True
    #     )
    #     self.thread.start()


        # self.ws.run_forever(ping_interval=20, ping_timeout=10)




def ts():
    return datetime.now().strftime("%H:%M:%S.%f")

# ==========================
# মাল্টি-ইউজার ম্যানেজার    
# ==========================
class MultiUserManager:
    def __init__(self):
        # self.users = []     # user_id → api info
        # self.users = {}   # GOOD: user_id -> user info
        # ✅ FINAL
        self.users: Dict[str, dict] = {}
        self.api_type_cache: Dict[str, str] = {}
        self.clients = {}   # user_id → WS client
        # self.lock = threading.Lock()
        self.db_ref = db.reference('/strategies')  # Firebase-এর strategies পাথ

        self._refreshing = set()
        # self._refresh_lock = threading.Lock()

        self._refresh_lock = Semaphore()


        
         # ✅ START PUBLIC WS HERE (ONE TIME)
        # self.public_ws = PublicWSClient(
        #     ws_url= "wss://socket.india.delta.exchange",
        #     symbols=["all"]
        # )
        # self.public_ws.start()

        self.public_ws_live = PublicWSClient(
        ws_url="wss://socket.india.delta.exchange",
        symbols=["all"]
        )

        self.public_ws_testnet = PublicWSClient(
        ws_url="wss://socket-ind.testnet.deltaex.org",
        symbols=["all"]
        )

        # self.public_ws_live.start()
        # self.public_ws_testnet.start()


        self._load_initial_users()
        # self._setup_firebase_listener()
        # self._setup_signal_handlers()

        self.api_url = {
            "TESTNET": "https://cdn-ind.testnet.deltaex.org",
            "LIVE": "https://api.india.delta.exchange"
        }


        # self.arbitrage_diff_threshold = 0.10   # 0.10%
        self.funding_arbitrage = []
        self.best_trade = None
        self.arbitrage_bot_loop_interval = 10  # seconds
        # self._arb_lock = threading.Lock()
        self._arb_lock = Semaphore()
        self._public_ws_lock = Semaphore()

        self._arb_thread = None
        self._arb_stop_event = threading.Event()

        


    def _setup_signal_handlers(self):
        """Ctrl+C এবং অন্যান্য টার্মিনেশন সিগন্যাল হ্যান্ডল করা।"""
        def signal_handler(sig, frame):
            logging.info("[ম্যানেজার] Ctrl+C সিগন্যাল পাওয়া গেছে, প্রোগ্রাম বন্ধ করা হচ্ছে...")
            print("[ম্যানেজার] Ctrl+C সিগন্যাল পাওয়া গেছে, প্রোগ্রাম বন্ধ করা হচ্ছে...")
            self.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # টার্মিনেশন সিগন্যাল



    def shutdown(self):
        logging.info("[ম্যানেজার] ক্লিনআপ শুরু হচ্ছে...")
        print("[ম্যানেজার] ক্লিনআপ শুরু হচ্ছে...")

        self.running = False

        # =========================
        # 1️⃣ Stop ALL USER WS
        # =========================
        with users_lock:
            for user_id, client_map in list(self.clients.items()):
                try:
                    delta = client_map.get("delta")
                    if delta:
                        # 🔥 stop auto reconnect
                        delta._reconnecting = False

                        if delta.ws:
                            delta.ws.close()

                        logging.info(f"[ম্যানেজার] User WS closed: {user_id}")
                except Exception as e:
                    logging.error(f"[ম্যানেজার] User WS close error {user_id}: {e}")

            self.clients.clear()
            self.users.clear()

        # =========================
        # 2️⃣ Clear in-memory data
        # =========================
        with data_lock:
            data_store.clear()

        # =========================
        # 3️⃣ STOP PUBLIC WS (VERY IMPORTANT)
        # =========================
        try:
            # 🔴 Tell public WS: DO NOT RECONNECT
            self.public_ws_live.running = False
            self.public_ws_testnet.running = False

            if self.public_ws_live.ws:
                self.public_ws_live.ws.close()

            if self.public_ws_testnet.ws:
                self.public_ws_testnet.ws.close()

            logging.info("[ম্যানেজার] Public WS stopped")

        except Exception as e:
            logging.error(f"[ম্যানেজার] Public WS close error: {e}")

        logging.info("[ম্যানেজার] ✅ Shutdown complete")
        print("[ম্যানেজার] ✅ Shutdown complete")






    # def shutdown(self):
    #     """Graceful shutdown of manager and all resources"""
    #     logging.info("[ম্যানেজার] ক্লিনআপ শুরু হচ্ছে...")
    #     print("[ম্যানেজার] ক্লিনআপ শুরু হচ্ছে...")

    #     self.running = False

    #     # 1️⃣ Stop all user WS clients
    #     with users_lock:
    #         for user_id, client in list(self.clients.items()):
    #             try:
    #                 client._reconnecting = False  # 🔥 stop auto reconnect
    #                 if client.ws:
    #                     client.ws.close()
    #                 logging.info(f"[ম্যানেজার] WebSocket বন্ধ করা হয়েছে: {user_id}")
    #             except Exception as e:
    #                 logging.error(
    #                     f"[ম্যানেজার] WebSocket বন্ধ করতে ত্রুটি: {user_id}, {e}"
    #                 )

    #         self.clients.clear()
    #         self.users.clear()

    #     # 2️⃣ Clear in-memory store
    #     with data_lock:
    #         data_store.clear()

    #     # 3️⃣ Stop public websockets
    #     try:
    #         if hasattr(self, "public_ws_live") and self.public_ws_live.ws:
    #             self.public_ws_live.ws.close()
    #         if hasattr(self, "public_ws_testnet") and self.public_ws_testnet.ws:
    #             self.public_ws_testnet.ws.close()
    #     except Exception as e:
    #         logging.error(f"[ম্যানেজার] Public WS close error: {e}")

    #     # 4️⃣ Stop Firebase listener
    #     try:
    #         if getattr(self, "listener", None):
    #             self.listener.close()  # if supported
    #             self.listener = None
    #     except Exception as e:
    #         logging.warning(f"[ম্যানেজার] Firebase listener close skipped: {e}")

    #     logging.info("[ম্যানেজার] ✅ Shutdown complete")
    #     print("[ম্যানেজার] ✅ Shutdown complete")

# /////////////////////////////////////////////////////////////////////


    # def shutdown(self):
    #     """প্রোগ্রাম বন্ধ করার জন্য ক্লিনআপ।"""
    #     self.running = False
    #     logging.info("[ম্যানেজার] ক্লিনআপ শুরু হচ্ছে...")
    #     print("[ম্যানেজার] ক্লিনআপ শুরু হচ্ছে...")

    #     # সব WebSocket ক্লায়েন্ট বন্ধ করা
    #     with users_lock:
    #         for user_id, client in list(self.clients.items()):
    #             try:
    #                 if client.ws:
    #                     client.ws.close()
    #                     logging.info(f"[ম্যানেজার] WebSocket বন্ধ করা হয়েছে: {user_id}")
    #                     print(f"[ম্যানেজার] WebSocket বন্ধ করা হয়েছে: {user_id}")
    #             except Exception as e:
    #                 logging.error(f"[ম্যানেজার] WebSocket বন্ধ করতে ত্রুটি: {user_id}, ত্রুটি: {e}")
    #             del self.clients[user_id]
    #         # self.users = {}
    #         self.users.clear()
    #         logging.info("[ম্যানেজার] সব ক্লায়েন্ট এবং ব্যবহারকারী তালিকা পরিষ্কার করা হয়েছে।")
    #         print("[ম্যানেজার] সব ক্লায়েন্ট এবং ব্যবহারকারী তালিকা পরিষ্কার করা হয়েছে।")

    #     # Firebase লিসেনার বন্ধ করা
    #     if self.listener:
    #         try:
    #             # pyrebase-এর জন্য listener বন্ধ করার মেথড নেই, তাই ম্যানুয়ালি হ্যান্ডল করা
    #             self.db_ref = None
    #             logging.info("[ম্যানেজার] Firebase লিসেনার বন্ধ করা হয়েছে।")
    #             print("[ম্যানেজার] Firebase লিসেনার বন্ধ করা হয়েছে।")
    #         except Exception as e:
    #             logging.error(f"[ম্যানেজার] Firebase লিসেনার বন্ধ করতে ত্রুটি: {e}")








    # def _load_initial_users(self):
    #     """Firebase থেকে প্রাথমিক ব্যবহারকারীদের লোড করা।"""
    #     with users_lock:
    #         strategies_data = self.db_ref.get()
    #         if strategies_data:
    #             self.users = []
    #             for user_id, data in enumerate(strategies_data):
    #                 print("data :",data)
    #                 delta_data = data.get('database', {}).get('delta', {})
    #                 if 'api_key' in delta_data and 'api_secret' in delta_data:
    #                     user = {
    #                         "user_id": user_id,
    #                         "api_key": delta_data["api_key"],
    #                         "api_secret": delta_data["api_secret"]
    #                     }
    #                     self.users.append(user)
    #                     self.clients[user_id] = UserWSClient(user_id, user["api_key"], user["api_secret"])
    #             print(f"[ম্যানেজার] প্রাথমিকভাবে {len(self.users)} জন ব্যবহারকারী লোড করা হয়েছে।")
    #         else:
    #             print("[ম্যানেজার] Firebase থেকে কোনো ব্যবহারকারী পাওয়া যায়নি।")

    # def _load_initial_users(self):
    #     """Firebase থেকে প্রাথমিক ব্যবহারকারীদের লোড করা।"""
    #     with users_lock:
    #         strategies_data = self.db_ref.get()
    #         logging.info(f"[ম্যানেজার] Firebase থেকে প্রাপ্ত ডেটা: {strategies_data}")
    #         self.users = {}
    #         if strategies_data is None:
    #             logging.info("[ম্যানেজার] Firebase থেকে কোনো ডেটা পাওয়া যায়নি।")
    #             return

    #         try:
    #             # ডিকশনারি হলে
    #             if isinstance(strategies_data, dict):
    #                 for user_id, data in strategies_data.items():
    #                     if data is None or not isinstance(data, dict):
    #                         logging.warning(f"[ম্যানেজার] user_id {user_id} এ অবৈধ ডেটা: {data}")
    #                         continue
    #                     delta_data = data.get('database', {}).get('delta', {})
    #                     if not isinstance(delta_data, dict) or not delta_data.get('api_key') or not delta_data.get('api_secret'):
    #                         logging.warning(f"[ম্যানেজার] অবৈধ ডেটা user_id {user_id}: api_key বা api_secret অনুপস্থিত")
    #                         continue
    #                     user = {
    #                         "user_id": str(user_id),  # user_id সবসময় স্ট্রিং হিসেবে
    #                         "api_key": delta_data["api_key"],
    #                         "api_secret": delta_data["api_secret"]
    #                     }
    #                     if self._validate_api_key(user):
    #                         # self.users.append(user)
    #                         self.users[user_id] = user
    #                         uid = str(user_id)  # force string
    #                         ws_url = self.get_ws_url_from_firebase(user_id)
    #                         self.clients[uid] = UserWSClient(user_id, user["api_key"], user["api_secret"],ws_url=ws_url,manager=self)
    #                         logging.info(f"[ম্যানেজার] ব্যবহারকারী লোড করা হয়েছে: {user_id}")
    #                     else:
    #                         logging.warning(f"[ম্যানেজার] অবৈধ API কী/সিক্রেট: {user_id}")
    #             # লিস্ট হলে
    #             elif isinstance(strategies_data, list):
    #                 for index, data in enumerate(strategies_data):
    #                     if data is None or not isinstance(data, dict):
    #                         logging.info(f"[ম্যানেজার] index {index} এ null বা অবৈধ ডেটা: {data}")
    #                         continue
    #                     delta_data = data.get('database', {}).get('delta', {})
    #                     if not isinstance(delta_data, dict) or not delta_data.get('api_key') or not delta_data.get('api_secret'):
    #                         logging.warning(f"[ম্যানেজার] অবৈধ ডেটা index {index}: api_key বা api_secret অনুপস্থিত")
    #                         continue
    #                     user_id = str(index)  # ইনডেক্সকে user_id হিসেবে
    #                     user = {
    #                         "user_id": user_id,
    #                         "api_key": delta_data["api_key"],
    #                         "api_secret": delta_data["api_secret"]
    #                     }
    #                     if self._validate_api_key(user):
    #                         # self.users.append(user)
    #                         self.users[user_id] = user
    #                         ws_url = self.get_ws_url_from_firebase(user_id)
    #                         self.clients[user_id] = UserWSClient(user_id, user["api_key"], user["api_secret"],ws_url=ws_url,manager=self)
    #                         logging.info(f"[ম্যানেজার] ব্যবহারকারী লোড করা হয়েছে: {user_id}")
    #                     else:
    #                         logging.warning(f"[ম্যানেজার] অবৈধ API কী/সিক্রেট: {user_id}")
    #             else:
    #                 logging.error(f"[ম্যানেজার] Firebase থেকে অপ্রত্যাশিত ডেটা ফরম্যাট: {type(strategies_data)}")
    #         except Exception as e:
    #             logging.error(f"[ম্যানেজার] ডেটা লোড করতে ত্রুটি: {e}")
    #         logging.info(f"[ম্যানেজার] প্রাথমিকভাবে {len(self.users)} জন ব্যবহারকারী লোড করা হয়েছে।")



    def _load_initial_users(self):
        """Firebase থেকে প্রাথমিক ব্যবহারকারীদের লোড করা।"""

        with users_lock:
            strategies_data = self.db_ref.get()
            self.users.clear()
            self.clients.clear()

            if not strategies_data:
                logging.info("[ম্যানেজার] Firebase empty")
                return

            try:
                if isinstance(strategies_data, dict):
                    iterator = strategies_data.items()
                elif isinstance(strategies_data, list):
                    iterator = ((str(i), v) for i, v in enumerate(strategies_data))
                else:
                    logging.error(f"[ম্যানেজার] Invalid Firebase data type")
                    return

                for user_id, data in iterator:
                    if not isinstance(data, dict):
                        continue

                    user_id = str(user_id)
                    db_data = data.get("database", {})

                    # ======================
                    # 🔵 DELTA CONFIG
                    # ======================

                    delta = db_data.get("delta", {})
                    if not delta.get("api_key") or not delta.get("api_secret"):
                        continue

                    user = {
                        "user_id": user_id,
                        "api_key": delta["api_key"],
                        "api_secret": delta["api_secret"]
                    }

                    if not self._validate_api_key(user):
                        continue

                    self.users[user_id] = user

                    # with data_lock:
                    #     data_store[user_id] = {
                    #         "positions": {},
                    #         "orders": {},
                    #         "margins": {}
                    #     }
                    with data_lock:
                        store = data_store[user_id]   # defaultdict auto-create
                        store["positions"].clear()
                        store["orders"].clear()
                        store["margins"].clear()
    

                    # self.clients[user_id] = {}
                    self.clients[user_id] = {
                        "delta": None,
                        "dcx": None
                    }



                    ws_url = self.get_ws_url_from_firebase(user_id)
                    self.clients[user_id]["delta"] = UserWSClient(
                        user_id,
                        user["api_key"],
                        user["api_secret"],
                        ws_url=ws_url,
                        manager=self
                    )

                    # ======================
                    # 🟢 DCX CONFIG (OPTIONAL)
                    # ======================
                    dcx = db_data.get("dcx", {})
                    if dcx.get("api_key") and dcx.get("api_secret"):
                        self.clients[user_id]["dcx"] = DcxRestClient(
                            dcx["api_key"],
                            dcx["api_secret"]
                        )
                        
                        logging.info(f"[ম্যানেজার] DCX enabled for user {user_id}")
                    else:
                        self.clients[user_id]["dcx"] = None

                    logging.info(f"[ম্যানেজার] Loaded user {user_id}")

                logging.info(f"[ম্যানেজার] Initial users loaded: {len(self.users)}")

            except Exception as e:
                logging.error(f"[ম্যানেজার] Load error: {e}")






    def _setup_firebase_listener(self):
        """Firebase-এ রিয়েলটাইম পরিবর্তন শনাক্ত করার জন্য লিসেনার সেটআপ।"""
        def listener(event):
            print(f"[Firebase] ইভেন্ট: {event.event_type}, পাথ: {event.path}, ডেটা: {event.data}")
            path_parts = event.path.strip("/").split("/")
            user_id = path_parts[0] if path_parts else None

            if event.event_type in ["put", "patch"]:
                if user_id and len(path_parts) > 1 and path_parts[1:] == ["database", "delta"]:
                    # নির্দিষ্ট user_id-এর delta ডেটা আপডেট
                    if event.data and 'api_key' in event.data and 'api_secret' in event.data:
                        user = {
                            "user_id": user_id,
                            "api_key": event.data["api_key"],
                            "api_secret": event.data["api_secret"]
                        }
                        self.add_user(user)
                elif event.path == "/" and event.data:
                    # পুরো strategies তালিকা আপডেট
                    with users_lock:
                        self._sync_users(event.data)
            elif event.event_type == "delete" and user_id:
                # ব্যবহারকারী অপসারণ
                self.remove_user(user_id)

        self.db_ref.listen(listener)
        print("[ম্যানেজার] Firebase রিয়েলটাইম লিসেনার শুরু হয়েছে।")

    
    def get_asset_id(self, symbol,url):
        """
         Delta Exchange asset list থেকে symbol অনুযায়ী asset_id রিটার্ন করবে
        """
        try:
            rest = DeltaRestClient(
            api_key="",
            api_secret="",
            base_url=url
            )

            assets = rest.get_assets()

            for asset in assets:
                if asset.get("symbol") == symbol.upper():
                    return int(asset.get("id"))

            return None

        except Exception as e:
            print("❌ Asset Fetch Error:", e)
            return None




    # def check_api_type(self, api_key, api_secret, user_id):
    #     if not api_key or not api_secret:
    #         return "INVALID_KEY"
    #     cache_key = f"{api_key}:{api_secret}"

    #     # 1️⃣ MEMORY CACHE
    #     if cache_key in self.api_type_cache:
    #         return self.api_type_cache[cache_key]

    #     # 2️⃣ FIREBASE CACHE
    #     data = self.db_ref.child(str(user_id)).get() or {}
    #     delta = data.get("database", {}).get("delta", {})
    #     fb_type = delta.get("type")

    #     if fb_type in ("LIVE", "TESTNET"):
    #         self.api_type_cache[cache_key] = fb_type
    #         return fb_type

    #     # 3️⃣ REAL CHECK (ONLY ONCE)
    #     urls = {
    #         "TESTNET": "https://cdn-ind.testnet.deltaex.org",
    #         "LIVE": "https://api.india.delta.exchange"
    #     }

    #     detected_type = None

    #     for api_type, url in urls.items():
    #         try:
    #             rest = DeltaRestClient(
    #                 api_key=api_key,
    #                 api_secret=api_secret,
    #                 base_url=url
    #             )

    #             asset_id = self.get_asset_id("USD", url)
    #             rest.get_balances(asset_id=asset_id)

    #             detected_type = api_type
    #             break

    #         except:
    #             continue

    #     if not detected_type:
    #         return "INVALID_KEY"

    #     # 4️⃣ SAVE BOTH CACHE + FIREBASE
    #     self.api_type_cache[cache_key] = detected_type

    #     self.db_ref.child(str(user_id)).child("database").child("delta").update({
    #         "type": detected_type
    #     })

    #     return detected_type



    def check_api_type(self, api_key, api_secret, user_id, force=False):
        cache_key = f"{api_key}:{api_secret}"

        if not force and cache_key in self.api_type_cache:
            return self.api_type_cache[cache_key]

        urls = {
            "TESTNET": "https://cdn-ind.testnet.deltaex.org",
            "LIVE": "https://api.india.delta.exchange"
        }

        detected_type = None

        for api_type, url in urls.items():
            try:
                rest = DeltaRestClient(
                    api_key=api_key,
                    api_secret=api_secret,
                    base_url=url
                )

                asset_id = self.get_asset_id("USD", url)
                rest.get_balances(asset_id=asset_id)

                detected_type = api_type
                break
            except Exception as e:
                logging.warning(f"[API CHECK] {api_type} failed: {e}")


        if not detected_type:
            return "INVALID_KEY"

        # save cache + firebase
        self.api_type_cache[cache_key] = detected_type
        self.db_ref.child(str(user_id)).child("database").child("delta").update({
            "type": detected_type
        })

        return detected_type







    

    # def check_api_type(self, api_key, api_secret, user_id):
    #     print("Checking API key type...")
    #     print("API Key:", api_key)
    #     print("API Secret:", api_secret)

    #     urls = {
    #     "testnet": "https://cdn-ind.testnet.deltaex.org",
    #     "live": "https://api.india.delta.exchange"
    #     }

    #     result = {}

    #     for name, url in urls.items():
    #         try:
    #             rest = DeltaRestClient(
    #             api_key=api_key,
    #             api_secret=api_secret,
    #             base_url=url
    #             )

    #             # USD asset_id fetch
    #             asset_id = self.get_asset_id("USD",url)

    #             res = rest.get_balances(asset_id=asset_id)

    #             # যদি response dict → valid
    #             result[name] = isinstance(res, dict)

    #         except Exception as e:
    #             print(f"[{name}] Error:", e)
    #             result[name] = False

    # ---------------------------
    # DEMO KEY (Works only on testnet)
    # ---------------------------
        if result["testnet"] and not result["live"]:
            ref = db.reference(f"/strategies/{user_id}/database/delta")
            ref.update({"type": "TESTNET"})
            print(f"Updated Firebase: user {user_id} → demo")
            return "TESTNET"

    # ---------------------------
    # REAL KEY (Works only on live)
    # ---------------------------
        if result["live"] and not result["testnet"]:
            ref = db.reference(f"/strategies/{user_id}/database/delta")
            ref.update({"type": "LIVE"})
            print(f"Updated Firebase: user {user_id} → real")
            return "LIVE"

    # ---------------------------
    # INVALID KEY
    # ---------------------------
        return "INVALID_KEY"


    # def _validate_api_key(self, user):
    #     api_key = user["api_key"]
    #     api_secret = user["api_secret"]
    #     user_id = user["user_id"]

    #     api_type = self.check_api_type(api_key, api_secret, user_id)

    #     if api_type == "INVALID_KEY":
    #         print(f"[{user_id}] ❌ Invalid API Key")
    #         return False

    #     print(f"[{user_id}] ✅ API Key Valid → {api_type}")
    #     return True


    def _validate_api_key(self, user):
        api_type = self.check_api_type(
            user["api_key"],
            user["api_secret"],
            user["user_id"]
        )

        if api_type == "INVALID_KEY":
            return False

        return True



    # def _validate_api_key(self, user):

    # """API কী এবং সিক্রেট বৈধ কিনা চেক করা।"""
    # try:
    #     response = get_api_data(
    #         "/v2/positions/margined",
    #         query=f"user_id={user['user_id']}",
    #         api_key=user['api_key'],
    #         api_secret=user['api_secret']
    #     )
    #     if response is not None:
    #         logging.info(f"[ম্যানেজার] API কী ভ্যালিডেশন সফল: {user['user_id']}")
        # return True
        # else:
        #     logging.warning(f"[ম্যানেজার] API কী ভ্যালিডেশন ব্যর্থ: {user['user_id']}, কোনো রেসপন্স নেই")
        #     return False
    # except Exception as e:
    #     logging.error(f"[ম্যানেজার] API কী ভ্যালিডেশন ব্যর্থ: {user['user_id']}, ত্রুটি: {e}")
    #     return False    

    # def _sync_users(self, strategies_data):
    #     """Firebase থেকে পুরো strategies তালিকা সিঙ্ক্রোনাইজ করা।"""
    #     new_users = []
    #     for user_id, data in enumerate(strategies_data):
    #         delta_data = data.get('database', {}).get('delta', {})
    #         if 'api_key' in delta_data and 'api_secret' in delta_data:
    #             new_users.append({
    #                 "user_id": user_id,
    #                 "api_key": delta_data["api_key"],
    #                 "api_secret": delta_data["api_secret"]
    #             })

    #     with users_lock:
    #         # বিদ্যমান ব্যবহারকারীদের তুলনা করে নতুন/অপসারিত ব্যবহারকারী শনাক্ত
    #         current_user_ids = {u["user_id"] for u in self.users}
    #         new_user_ids = {u["user_id"] for u in new_users}

    #         # নতুন ব্যবহারকারী যোগ
    #         for user in new_users:
    #             if user["user_id"] not in current_user_ids:
    #                 self.add_user(user)

    #         # অপসারিত ব্যবহারকারী মুছে ফেলা
    #         for user_id in current_user_ids - new_user_ids:
    #             self.remove_user(user_id)

    #         self.users = new_users
    #         print(f"[ম্যানেজার] ব্যবহারকারী তালিকা সিঙ্ক্রোনাইজ করা হয়েছে: {len(self.users)} জন ব্যবহারকারী।")



    # def _sync_users(self, strategies_data):
    #     """Firebase থেকে পুরো strategies তালিকা সিঙ্ক্রোনাইজ করা।"""
    #     new_users = []
    #     with users_lock:
    #         try:
    #             if isinstance(strategies_data, dict):
    #                 for user_id, data in strategies_data.items():
    #                     if data is None or not isinstance(data, dict):
    #                         logging.warning(f"[ম্যানেজার] user_id {user_id} এ অবৈধ ডেটা: {data}")
    #                         continue
    #                     delta_data = data.get('database', {}).get('delta', {})
    #                     if not isinstance(delta_data, dict) or not delta_data.get('api_key') or not delta_data.get('api_secret'):
    #                         logging.warning(f"[ম্যানেজার] অবৈধ ডেটা user_id {user_id}: api_key বা api_secret অনুপস্থিত")
    #                         continue
    #                     user = {
    #                         "user_id": str(user_id),
    #                         "api_key": delta_data["api_key"],
    #                         "api_secret": delta_data["api_secret"]
    #                     }
    #                     if self._validate_api_key(user):
    #                         new_users.append(user)
    #                     else:
    #                         logging.warning(f"[ম্যানেজার] অবৈধ API কী/সিক্রেট: {user_id}")
    #             elif isinstance(strategies_data, list):
    #                 for index, data in enumerate(strategies_data):
    #                     if data is None or not isinstance(data, dict):
    #                         logging.info(f"[ম্যানেজার] index {index} এ null বা অবৈধ ডেটা: {data}")
    #                         continue
    #                     delta_data = data.get('database', {}).get('delta', {})
    #                     if not isinstance(delta_data, dict) or not delta_data.get('api_key') or not delta_data.get('api_secret'):
    #                         logging.warning(f"[ম্যানেজার] অবৈধ ডেটা index {index}: api_key বা api_secret অনুপস্থিত")
    #                         continue
    #                     user_id = str(index)
    #                     user = {
    #                         "user_id": user_id,
    #                         "api_key": delta_data["api_key"],
    #                         "api_secret": delta_data["api_secret"]
    #                     }
    #                     if self._validate_api_key(user):
    #                         new_users.append(user)
    #                     else:
    #                         logging.warning(f"[ম্যানেজার] অবৈধ API কী/সিক্রেট: {user_id}")
    #             else:
    #                 logging.error(f"[ম্যানেজার] Firebase থেকে অপ্রত্যাশিত ডেটা ফরম্যাট: {type(strategies_data)}")
    #                 return

    #             # বিদ্যমান এবং নতুন ব্যবহারকারীদের তুলনা
    #             current_user_ids = {u["user_id"] for u in self.users}
    #             new_user_ids = {u["user_id"] for u in new_users}

    #             # নতুন ব্যবহারকারী যোগ
    #             for user in new_users:
    #                 if user["user_id"] not in current_user_ids:
    #                     self.add_user(user)

    #             # অপসারিত ব্যবহারকারী মুছে ফেলা
    #             for user_id in current_user_ids - new_user_ids:
    #                 self.remove_user(user_id)

    #             self.users = new_users
    #             logging.info(f"[ম্যানেজার] ব্যবহারকারী তালিকা সিঙ্ক্রোনাইজ করা হয়েছে: {len(self.users)} জন ব্যবহারকারী।")
    #         except Exception as e:
    #             logging.error(f"[ম্যানেজার] ডেটা সিঙ্ক্রোনাইজেশনে ত্রুটি: {e}")



    def _sync_users(self, strategies_data):
        """Firebase থেকে পুরো strategies তালিকা সিঙ্ক্রোনাইজ করা (DICT based)।"""

        new_users: dict[str, dict] = {}

        with users_lock:
            try:
                if isinstance(strategies_data, dict):
                    iterator = strategies_data.items()
                elif isinstance(strategies_data, list):
                    iterator = ((str(i), v) for i, v in enumerate(strategies_data))
                else:
                    logging.error(f"[ম্যানেজার] অপ্রত্যাশিত Firebase ডেটা: {type(strategies_data)}")
                    return

                for user_id, data in iterator:
                    if not isinstance(data, dict):
                        continue

                    delta = data.get("database", {}).get("delta", {})
                    # if not delta.get("api_key") or not delta.get("api_secret"):
                    #     continue
                    if not delta.get("api_key") or not delta.get("api_secret"):
                        if user_id in self.users:
                            self.remove_user(user_id)
                            continue


                    user_id = str(user_id)
                    user = {
                        "user_id": user_id,
                        "api_key": delta["api_key"],
                        "api_secret": delta["api_secret"]
                    }

                    if self._validate_api_key(user):
                        new_users[user_id] = user

                current_ids = set(self.users.keys())
                new_ids = set(new_users.keys())

                # ➕ ADD
                for uid in new_ids - current_ids:
                    self.add_user(new_users[uid])

                # ➖ REMOVE
                for uid in current_ids - new_ids:
                    self.remove_user(uid)

                logging.info(f"[ম্যানেজার] Sync complete → {len(new_users)} users")

            except Exception as e:
                logging.error(f"[ম্যানেজার] Sync error: {e}")




    # def start_all(self):
    #     with users_lock:
    #         for c in self.clients.values():
    #             c.start()

    def start_all(self):
        with users_lock:
            for client_map in self.clients.values():
                delta = client_map.get("delta")
                if delta:
                    delta.start()




    # def add_user(self, user):
    #     """
    # user = {
    #     "user_id": str,
    #     "api_key": str,
    #     "api_secret": str
    # }
    # """
    #     user_id = str(user["user_id"])

    #     with users_lock:
    #         if user_id in self.users:
    #             logging.info(f"[ম্যানেজার] ব্যবহারকারী {user_id} ইতিমধ্যে আছে")
    #             return False

    #         if user_id in self.clients:
    #             logging.info(f"[ম্যানেজার] ব্যবহারকারী {user_id} ইতিমধ্যে আছে")
    #             return False

    #         # ✅ save user
    #         self.users[user_id] = user

    #         # ✅ create WS client
    #         ws_url = self.get_ws_url_from_firebase(user_id)
    #         client = UserWSClient(
    #             user_id,
    #             user["api_key"],
    #             user["api_secret"],
    #             ws_url=ws_url,
    #             manager=self
    #         )

    #         self.clients[user_id] = {
    #             "delta": client,
    #             "dcx": None
    #         }
    #         client.start()

    #         logging.info(f"[ম্যানেজার] ব্যবহারকারী যোগ করা হয়েছে: {user_id}")
    #         return True


    def add_user(self, user):
        user_id = str(user["user_id"])

        with users_lock:
            if user_id in self.users:
                return False

            self.users[user_id] = user

            # with data_lock:
            #     data_store[user_id] = {
            #         "positions": {},
            #         "orders": {},
            #         "margins": {}
            #     }

            with data_lock:
                store = data_store[user_id]
                store["positions"].clear()
                store["orders"].clear()
                store["margins"].clear()

                # ---- DELTA ----
            ws_url = self.get_ws_url_from_firebase(user_id)
            delta_client = UserWSClient(
                user_id,
                user["api_key"],
                user["api_secret"],
                ws_url=ws_url,
                manager=self
            )

            # ---- DCX ----
            data = self.db_ref.child(user_id).get() or {}
            dcx_cfg = data.get("database", {}).get("dcx", {})

            dcx_client = None
            if dcx_cfg.get("api_key") and dcx_cfg.get("api_secret"):
                dcx_client = DcxRestClient(
                    dcx_cfg["api_key"],
                    dcx_cfg["api_secret"]
                )

            self.clients[user_id] = {
                "delta": delta_client,
                "dcx": dcx_client
            }

            delta_client.start()
            logging.info(f"[ম্যানেজার] User added: {user_id}")
            return True



    def remove_user(self, user_id):
        user_id = str(user_id)

        with users_lock:
            client_map = self.clients.get(user_id)
            if not client_map:
                return False

            # 🔴 Stop Delta WS
            delta = client_map.get("delta")
            try:
                if delta:
                    delta._reconnecting = False
                    if delta.ws:
                        delta.ws.close()
            except Exception as e:
                logging.warning(f"[ম্যানেজার] WS close error {user_id}: {e}")

            # 🧹 Remove structures
            self.clients.pop(user_id, None)
            self.users.pop(user_id, None)

            with data_lock:
                data_store.pop(user_id, None)

            logging.info(f"[ম্যানেজার] User removed: {user_id}")
            return True







    # def remove_user(self, user_id):
    #     user_id = str(user_id)

    #     with users_lock:
    #         user = self.users.get(user_id)
    #         if user:
    #             cache_key = f"{user['api_key']}:{user['api_secret']}"
    #             self.api_type_cache.pop(cache_key, None)

    #         if user_id not in self.clients:
    #             logging.info(f"[ম্যানেজার] ব্যবহারকারী {user_id} পাওয়া যায়নি")
    #             return False

    #         # 🔴 Stop WS
    #         client = self.clients[user_id]["delta"]
    #         try:
    #             client._reconnecting = False
    #             if client.ws:
    #                 client.ws.close()
    #         except Exception as e:
    #             logging.warning(f"[ম্যানেজার] WS close error {user_id}: {e}")

    #         # 🧹 Remove client
    #         self.clients.pop(user_id, None)

    #         # 🧹 Remove user
    #         self.users.pop(user_id, None)

    #         # 🧹 Clear data_store
    #         with data_lock:
    #             data_store.pop(user_id, None)

    #         logging.info(f"[ম্যানেজার] ব্যবহারকারী অপসারিত: {user_id}")
    #         return True





    # def add_user(self, user):
    #     """
    #     গতিশীলভাবে নতুন ব্যবহারকারী যোগ করে এবং তাদের ওয়েবসকেট সংযোগ শুরু করে।
    #     user: 'user_id', 'api_key', 'api_secret' কী সহ ডিকশনারি
    #     """
    #     with users_lock:
    #         # ব্যবহারকারী ইতিমধ্যে আছে কিনা চেক করুন
    #         if any(u["user_id"] == user["user_id"] for u in self.users):
    #             logging.info(f"[ম্যানেজার] ব্যবহারকারী {user['user_id']} ইতিমধ্যে আছে, এড়িয়ে যাচ্ছি।")
    #             return False

    #         # USERS তালিকায় যোগ করুন
    #         self.users.append(user)
    #         # নতুন ওয়েবসকেট ক্লায়েন্ট তৈরি এবং শুরু
    #         ws_url = self.get_ws_url_from_firebase(user["user_id"])
    #         client = UserWSClient(user["user_id"], user["api_key"], user["api_secret"],ws_url=ws_url,manager=self)
    #         self.clients[user["user_id"]] = client
    #         client.start()
    #         logging.info(f"[ম্যানেজার] ব্যবহারকারী যোগ এবং ওয়েবসকেট শুরু: {user['user_id']}")
    #         return True

    # def remove_user(self, user_id):
    #     """
    #     গতিশীলভাবে ব্যবহারকারী অপসারণ করে এবং তাদের ওয়েবসকেট সংযোগ বন্ধ করে।
    #     """
    #     with users_lock:
    #         # ব্যবহারকারী আছে কিনা চেক করুন
    #         if user_id not in self.clients:
    #             print(f"[ম্যানেজার] ব্যবহারকারী {user_id} পাওয়া যায়নি, অপসারণ সম্ভব নয়।")
    #             logging.info(f"[ম্যানেজার] ব্যবহারকারী {user_id} পাওয়া যায়নি, অপসারণ সম্ভব নয়।")
    #             return False

    #         # ওয়েবসকেট সংযোগ বন্ধ করুন
    #         client = self.clients[user_id]
    #         if client.ws:
    #             try:
    #                 client.ws.close()
    #                 logging.info(f"[ম্যানেজার] ব্যবহারকারীর জন্য ওয়েবসকেট বন্ধ: {user_id}")
    #             except Exception as e:
    #                 logging.info(f"[ম্যানেজার] {user_id} এর ওয়েবসকেট বন্ধ করতে ত্রুটি: {e}")

    #         # ক্লায়েন্ট এবং USERS থেকে অপসারণ
    #         del self.clients[user_id]
    #         self.users[:] = [u for u in self.users if u["user_id"] != user_id]

    #         # data_store থেকে ব্যবহারকারীর ডেটা মুছে ফেলুন
    #         with data_lock:
    #             if user_id in data_store:
    #                 del data_store[user_id]
    #                 logging.info(f"[ম্যানেজার] ব্যবহারকারীর জন্য data_store মুছে ফেলা হয়েছে: {user_id}")

    #         logging.info(f"[ম্যানেজার] ব্যবহারকারী অপসারিত: {user_id}")
    #         return True

    def get_user_data(self, user_id):
        user_id = str(user_id)
        with data_lock:
            if user_id not in data_store:
                return None
            return data_store[user_id]

        

    def getClintdata(self,user_id):
        # return self.clients[str(user_id)]
        return len(self.clients)
    
    def getnewClintid(self):
        # return self.clients[str(user_id)]
        return len(self.clients)+1
    

    
    
    def place_order(self, user_id, data):
        """
        নির্দিষ্ট user এর API key দিয়ে অর্ডার প্লেস করবে
        """
        # time_ = datetime.now().strftime("%H:%M:%S.%f")
        print(f"[{user_id}] place_order :{ts()}")

        if user_id not in self.clients:
            print(f"[ম্যানেজার] ❌ ব্যবহারকারী {user_id} পাওয়া যায়নি")
            return {"error": "user_not_found"}

        
        client = self.clients[user_id]["delta"]

        

        # Particular user-এর api key / secret
        api_key = client.api_key
        api_secret = client.api_secret
        base_url = self.get_rest_url_from_firebase(user_id)   # LIVE

        print(f"[{user_id}] 🔑 Using API Key: {api_key} : {ts()}")

        try:
            rest = DeltaRestClient(
                api_key=api_key,
                api_secret=api_secret,
                base_url=base_url
            )   

            # during user load
            # self.clients[user_id]["delta_rest"] = DeltaRestClient(
            #     api_key=api_key,
            #     api_secret=api_secret,
            #     base_url=base_url
            # )

            # rest = self.clients[user_id]["delta_rest"]
            # response = rest.place_order(**data)


            print(f"[{user_id}] 📤 Sending Order → {data} : {ts()}")

            # 🔥 FIX HERE
            if "product_id" in data:
                data["product_id"] = int(data["product_id"])

            # Order execute
            response = rest.place_order(**data)

            print(f"[{user_id}] ✅ Order Success → {response} :: {ts()}")
            logging.info("[%s] Order success → %s", user_id, response.get("id"))

            # Success condition: order has an ID
            # SUCCESS IF order_id exists
            is_success = bool(response.get("id")) or bool(response.get("state"))
                                                        #    in ["open", "filled", "closed"])

            return {
                "success": is_success,
                "order": response
            }
            # return response

        except Exception as e:
            print(f"[{user_id}] ❌ Order Error → {e}")
            logging.error("[%s] Order error", user_id, exc_info=True)
            # return {"error": str(e)}
            return {
                "success": False,
                "error": str(e)
                }



    def smart_close(self, user_id, symbol):
        """
        Smart close:
        1) যদি symbol এর active order থাকে → cancel order
        2) যদি active position থাকে → reduce-only close
        """
        user_id = str(user_id)

        # 🔐 SAFE READ SNAPSHOT
        with data_lock:
            user_data = data_store.get(user_id)
            if not user_data:
                return {"success": False, "error": "user_not_found"}

            orders = list(user_data["orders"].values())   # ✅ snapshot
            pos = user_data["positions"].get(symbol)     # ✅ snapshot

        if user_id not in self.clients:
            return {"success": False, "error": "user_not_found"}

        client = self.clients[user_id]["delta"]

        # =========================
        # STEP 1: ACTIVE ORDER?
        # =========================
        active_order = None
        for order in orders:
            if (
                order.get("product_symbol") == symbol
                and order.get("state") not in ["cancelled", "closed"]
            ):
                active_order = order
                break

        if active_order:
            order_id = active_order.get("id")
            product_id = active_order.get("product_id")
            print(f"[{user_id}] 🟥 Active order detected → Canceling {order_id}")

            try:
                rest = DeltaRestClient(
                    api_key=client.api_key,
                    api_secret=client.api_secret,
                    base_url=self.get_rest_url_from_firebase(user_id)
                )

                response = rest.cancel_order(
                    order_id=order_id,
                    product_id=product_id
                )

                success = response.get("state") in ["cancelled", "closed"]
                return {
                    "success": success,
                    "type": "order_cancel",
                    "order": response
                }

            except Exception as e:
                print(f"[{user_id}] ❌ Cancel Error → {e}")
                return {"success": False, "error": str(e)}

        # =========================
        # STEP 2: ACTIVE POSITION?
        # =========================
        if pos:
            product_id = pos["product_id"]
            size = abs(float(pos["size"]))
            side = "sell" if float(pos["size"]) > 0 else "buy"

            order_data = {
                "product_id": product_id,
                "size": size,
                "side": side,
                "order_type": OrderType.MARKET,
                "reduce_only": True,
                "time_in_force": TimeInForce.IOC
            }

            print(f"[{user_id}] 🔻 Closing position {symbol}")

            try:
                rest = DeltaRestClient(
                    api_key=client.api_key,
                    api_secret=client.api_secret,
                    base_url=self.get_rest_url_from_firebase(user_id)
                )

                response = rest.place_order(**order_data)

                return {
                    "success": True,
                    "type": "position_close",
                    "order": response
                }

            except Exception as e:
                print(f"[{user_id}] ❌ Position Close Error → {e}")
                return {"success": False, "error": str(e)}

        # =========================
        # NOTHING TO CLOSE
        # =========================
        print(f"[{user_id}] ⚠ Nothing to close for {symbol}")
        return {
            "success": False,
            "error": "no_active_order_or_position"
        }








    # def smart_close(self, user_id, symbol):
    #     """
    #     Smart close:
    #     1) যদি symbol এর active order থাকে → cancel order
    #     2) যদি active position থাকে → reduce-only close
    #     """
    #     user_data = data_store.get(user_id)
    #     if not user_data:
    #         return {"success": False, "error": "user_not_found"}

    #     if user_id not in self.clients:
    #         return {"success": False, "error": "user_not_found"}

    #     client = self.clients[user_id]["delta"]

    #     # -------- STEP 1: CHECK ACTIVE ORDER --------
    #     active_order = None

    #     for oid, order in data_store[user_id]["orders"].items():
    #         if order.get("product_symbol") == symbol and order.get("state") not in ["cancelled", "closed"]:
    #             active_order = order
    #             break

    #     if active_order:
    #         order_id = active_order.get("id")
    #         product_id = active_order.get("product_id")
    #         print(f"[{user_id}] 🟥 Active order detected → Canceling order {order_id}")

    #         try:
    #             rest = DeltaRestClient(
    #             api_key=client.api_key,
    #             api_secret=client.api_secret,
    #             # base_url="https://cdn-ind.testnet.deltaex.org"
    #             base_url = self.get_rest_url_from_firebase(user_id)   # LIVE
    #             )

    #             # response = rest.cancel_order(order_id=order_id)
    #             response = rest.cancel_order(order_id=order_id, product_id=product_id)
    #             print(f"[{user_id}] 🟦 Cancel Response → {response}")

    #             success = response.get("state") in ["cancelled", "closed"]
    #             return {
    #             "success": success,
    #             "type": "order_cancel",
    #             "order": response
    #             }

    #         except Exception as e:
    #             print(f"[{user_id}] ❌ Cancel Error → {e}")
    #             return {"success": False, "error": str(e)}


    # # -------- STEP 2: CHECK ACTIVE POSITION --------
    #     pos = data_store[user_id]["positions"].get(symbol)
    #     print("pos :",pos)
    #     print("positions : ",data_store[user_id]["positions"])

    #     if pos:
    #         product_id = pos["product_id"]
    #         size = abs(float(pos["size"]))
    #         side = "sell" if float(pos["size"]) > 0 else "buy"

    #         order_data = {
    #         "product_id": product_id,
    #         "size": size,
    #         "side": side,
    #         "order_type": OrderType.MARKET,
    #         "reduce_only": True,
    #         "time_in_force": TimeInForce.IOC
    #         }

    #         print(f"[{user_id}] 🔻 Closing Position → {symbol} → {order_data}")

    #         try:
    #             rest = DeltaRestClient(
    #             api_key=client.api_key,
    #             api_secret=client.api_secret,
    #             # base_url="https://cdn-ind.testnet.deltaex.org"
    #             base_url = self.get_rest_url_from_firebase(user_id)   # LIVE
    #             )

    #             response = rest.place_order(**order_data)
    #             print(f"[{user_id}] ✅ Position Closed → {response}")

    #             return {
    #             "success": True,
    #             "type": "position_close",
    #             "order": response
    #             }

    #         except Exception as e:
    #             print(f"[{user_id}] ❌ Position Close Error → {e}")
    #             return {"success": False, "error": str(e)}


    # # -------- NO ORDER, NO POSITION --------
    #     print(f"[{user_id}] ⚠ Nothing to close for {symbol}")
    #     return {
    #     "success": False,
    #     "error": "no_active_order_or_position"
    #     }




    def get_data_ticker(self,user_id,symbol):
        client = self.clients[user_id]["delta"]
        api_key = client.api_key
        api_secret = client.api_secret

        # base_url='https://cdn-ind.testnet.deltaex.org'
        base_url = self.get_rest_url_from_firebase(user_id)   # LIVE
        client = DeltaRestClient(base_url=base_url,api_key=api_key, api_secret=api_secret)
        ticker = client.get_ticker(symbol)
        # print(ticker)

        if ticker:
            item = ticker
            print("item :",item)
            product_id = item.get("product_id")
            mark_price = float(item.get("mark_price", 0))
            best_bid = float(item["quotes"]["best_bid"]
                         ) if item.get("quotes") else None
            best_ask = float(item["quotes"]["best_ask"]
                         ) if item.get("quotes") else None
            spot_price = float(item.get("spot_price", 0))

            # print(f"📊 STBLUSD Perp Futures:")
            # print("   product id :", product_id)
            # print("   Mark Price:", mark_price)
            # print("   Spot Price:", spot_price)
            # print("   Best Bid/Ask:", best_bid, "/", best_ask)    
    
    
        return best_bid,product_id

    def set_laverage(self, user_id, product_id, leverage):
        client_ws = self.clients[user_id]["delta"]
        api_key = client_ws.api_key
        api_secret = client_ws.api_secret

        # ✅ DYNAMIC BASE URL (LIVE / TESTNET)
        base_url = self.get_rest_url_from_firebase(user_id)

        client = DeltaRestClient(
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret
        )

        try:
            leverage_data = client.set_leverage(
                product_id=product_id,
                leverage=leverage
            )

            print("leverage_data :", leverage_data)

            if leverage_data and leverage_data.get("leverage"):
                return {
                    "success": True,
                    "leverage": leverage_data.get("leverage")
                }

            return {
                "success": False,
                "error": "Invalid leverage response"
            }

        except Exception as e:
            print(f"[{user_id}] ❌ set leverage error:", e)
            return {
                "success": False,
                "error": str(e)
            }

    


    # def refresh_positions_once(self,user_id):
    #     # 2 seconds delay
    #     time.sleep(1)
    #     for user_id in data_store.keys():
    #         positions_data = self.get_api_datakk(user_id,"/v2/positions/margined", query={"user_id": user_id})
    #         if positions_data:
    #             with data_lock:
    #                 for pos in positions_data:
    #                     symbol = pos.get("product_symbol")
    #                     if symbol:
    #                         data_store[user_id]["positions"][symbol] = pos
    #     print("🔄 Positions refreshed once")


    # def refresh_positions_once(self, user_id):
    #     time.sleep(1)
    #     positions_data = self.get_api_datakk(
    #         user_id, "/v2/positions/margined", query={"user_id": user_id}
    #     )
    #     if positions_data:
    #         with data_lock:
    #             for pos in positions_data:
    #                 symbol = pos.get("product_symbol")
    #                 if symbol:
    #                     data_store[user_id]["positions"][symbol] = pos
    #     print(f"[{user_id}] 🔄 Positions refreshed once")


    # def refresh_positions_once(self, user_id):
    #     time.sleep(1)

    #     positions_data = self.get_api_datakk(
    #         user_id,
    #         "/v2/positions/margined",
    #         query={"user_id": user_id}
    #     )

    #     with data_lock:
    #         if not positions_data:
    #             # ❗ No positions → clear all
    #             data_store[user_id]["positions"].clear()
    #             print(f"[{user_id}] 🔄 No active positions (cleared)")
    #             return

    #         # ✅ FULL REPLACE (no ghost positions)
    #         new_positions = {}

    #         for pos in positions_data:
    #             symbol = pos.get("product_symbol")
    #             if symbol:
    #                 new_positions[symbol] = pos

    #         data_store[user_id]["positions"] = new_positions

    #     print(f"[{user_id}] 🔄 Positions refreshed ({len(new_positions)})")



    def refresh_positions_once(self, user_id):
        with self._refresh_lock:
            if user_id in self._refreshing:
                return
            self._refreshing.add(user_id)

        try:
            sleep(1)
            positions_data = self.get_api_datakk(
                user_id,
                "/v2/positions/margined",
                query={"user_id": user_id}
            )

            with data_lock:
                if user_id not in data_store:
                    return
                positions = data_store[user_id]["positions"]

                if not positions_data:
                    positions.clear()
                    return

                new_positions = {
                    pos["product_symbol"]: pos
                    for pos in positions_data
                    if pos.get("product_symbol")
                }

                # 🔥 SAFE UPDATE (reference intact)
                positions.clear()
                positions.update(new_positions)

        finally:
            with self._refresh_lock:
                self._refreshing.discard(user_id)




    # def refresh_positions_once(self, user_id):
    #     with self._refresh_lock:
    #         if user_id in self._refreshing:
    #             return
    #         self._refreshing.add(user_id)

    #     try:
    #         time.sleep(1)
    #         positions_data = self.get_api_datakk(
    #             user_id,
    #             "/v2/positions/margined",
    #             query={"user_id": user_id}
    #         )

    #         with data_lock:
    #             if not positions_data:
    #                 data_store[user_id]["positions"].clear()
    #                 return

    #             data_store[user_id]["positions"] = {
    #                 pos["product_symbol"]: pos
    #                 for pos in positions_data
    #                 if pos.get("product_symbol")
    #             }
    #     finally:
    #         with self._refresh_lock:
    #             self._refreshing.discard(user_id)




    # def restart_user_ws(self, user_id):
    #     with users_lock:
    #         # যদি client আগে থেকেই থাকে → বন্ধ করো
    #         if user_id in self.clients:
    #             old_client = self.clients[user_id]
    #             try:
    #                 if old_client.ws:
    #                     old_client.ws.close()
    #                     print(f"[{user_id}] 🔴 Old WS closed")
    #             except Exception as e:
    #                 print(f"[{user_id}] WS close error:", e)

    #             del self.clients[user_id]

    #         # Firebase থেকে updated API data নাও
    #         data = self.db_ref.child(str(user_id)).get()
    #         delta = data.get("database", {}).get("delta", {})

    #         api_key = delta.get("api_key")
    #         api_secret = delta.get("api_secret")

    #         print(api_key,api_secret)

    #         if not api_key or not api_secret:
    #             print(f"[{user_id}] ❌ Missing API after save")
    #             return False

    #         ws_url = self.get_ws_url_from_firebase(user_id)

    #         # নতুন WS client তৈরি
    #         client = UserWSClient(
    #             user_id=user_id,
    #             api_key=api_key,
    #             api_secret=api_secret,
    #             ws_url=ws_url
    #         )

    #         self.clients[user_id] = client
    #         client.start()

    #         print("clint api_key :",client.api_key)

    #         print(f"[{user_id}] 🟢 WS restarted successfully")
    #         return True
# //////////////////////////////
    # def upsert_user(self, user_id, api_key, api_secret, ws_url):
    #     user_id = str(user_id)

    #     for u in self.users:
    #         if u["user_id"] == user_id:
    #         # 🔁 UPDATE existing
    #             u.update({
    #                 "api_key": api_key,
    #                 "api_secret": api_secret,
    #                 "ws_url": ws_url,
    #                 "updated_at": time.time()
    #             })
    #             return

    #     # ➕ ADD new
    #     self.users.append({
    #         "user_id": user_id,
    #         "api_key": api_key,
    #         "api_secret": api_secret,
    #         "ws_url": ws_url,
    #         "updated_at": time.time()
    #     })



    # def restart_user_ws(self, user_id):
    #     user_id = str(user_id)

    #     with self.lock:

    #         if user_id in self.clients:
    #             try:
    #                 if self.clients[user_id].ws:
    #                     self.clients[user_id].ws.close()
    #             except Exception as e:
    #                 print(f"[{user_id}] WS close error:", e)
    #             del self.clients[user_id]


    #           # ✅ data_store RESET (IMPORTANT)
    #         with data_lock:
    #             data_store[user_id] = {
    #                 "positions": {},
    #                 "orders": {},
    #                 "margins": {}
    #             }
    #             print(f"[{user_id}] 🧹 data_store reset")

    #             # restart এর শেষে


    #         # Firebase থেকে নতুন API


    #         data = self.db_ref.child(user_id).get() or {}
    #         delta = data.get("database", {}).get("delta", {})

    #         api_key = delta.get("api_key")
    #         api_secret = delta.get("api_secret")

    #         if not api_key or not api_secret:
    #             print(f"[{user_id}] ❌ Missing API after save")
    #             return False

    #         ws_url = self.get_ws_url_from_firebase(user_id)

    #         # ✅ now safe
    #         # self.users[user_id] = {
    #         #     "api_key": api_key,
    #         #     "api_secret": api_secret,
    #         #     "ws_url": ws_url,
    #         #     "updated_at": time.time()
    #         # }

    #         # user = {
    #         #     "user_id": str(user_id),  # user_id সবসময় স্ট্রিং হিসেবে
    #         #     "api_key": api_key,
    #         #     "api_secret": api_secret,
    #         #     "ws_url": ws_url,
    #         #     "updated_at": time.time()
    #         #     }
    #         # self.users[user_id].append(user)

    #         self.upsert_user(user_id, api_key, api_secret, ws_url)
            

    #         client = UserWSClient(user_id, api_key, api_secret, ws_url)
    #         self.clients[user_id] = client
    #         client.start()

    #         print("data_store user:",data_store.get(user_id))
    #         print(f"[{user_id}] 🟢 WS restarted with NEW API")
    #         return True

# ///////////////////////////////////////////////////////////////////////

    def restart_user_ws(self, user_id):
        user_id = str(user_id)

        with users_lock:
            # Ensure client map exists
            if user_id not in self.clients:
                self.clients[user_id] = {"delta": None, "dcx": None}

            # 1️⃣ STOP old WS safely
            old = self.clients[user_id].get("delta")
            if old:
                old._reconnecting = False
                if old.ws:
                    old.ws.close()

            self.clients[user_id]["delta"] = None

            # 2️⃣ RESET data_store (SAFE mutate, no replace)
            with data_lock:
                if user_id not in data_store:
                    return False
                store = data_store[user_id]
                store["positions"].clear()
                store["orders"].clear()
                store["margins"].clear()

            # 3️⃣ LOAD fresh creds
            data = self.db_ref.child(user_id).get() or {}
            delta = data.get("database", {}).get("delta", {})

            api_key = delta.get("api_key")
            api_secret = delta.get("api_secret")
            if not api_key or not api_secret:
                return False

            ws_url = self.get_ws_url_from_firebase(user_id)

            # 4️⃣ START new WS
            client = UserWSClient(
                user_id,
                api_key,
                api_secret,
                ws_url,
                manager=self
            )

            self.clients[user_id]["delta"] = client
            client.start()

            return True







    # def restart_user_ws(self, user_id):
    #     user_id = str(user_id)

    #     if user_id not in self.clients:
    #         self.clients[user_id] = {"delta": None, "dcx": None}


    #     with self.lock:
    #         # 1️⃣ STOP old
    #         if user_id in self.clients:
    #             old = self.clients[user_id]["delta"]
    #             try:
    #                 # old._reconnecting = False
    #                 # if old.ws:
    #                 #     old.ws.close()

    #                 old = self.clients[user_id].get("delta")
    #                 if old:
    #                     old._reconnecting = False
    #                     if old.ws:
    #                         old.ws.close()

    #             except:
    #                 pass
    #             del self.clients[user_id]["delta"]

    #         # 2️⃣ RESET store
    #         with data_lock:
    #             data_store[user_id] = {
    #                 "positions": {},
    #                 "orders": {},
    #                 "margins": {}
    #             }

    #         # 3️⃣ LOAD fresh creds
    #         data = self.db_ref.child(user_id).get() or {}
    #         delta = data.get("database", {}).get("delta", {})

    #         api_key = delta.get("api_key")
    #         api_secret = delta.get("api_secret")
    #         if not api_key or not api_secret:
    #             return False

    #         ws_url = self.get_ws_url_from_firebase(user_id)

    #         # 4️⃣ START new
    #         client = UserWSClient(user_id, api_key, api_secret, ws_url,manager=self)
    #         self.clients[user_id]["delta"] = client
    #         client.start()

    #         return True


# ///////////////////////////////////////////////////////////////////////





    # def get_ws_url(self,user_id):
    #     client = self.clients[user_id]
    #     api_key = client.api_key
    #     api_secret = client.api_secret

    #     api_type = self.check_api_type(api_key, api_secret, user_id)

    #     if api_type == "TESTNET":
    #         return self.api_url["TESTNET"]
    #     else:
    #         return self.api_url["LIVE"]
    
        

    def get_rest_url_from_firebase(self, user_id):
        data = self.db_ref.child(str(user_id)).get()
        delta = data.get("database", {}).get("delta", {})
        api_type = delta.get("type", "TESTNET")

        if api_type == "LIVE":
            return "https://api.india.delta.exchange"
        return "https://cdn-ind.testnet.deltaex.org"
    
        

    def get_ws_url_from_firebase(self, user_id):
        data = self.db_ref.child(str(user_id)).get()
        delta = data.get("database", {}).get("delta", {})
        api_type = delta.get("type", "TESTNET")

        if api_type == "LIVE":
            return "wss://socket.india.delta.exchange"
        return "wss://socket-ind.testnet.deltaex.org"
    
    # ==========================
    # HELPER FUNCTIONS
    # ==========================


    def format_time_remaining(self,seconds):
        """Format seconds to HHh:MMm:SSs"""
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hours:02d}h:{minutes:02d}m:{secs:02d}s"
    

    def parse_funding_data(self,data):
        """Convert raw WebSocket funding data to readable dict"""
        next_funding_ts = data.get('next_funding_realization', 0) / 1_000_000
        now_ts = time.time()
        seconds_remaining = max(0, int(next_funding_ts - now_ts))
        formatted_time = self.format_time_remaining(seconds_remaining)

        timestamp_micro = data.get('timestamp', 0)
        timestamp_sec = timestamp_micro / 1_000_000
        update_time = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
        update_time_local = update_time + timedelta(hours=5, minutes=30)  # IST

        return {
        "symbol": data.get("symbol"),
        "funding_rate": data.get("funding_rate"),
        "funding_rate_8h": data.get("funding_rate_8h"),
        "predicted_funding_rate": data.get("predicted_funding_rate"),
        "predicted_funding_rate_8h": data.get("predicted_funding_rate_8h"),
        "next_funding_in": formatted_time,
        "timestamp_utc": update_time.strftime("%Y-%m-%d %H:%M:%S"),
        "timestamp_ist": update_time_local.strftime("%Y-%m-%d %H:%M:%S")
        }
    

    # def get_live_delta_ticker(self, symbols=None):
    #     """
    #     Returns latest ticker data from PublicWSClient
    #     """
    #     symbols = symbols or ["all"]

    #     result = {}

    #     with self.public_ws.lock:
    #         ticker_data = self.public_ws.ticker.copy()

    #     if symbols == ["all"]:
    #         for symbol, data in ticker_data.items():
    #             # result[symbol] = self.parse_ticker_data(data)
    #             result[symbol] = data
    #     else:
    #         for symbol in symbols:
    #             if symbol in ticker_data:
    #                 # result[symbol] = self.parse_ticker_data(ticker_data[symbol])
    #                 result[symbol] = ticker_data[symbol]

    #     return result




    # def get_live_delta_funding(self, symbols=None):
    #     """
    #     Returns latest funding data from PublicWSClient
    #     """
    #     symbols = symbols or ["all"]

    #     result = {}

    #     with self.public_ws.lock:
    #         funding_data = self.public_ws.funding.copy()

    #     if symbols == ["all"]:
    #         for symbol, data in funding_data.items():
    #             result[symbol] = self.parse_funding_data(data)
    #     else:
    #         for symbol in symbols:
    #             if symbol in funding_data:
    #                 result[symbol] = self.parse_funding_data(funding_data[symbol])

    #     return result


    # def get_public_ws(self, user_id):
    #     data = self.db_ref.child(str(user_id)).get()
    #     api_type = data.get("database", {}).get("delta", {}).get("type", "TESTNET")

    #     if api_type == "LIVE":
    #         return self.public_ws_live
    #     return self.public_ws_testnet
    
# //////////////////////////
    # def get_public_ws(self, user_id):
    #     user_id = str(user_id)
    #     data = self.db_ref.child(user_id).get() or {}

    #     api_type = data.get("database", {}) \
    #                 .get("delta", {}) \
    #                 .get("type", "TESTNET")

    #     return self.public_ws_live if api_type == "LIVE" else self.public_ws_testnet
    

    def get_public_ws(self, user_id):
        user_id = str(user_id)
        data = self.db_ref.child(user_id).get() or {}
        api_type = data.get("database", {}).get("delta", {}).get("type", "TESTNET")

        ws = self.public_ws_live if api_type == "LIVE" else self.public_ws_testnet

        with self._public_ws_lock:
            if not ws.running:
                ws.start()

        return ws








    def get_live_delta_ticker(self, user_id, symbols=None):
        ws = self.get_public_ws(user_id)
        symbols = symbols or ["all"]

        # with ws.lock:
        #     data = ws.ticker.copy()

        # with ws.lock:
        data = {k: v.copy() for k, v in ws.ticker.items()}

        if symbols == ["all"]:
            return data

        return {s: data[s] for s in symbols if s in data}



    def get_live_delta_funding(self, user_id, symbols=None):
        ws = self.get_public_ws(user_id)
        symbols = symbols or ["all"]

        # with ws.lock:
        data = ws.funding.copy()

        if symbols == ["all"]:
            return {k: self.parse_funding_data(v) for k, v in data.items()}

        return {s: self.parse_funding_data(data[s]) for s in symbols if s in data}




        # /////////////////////////////////////////////////////////////

    def _calculate_arbitrage(self, threshold=0.0):
        ex_data = self.get_live_delta_funding()
        dcx_data = get_dcx_funding_rate()
        dcx_map = dcx_map_builder(dcx_data)

        rows = []
        best = None

        for sym, ex in ex_data.items():
            if sym not in dcx_map:
                continue

            dcx = dcx_map[sym]
            diff = abs(ex["funding_rate"]) - abs(dcx["funding"])

            
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
            # now = datetime.now()
            # funding_dt1 = self.funding_time_to_datetime(ex["next_funding_in"], now)
            # print("Funding datetime for funding_dt1", sym, "is", funding_dt1.strftime("%H:%M:%S"))
            # funding_dt = self.round_to_next_30min(funding_dt1)
            # print("Funding datetime for", sym, "is", funding_dt.strftime("%H:%M:%S"))
            # ////////////////////////////////////

            row = {
                "symbol": sym,
                "ex_funding": ex["funding_rate"],
                "dcx_funding": dcx["funding"],
                "funding_diff": diff,
                "signal": "LONG" if diff < 0 else "SHORT",
                "dcx_price": dcx["mark_price"],
                "ex_funding_time": ex["next_funding_in"],
                "binance_funding_time": countdown_from_ms(
                    bn_data["next_funding_time"]
                    ),
                # "dcx_funding_time": countdown_from_ms(dcx["funding_time_ms"]),
                 "dcx_funding_time": countdown_from_ms(
                    bn_data["next_funding_time"]
                    ),
            }

            rows.append(row)

            if abs(diff) >= threshold:
                if not best or abs(diff) > abs(best["funding_diff"]):
                    best = row

        return rows, best    


    def arbitrage_bot_loop(self, interval=None):
        interval = interval or self.arbitrage_bot_loop_interval

        print("🤖 Arbitrage bot loop started")

        while not self._arb_stop_event.is_set():
            try:
                rows, best = self._calculate_arbitrage()

                with self._arb_lock:
                    self.funding_arbitrage = rows

                    if best:
                        if (
                            not self.best_trade
                            or best["symbol"] != self.best_trade["symbol"]
                            or abs(best["funding_diff"]) != abs(self.best_trade["funding_diff"])
                        ):
                            self.best_trade = best
                            print(
                                f"🔥 NEW BEST TRADE → {best['symbol']} "
                                f"diff={best['funding_diff']:.6f}"
                            )

                            # if abs(best['funding_diff']) > self.arbitrage_diff_threshold:
                              # 🔥 AUTO EXECUTION SCHEDULER
                                # self.auto_schedule_best_trade()

            except Exception as e:
                print("❌ arbitrage loop error:", e)

            # ⏳ interruptible sleep
            self._arb_stop_event.wait(interval)

        print("🛑 Arbitrage bot stopped cleanly")


    # def start_arbitrage_bot(self):
    #     if self._arb_thread and self._arb_thread.is_alive():
    #         print("⚠️ Bot already running")
    #         return

    #     self._arb_stop_event.clear()

    #     self._arb_thread = threading.Thread(
    #         target=self.arbitrage_bot_loop,
    #         daemon=True
    #     )
    #     self._arb_thread.start()

    def start_arbitrage_bot(self):
        if self._arb_thread:
            return
        self._arb_stop_event.clear()
        self._arb_thread = gevent.spawn(self.arbitrage_bot_loop)
    






    def stop_arbitrage_bot(self):
        print("🛑 Stopping arbitrage bot...")
        self._arb_stop_event.set()

    

        # /////////////////////////////////////////////////////////////





    def requast_api_data(self, user_id, path, query="", body="", method="GET"):
        user_id = str(user_id)

        print(f"[{user_id}] 🌐 API Request → {method} {path}")

        

        print("user  id problem :",self.clients[user_id]["delta"],"\n",self.clients.get(user_id, {}).get("delta"))

        client = self.clients[user_id]["delta"]

        # client_map = self.clients.get(user_id)
        # if not client_map or not client_map.get("delta"):
        #     return {"success": False, "error": "user_not_initialized"}
        
        # client = client_map["delta"]

        api_key = client.api_key
        api_secret = client.api_secret
        BASE_URL = self.get_rest_url_from_firebase(user_id)   # LIVE

        print("🔄 get_api_data called with:", path, query, body, method)
        # query = ""
        url = BASE_URL + path
        timestamp = str(int(time.time()))
        body_str = body if isinstance(body, str) else json.dumps(body)
        signature_payload = method + timestamp + path + query + body_str
        signature = hmac.new(
            api_secret.encode(), signature_payload.encode(), hashlib.sha256).hexdigest()

        headers = {
            "api-key": api_key,
            "timestamp": timestamp,
            "signature": signature,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        print("🔍 Headers:", headers)
        print("🔍 Body sent:", body_str)

        try:
            print("path:", path)
            print("url:", url)
            if method.upper() == "POST":
                r = requests.post(url, headers=headers, json=json.loads(body_str) if body_str else {}, timeout=30)
            else:
                r = requests.get(url, params={}, headers=headers, timeout=30)
            print("✅ Response status:", r.status_code)
            r.raise_for_status()

            data = r.json()
            # print("✅ Data received:", data)
            return data.get("result", {})
        except requests.exceptions.HTTPError as e:
            print(f"⚠️ HTTP Error for {path}: {e}, Status: {e.response.status_code}, Detail: {e.response.text}")
            if e.response.status_code == 401:
                print("   সম্ভাব্য সমস্যা: অগত্যা API কী, IP whitelisting, বা ৫ মিনিট অপেক্ষা।")
            return None
        except json.JSONDecodeError as e:
            print(f"⚠️ JSON Error for {path}: {e}, Body: {body_str}")
            return None
        except Exception as e:
            print(f"⚠️ সাধারণ ত্রুটি for {path}: {str(e)}")
            return None

    


    def get_api_datakk(self,user_id,path, query=None, body=None, method="GET"):

        user_id = str(user_id)

        if user_id not in self.clients or not self.clients[user_id].get("delta"):
            return None


        print(f"[{user_id}] 🌐 API Request → {method} {path}")

        client = self.clients[user_id]["delta"]

        api_key = client.api_key
        api_secret = client.api_secret

         # 🔥 FIX: Prevent string query/body from breaking signature
        if isinstance(query, str) and query.strip() == "":
            query = None

        if isinstance(body, str) and body.strip() == "":
            body = None

        print(f"[{user_id}] 🔑 Using API Key:  api_key :{api_key} \n  api_secret :{api_secret}")



        # base_url='https://cdn-ind.testnet.deltaex.org'
        base_url = self.get_rest_url_from_firebase(user_id)   # LIVE
        client = DeltaRestClient(base_url=base_url,api_key=api_key, api_secret=api_secret)

        try:
            print("🔄 Calling:", path, "Method:", method)
            print("🔍 🌐 Query:", query)
            print("🔍 Body:", body)

            if method.upper() == "POST":
                # POST = payload (body), GET-style query is None
                response = client.request(
                    method="POST",
                    path=path,
                    payload=body or {},
                    query=None,
                    auth=True
                )
            else:
            # GET = payload None, query params dict
                response = client.request(
                    method="GET",
                    path=path,
                    payload=None,
                    query=query or {},
                    auth=True
                )

            print("✅ API response received")


            # print("🔍 Response type:", type(response))

            # FIX: response is a requests.Response object → use .json()
            data = response.json()
            # print("🔍 Parsed JSON:", data)

            return data.get("result", {})


        # HTTP Error (status code related)
        except Exception as e:
            err = str(e).lower()

            if "401" in err or "unauthorized" in err:
                print("⚠️ HTTP 401: Check API key, signature, or IP whitelist.")
        
            elif "403" in err:
                print("⚠️ HTTP 403: Access forbidden (permission or IP issue).")
        
            elif "404" in err:
                print(f"⚠️ HTTP 404: Endpoint not found → {path}")
        
            elif "500" in err:
                print("⚠️ Server error (500). Try again later.")
        
            elif "json" in err:
                print("⚠️ JSON Parse Error: API returned invalid JSON.")

            else:
                print("⚠️ Unknown Error:", str(e))

            return None
        

    # --------------------- DCX - REQUAST ---------------------------------

    # def Dcx_place_order(self, user_id, data):
    #     """
    #     নির্দিষ্ট user এর API key দিয়ে অর্ডার প্লেস করবে
    #     """
    #     if user_id not in self.clients:
    #         print(f"[ম্যানেজার] ❌ ব্যবহারকারী {user_id} পাওয়া যায়নি")
    #         return {"error": "user_not_found"}

    #     client = self.clients[user_id]
        
    #     # Particular user-এর api key / secret
    #     api_key = client.api_key
    #     api_secret = client.api_secret

    #     print("api_key :",api_key,)


    # --------------------------------------------------------


    
    








# class MultiUserManager:
#     def __init__(self, users):
#         self.clients = {}
#         for u in users:
#             self.clients[u["user_id"]] = UserWSClient(u["user_id"], u["api_key"], u["api_secret"])

    # def start_all(self):
    #     for c in self.clients.values():
    #         c.start()

    # def get_user_data(self, user_id):
    #     # client = self.clients.get(user_id)
    #     # client.subscribe_channel("positions", ["all"])
    #     with data_lock:
    #         return data_store.get(user_id, {})
        

        
def show_user_data(user_id):
    
    with data_lock:
        data = data_store.get(user_id, {})
        # if not data:
        #     print(f"❌ No data found for user: {user_id}")
        #     return {}
        
        # return data
    # print("\n===============================")
    # print(f"📊 Data for {user_id}")
    # print("===============================\n")

    # # # Positions
    # positions = data.get("positions", {})
    # if positions:
    #     print("🟦 Positions:")
    #     for symbol, pos in positions.items():
    #         # print(pos)
    #         print(f"  • {symbol} | size={pos.get('size')} | entry={pos.get('entry_price')} | "
    #               f"mark={pos.get('mark_price')} | pnl={pos.get('unrealized_pnl')}")
    # else:
    #     print("🟦 Positions: None")

    # # Orders
    # # Orders
    # orders = data.get("orders", {})
    # if orders:
    #     print("\n🟨 Orders:")
    #     for oid, order in orders.items():
    #         symbol = order.get("product_symbol") or order.get("symbol")
    #         price = order.get("limit_price") or order.get("price")
    #         side = order.get("side")
    #         size = order.get("size")
    #         state = order.get("state")
    #         print(f"  • {symbol} | side={side} | price={price} | size={size} | state={state}")
    # else:
    #     print("\n🟨 Orders: None")


    # # Margins
    # margins = data.get("margins", {})
    # if margins:
    #     print("\n🟩 Margins:")
    #     for asset, margin in margins.items():
    #         equity = margin.get('equity')
    #         available = margin.get('available_balance')
    #         print(f"  • {asset}: equity={equity}, available={available}")
    # else:
    #     print("\n🟩 Margins: None")

    # print("===============================")

    return data




# ==========================
# 🔁 Update Unrealized PnL (from live ticker)
# ==========================
# def update_unrealized_pnl_for_all_users(live_data):
#     print("hhh")
#     try:
#         # Get latest mark prices (from ex.py)
#         # live_data = get_live_ticker()
#         if not live_data:
#             print("not live data")
#             return
#         print(live_data)
#         with data_lock:
#             for user_id, user_data in data_store.items():
#                 positions = user_data.get("positions", {})
#                 for symbol, pos in positions.items():
#                     entry = float(pos.get("entry_price", 0) or 0)
#                     size = float(pos.get("size", 0) or 0)

#                     ticker = live_data.get(symbol)
#                     if not ticker:
#                         continue

#                     mark_price = float(ticker.get("mark_price", 0) or 0)
#                     print(mark_price)

#                     # PnL হিসাব
#                     pnl = (mark_price - entry) * size if size > 0 else (entry - mark_price) * abs(size)

#                     # store update
#                     pos["mark_price"] = mark_price
#                     pos["unrealized_pnl"] = pnl

#                     print(f"[{user_id}] {symbol} | entry={entry} | mark={mark_price} | PnL={pnl:.8f}")

#     except Exception as e:
#         print("⚠️ Error updating PnL:", e)


# def update_unrealized_pnl_for_user(user_id, live_data):
#     """
#     নির্দিষ্ট user-এর সব positions-এর unrealized PnL আপডেট করে।
#     live_data থেকে প্রতিটি symbol-এর mark_price ব্যবহার করা হয়।
#     """
#     with data_lock:
#         user_data = data_store.get(user_id)
#         if not user_data:
#             return  # user ডেটা নাই, কিছুই করব না

#         positions = user_data.get("positions", {})
#         if not positions:
#             return  # কোনো position নাই

#         for symbol, pos in positions.items():
#             mark_info = live_data.get(symbol)
#             if not mark_info:
#                 continue  # ঐ symbol-এর live data নাই

#             mark_price = float(mark_info.get("mark_price", 0))
#             entry_price = float(pos.get("entry_price", 0))
#             size = float(pos.get("size", 0))

#             if mark_price == 0 or entry_price == 0 or size == 0:
#                 continue

#             # Unrealized PnL হিসাব (long/short দুই দিকেই)
#             if size > 0:
#                 pnl = (mark_price - entry_price) * size
#             else:
#                 pnl = (entry_price - mark_price) * abs(size)

#             # আপডেট store-এ
#             pos["unrealized_pnl"] = round(pnl, 6)

#             pos["mark_price"] = mark_price
#             # pos["unrealized_pnl"] = pnl

#         # data_store তে আপডেট হয়ে গেছে
#         data_store[user_id]["positions"] = positions
#         print(f"[{user_id}] {symbol} | entry={entry_price:.8f} | mark={mark_price:.8f} | PnL={pnl:.8f}")


def update_unrealized_pnl_for_user(user_id, live_data):
    """
    নির্দিষ্ট user-এর সব positions-এর unrealized PnL আপডেট করে।
    live_data থেকে প্রতিটি symbol-এর mark_price এবং contract_value ব্যবহার করা হয়।
    """
    with data_lock:
        user_data = data_store.get(user_id)
        if not user_data:
            return  # user data নাই

        positions = user_data.get("positions", {})
        if not positions:
            return  # কোনো position নাই

        print("positions : ",positions)

        # for symbol, pos in positions.items():
        for symbol, pos in list(positions.items()):
    
            ticker = live_data.get(symbol)
            if not ticker:
                continue  # live data নাই
            
            # print("quotes :",ticker.get("quotes", 0))

            # print("pos :",pos)
            # mark price & contract value
            mark_price = float(ticker.get("mark_price", 0))
            contract_value = float(ticker.get("contract_value", 1))  # default 1
            entry_price = float(pos.get("entry_price", 0))
            size = float(pos.get("size", 0))
            actual_margin = float(pos.get("margin", 0.0))  # API থেকে মার্জিন
            if actual_margin == 0:
                notional = entry_price * abs(size) * contract_value
                leverage = float(pos.get("default_leverage", 10))
                actual_margin = notional / leverage
                print(f"[{user_id}] Calculated margin for {symbol}: {actual_margin:.6f}")

            print(f"[{user_id}] Current Margin: {actual_margin}")
            print(f"[{user_id}] Margin check - Symbol: {symbol}, Margin: {actual_margin:.6f}")

            if mark_price == 0 or entry_price == 0 or size == 0 :
                continue

            # print("size :",size)

            # Unrealized PnL calculation
            if size > 0:
                pnl = (mark_price - entry_price) * abs(size) * contract_value
            else:
                pnl = (entry_price - mark_price) * abs(size) * contract_value

            # print("pnl :",f"{pnl:.3f}".rstrip('0').rstrip('.'))
            # Notional-based value
            position_value = entry_price * abs(size) * contract_value

            # Two kinds of %
            # pnl_pct_notional = (pnl / position_value) * 100
            # pnl_pct_margin = (pnl / margin) * 100   # Exchange-style %

            
            pnl_pct_notional = (pnl / position_value) * 100 if position_value != 0 else 0
            pnl_pct_margin = (pnl / actual_margin) * 100

            
            # store update
            pos["mark_price"] = mark_price
            # pos["unrealized_pnl"] = f"{pnl:.3f}".rstrip('0').rstrip('.')  # e.g. 0.099999 → '0.099'
            pos["unrealized_pnl"] = f"{pnl:.3f}".rstrip('0').rstrip('.')
            pos["unrealized_pnl_pct_notional"] = round(pnl_pct_notional, 2)
            pos["unrealized_pnl_pct_margin"] = round(pnl_pct_margin, 2)
            pos["margin"] = actual_margin

            #update bestbid,best ask, quotes ...
            pos["quotes"] = ticker.get("quotes", 0)

        # data_store এ আপডেট হয়ে গেছে
        # data_store[user_id]["positions"] = positions
        print(f"[{user_id}] positions updated.")









# ==========================
        
# get_user_data
# ==========================
# Example usage
# ==========================
# if __name__ == "__main__":
    
#     try:
#         data_store.clear()
#         # manager = MultiUserManager(USERS)
#         manager = MultiUserManager()
#         manager.start_all()

        # user = {
            # "user_id": 2,
        #     "api_key": event.data["api_key"],
        #     "api_secret": event.data["api_secret"]
        #     }
        # manager.add_user()

        # while True:
        #     user_id = "1"
        #     data = manager.get_api_datakk(user_id, "/v2/wallet/balances",query={"user_id": user_id})
        #     print("data :",data)
            # time.sleep(10)



            # positions_data = manager.get_api_datakk(
            #     user_id,
            #     "/v2/positions/margined", 
            #     # query=f"user_id={user_id}")
            #     query={"user_id": user_id})
            

            # if positions_data:
            #     for pos in positions_data:
            #         symbol = pos.get("product_symbol")
            #         if symbol:
            #             print("symbol :",symbol)
            # time.sleep(10)




        #     time.sleep(5)
        #     user_id = "1"
            # print("Loaded clients:", manager.clients.keys())
            # client = manager.clients[str(user_id)]


            # api_type = manager.check_api_type(user_id=user_id,api_key=client.api_key, api_secret=client.api_secret)
            # print("API Type:", api_type)


            # user_id = "1"
    #         contact = "AIAUSD"
    #         # data = manager.get_user_data(1)
    #         # data = manager.place_order("1")
    #         # print("data 1:",data)

    #         best_bid,product_id = manager.get_data_ticker(user_id=user_id, symbol=contact)
    #         print("best_bid :",best_bid," product_id :",product_id)
    #         size = 1
    #         side = "buy"
    #         limit_price = best_bid
    #         client_order_id = str(int(time.time())) + uuid.uuid4().hex[:8]

    #         uiii = { 
    #             "product_id": product_id,
    #             "size": size,
    #             "side": side,
    #             "client_order_id": client_order_id,
    #             "time_in_force": TimeInForce.GTC,
    #             "order_type": OrderType.LIMIT,
    #             "limit_price": best_bid
    #             }

    #         manager.place_order(user_id=user_id, data=uiii)
            
            


    # except KeyboardInterrupt:
    #     print("বন্ধ হচ্ছে...")    


    # try:
    #     while True:
    #         time.sleep(5)
    #         live_data = get_api_data("/v2/tickers")  # ধরে নিচ্ছি এটি live ticker ডেটা ফেরায়
    #         if live_data:
    #             with users_lock:
    #                 for user_id in manager.clients.keys():
    #                     update_unrealized_pnl_for_user(user_id, live_data)
    #                     show_user_data(user_id)
    # except KeyboardInterrupt:
    #     print("বন্ধ হচ্ছে...")

    
    # while True:
    #     time.sleep(3)
    # data = get_live_ticker()
    # print(SYMBOLS)
    # print(data)


        # print("hh")
        # 🔁 প্রতি ২ সেকেন্ডে সব user এর position update হবে
        # update_unrealized_pnl_for_all_users()

    # while True:
    #     time.sleep(5)
        
    #     for u in USERS:
        # data = show_user_data("user1")
        # print(data)
        # positions = data.get("positions", {})
        # if positions:
        #     print("🟦 Positions:")
        #     for symbol, pos in positions.items():
        #             # print(pos)
        #         print(f"  • {symbol} | size={pos.get('size')} | entry={pos.get('entry_price')} | "
        #         f"mark={pos.get('mark_price')} | pnl={pos.get('unrealized_pnl')}")
        # else:
        #     print("🟦 Positions: None")


            # user_id = u["user_id"]
            # data = manager.get_user_data(user_id)
            # print(f"Data for {user_id}: {data}")

    #         # Margins display
    #         margins = data.get("margins", {})
    #         for asset, info in margins.items():
    #             print(f"{user_id} - {asset} available balance: {info.get('available_balance')}")
