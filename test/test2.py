# while True:
#     confirm = input("Confirm করবেন? (y/n): ").strip().lower()

#     if confirm in ["y", "yes"]:
#         print("✔ Confirmed")
#         break
#     elif confirm in ["n", "no"]:
#         print("❌ Cancelled")
#         break
#     else:
#         print("⚠ ভুল input, আবার দিন")


# from threading import Event
# import base64
from delta_rest_client import DeltaRestClient
import time
import hmac
import hashlib
import requests
import json
# import websocket
from datetime import datetime, timezone, timedelta

from delta_rest_client import DeltaRestClient,OrderType,TimeInForce

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from binance import get_binance_funding,dcx_to_binance_symbol,get_binance_funding_safe,get_live_binance_funding,get_all_binance_funding

# data = get_live_binance_funding("BTCUSDT")
data = get_all_binance_funding()
print("data : ",data)





