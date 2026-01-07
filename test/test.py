from threading import Event
import base64
from delta_rest_client import DeltaRestClient
import time
import hmac
import hashlib
import requests
import json
import websocket
from datetime import datetime, timezone, timedelta

from delta_rest_client import DeltaRestClient,OrderType,TimeInForce

import uuid

import threading
from threading import Thread, Event
import time
from collections import defaultdict

from flask import Flask, request, jsonify
from flask_cors import CORS

# from aa import _request

app = Flask(__name__)

# API credentials

# Base URL
base_url = "https://api.india.delta.exchange"
# base_url = "https://testnet-api.delta.exchange/v2"

WS_URL = "wss://socket.india.delta.exchange"
# WS_URL = "wss://socket-ind.testnet.deltaex.org"

API_KEY = "3nIxmgHVOlDpVAEUST1oqlP45SG69q"
API_SECRET = "XdWUfR9aHgv6OqhlhW9pLuBb8wackKZhmJT6pSEcgAyaKoUzNMzTLtJvhteU"

# api_key = "8HiEv5IWu67YNnO1iqUX2cIx3hCa7p"
# api_secret = "O5dbzlC5IHgYEM1mfC0TUXnRSSokzzNhc5ucTcIYna7xNeNCyBaoHhw03GVB"

# BASE_URL = "https://cdn-ind.testnet.deltaex.org"
# WS_URL = "wss://socket-ind.testnet.deltaex.org"


# Function to generate signature for authentication


# client = DeltaRestClient(API_KEY, API_SECRET)
client = DeltaRestClient(
    base_url=base_url,
    api_key=API_KEY,
    api_secret=API_SECRET
    )


def get_asset_id(symbol,url):
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
    





def get_api_data(path, query="", body="", method="GET"):
    time_ = datetime.now().strftime("%H:%M:%S.%f")    
    print(f'🔄 get_api_data called with: {time_}', path, query, body, method)
    # query = ""
    url = base_url + path
    timestamp = str(int(time.time()))
    body_str = body if isinstance(body, str) else json.dumps(body)
    signature_payload = method + timestamp + path + query + body_str
    signature = hmac.new(
        API_SECRET.encode(), signature_payload.encode(), hashlib.sha256).hexdigest()

    headers = {
        "api-key": API_KEY,
        "timestamp": timestamp,
        "signature": signature,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    print("🔍 Headers:", headers)
    print("🔍 Body sent:", body_str)

    try:
        print(f"path: {time_}", path)
        print("url:", url)
        if method.upper() == "POST":
            r = requests.post(url, headers=headers, json=json.loads(body_str) if body_str else {}, timeout=30)
        else:
            r = requests.get(url, params={}, headers=headers, timeout=30)
        
        print(f'✅ Response status: {time_}', r.status_code)
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




# gg = client.get_assets()
# # print(gg)

# asset_id = get_asset_id("USD", base_url)
# print(asset_id)
gg = client.get_balances(asset_id=14)
print("gg :",gg)



# data = get_api_data("/v2/wallet/balances")
# print("data :",data)