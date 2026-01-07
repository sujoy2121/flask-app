import hmac, hashlib, time, json, requests

class DcxRestClient:
    BASE_URL = "https://api.coindcx.com"

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret

    def _headers(self, body):
        timestamp = int(time.time() * 1000)
        json_body = json.dumps(body, separators=(',', ':'))
        signature = hmac.new(
            self.api_secret,
            json_body.encode(),
            hashlib.sha256
        ).hexdigest()

        return {
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": signature,
            "Content-Type": "application/json"
        }, json_body

    def get_positions(self):
        body = {"timestamp": int(time.time() * 1000)}
        headers, payload = self._headers(body)
        r = requests.post(
            self.BASE_URL + "/exchange/v1/derivatives/futures/positions",
            headers=headers,
            data=payload
        )
        return r.json()

    def place_order(self, body):
        headers, payload = self._headers(body)
        r = requests.post(
            self.BASE_URL + "/exchange/v1/derivatives/futures/orders",
            headers=headers,
            data=payload
        )
        return r.json()
