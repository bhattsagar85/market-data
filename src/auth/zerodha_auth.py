from kiteconnect import KiteConnect
import os
import json
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

API_KEY = os.getenv("KITE_API_KEY")
API_SECRET = os.getenv("KITE_API_SECRET")
TOKEN_FILE = "kite_token.json"


def get_login_url() -> str:
    """
    Step 1: Generate Zerodha login URL
    """
    if not API_KEY:
        raise RuntimeError("KITE_API_KEY not set")

    kite = KiteConnect(api_key=API_KEY)
    return kite.login_url()


def _serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def generate_session(request_token: str):
    """
    Step 2: Exchange request_token for access_token
    """
    if not API_KEY or not API_SECRET:
        raise RuntimeError("KITE_API_KEY / KITE_API_SECRET not set")

    kite = KiteConnect(api_key=API_KEY)

    data = kite.generate_session(
        request_token=request_token,
        api_secret=API_SECRET
    )

    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f, indent=2, default=_serialize)

    print("✅ Access token generated and saved")
    return data


def load_access_token() -> str:
    """
    Step 3: Load saved access_token
    """
    if not os.path.exists(TOKEN_FILE):
        raise RuntimeError("Access token not found. Please login again.")

    with open(TOKEN_FILE) as f:
        return json.load(f)["access_token"]
