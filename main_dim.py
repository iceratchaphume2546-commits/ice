import os
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime
import pytz
from google.cloud import storage
import re

# ----------------------
# โหลด .env
# ----------------------
load_dotenv()

# ----------------------
# Environment variables
# ----------------------
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "hongthai")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")
SCOPE = os.getenv("SCOPE")  # เพิ่มบรรทัดนี้

# -----------------------------
# ฟังก์ชันเวลาแบบ Bangkok
# -----------------------------
def now_th(fmt=None):
    tz = pytz.timezone("Asia/Bangkok")
    now = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(tz)
    return now.strftime(fmt) if fmt else now

def now_th_iso():
    tz = pytz.timezone("Asia/Bangkok")
    now = datetime.now(tz)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")

# -----------------------------
# ขอ access token จาก Azure AD
# -----------------------------
def get_access_token():
    tenant_id = os.getenv("TENANT_ID")
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    scope = os.getenv("SCOPE")

    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    r = requests.post(url, data=data, headers=headers)
    r.raise_for_status()
    return r.json()["access_token"]

# -----------------------------
# ดึงข้อมูล Dataverse (FULL LOAD)
# -----------------------------
def fetch_dataverse_data(token, api_name):
    url = f"{DATAVERSE_URL}/api/data/v9.2/{api_name}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    data = []
    while url:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        js = r.json()
        data.extend(js.get("value", []))
        url = js.get("@odata.nextLink")
    return data

# -----------------------------
# ทำ column ให้ปลอดภัย
# -----------------------------
def clean_columns_for_bq(df):
    df.columns = [re.sub(r"[^\w]", "_", c).lower() for c in df.columns]
    df.columns = [
        c if c[0].isalpha() or c[0] == "_" else f"col_{i}"
        for i, c in enumerate(df.columns)
    ]
    return df

# -----------------------------
# Upload GCS
# -------------------
