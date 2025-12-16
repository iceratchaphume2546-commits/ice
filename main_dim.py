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
GCS_KEY_PATH = os.getenv("GCS_KEY_PATH")

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")

if not GCS_KEY_PATH or not os.path.exists(GCS_KEY_PATH):
    raise ValueError("❌ ไม่พบไฟล์ GCS_KEY_PATH ใน .env หรือ path ไม่ถูกต้อง")
else:
    print(f"✅ ใช้ GCS credentials จาก: {GCS_KEY_PATH}")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_KEY_PATH

# -----------------------------
# เวลา Bangkok
# -----------------------------
def now_th(fmt=None):
    tz = pytz.timezone("Asia/Bangkok")
    now = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(tz)
    return now.strftime(fmt) if fmt else now

def now_th_iso():
    tz = pytz.timezone("Asia/Bangkok")
    now = datetime.now(tz)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")

YEAR = now_th("%Y")
MONTH = now_th("%m")
DAY = now_th("%d")

# -----------------------------
#

