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

YEAR = now_th("%Y")
MONTH = now_th("%m")
DAY = now_th("%d")

# -----------------------------
# ขอ access token จาก Azure AD
# -----------------------------
def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": f"{DATAVERSE_URL}/.default"
    }
    r = requests.post(url, data=payload)
    r.raise_for_status()
    return r.json()["access_token"]

# -----------------------------
# ดึงข้อมูลจาก Dataverse
# -----------------------------
def fetch_dataverse_data(token, api_name):
    time_now = now_th_iso()
    url = (
        f"{DATAVERSE_URL}/api/data/v9.2/{api_name}"
        f"?$filter=modifiedon lt '{time_now}'"
    )

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
# ปรับชื่อคอลัมน์ให้ BigQuery-safe
# -----------------------------
def clean_columns_for_bq(df):
    df.columns = [re.sub(r"[^\w]", "_", c).lower() for c in df.columns]
    df.columns = [
        c if c[0].isalpha() or c[0] == "_" else f"col_{i}"
        for i, c in enumerate(df.columns)
    ]
    return df

# -----------------------------
# 🔥 แก้ปัญหา BigQuery JSON error
# -----------------------------
def sanitize_for_bigquery(df):
    # แปลง dict / list → string
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: str(x) if isinstance(x, (dict, list)) else x
        )

    # NaN → None (BigQuery อ่านได้)
    df = df.where(pd.notnull(df), None)

    return df

# -----------------------------
# อัปโหลด DataFrame ขึ้น GCS
# -----------------------------
def upload_to_gcs(df, folder, filename):
    path = f"{folder}/{YEAR}/{MONTH}/{DAY}/{filename}"

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(path)

    temp_file = "temp.ndjson"
    df.to_json(
        temp_file,
        orient="records",
        lines=True,
        force_ascii=False
    )

    blob.upload_from_filename(temp_file)
    os.remove(temp_file)

    print(f"✅ อัปโหลดสำเร็จ → gs://{GCS_BUCKET_NAME}/{path}")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("🚀 เริ่มรัน Dataverse → GCS")

    token = get_access_token()

    entities = {
        "ads/header": "itsm_adses",
        "ads/line": "itsm_ads_product_lines"
    }

    for folder, api_name in entities.items():
        print(f"\n📥 กำลังดึงข้อมูล {api_name}")
        data = fetch_dataverse_data(token, api_name)

        if not data:
            print("⚠️ ไม่มีข้อมูล")
            continue

        df = pd.DataFrame(data)
        df = clean_columns_for_bq(df)
        df = sanitize_for_bigquery(df)   # 👈 จุดที่แก้ให้ line ไม่พัง

        filename = f"{folder.split('/')[-1]}.ndjson"
        upload_to_gcs(df, folder, filename)

    print("🎉 รันเสร็จสมบูรณ์ – Job จบการทำงาน")
