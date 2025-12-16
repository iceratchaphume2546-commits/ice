import os
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime
import pytz
from google.cloud import storage
import re

# -----------------------------
# โหลด .env
# -----------------------------
load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "hongthai")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")

# -----------------------------
# เวลาแบบ Bangkok
# -----------------------------
def now_th(fmt=None):
    tz = pytz.timezone("Asia/Bangkok")
    now = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(tz)
    return now.strftime(fmt) if fmt else now

YEAR = now_th("%Y")
MONTH = now_th("%m")
DAY = now_th("%d")
TIME = now_th("%H%M%S")

# -----------------------------
# Azure AD Access Token
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
# ดึงข้อมูล Dataverse
# -----------------------------
def fetch_dataverse_data(token, api_name):
    url = f"{DATAVERSE_URL}/api/data/v9.2/{api_name}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    data = []
    while url:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        js = r.json()
        data.extend(js.get("value", []))
        url = js.get("@odata.nextLink")
    return data

# -----------------------------
# Clean columns
# -----------------------------
def clean_columns_for_bq(df):
    df.columns = [re.sub(r"[^\w]", "_", c).lower() for c in df.columns]
    df.columns = [c if c[0].isalpha() or c[0] == "_" else f"col_{i}" 
                  for i, c in enumerate(df.columns)]
    return df

# -----------------------------
# Upload ไป GCS
# -----------------------------
def upload_to_gcs(df, folder, filename):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    
    path = f"{folder}/{YEAR}/{MONTH}/{DAY}/{TIME}_{filename}"
    blob = bucket.blob(path)
    
    temp_file = "temp.ndjson"
    df.to_json(temp_file, orient="records", lines=True, force_ascii=False)
    blob.upload_from_filename(temp_file)
    os.remove(temp_file)
    
    print(f"✅ อัปโหลด → gs://{GCS_BUCKET_NAME}/{path}")
    
    # แสดงไฟล์ใน bucket หลังอัปโหลด
    print("📄 รายการไฟล์ล่าสุดใน bucket:")
    for b in bucket.list_blobs(prefix=folder):
        print(f" - {b.name}")

# -----------------------------
# MAIN FULL LOAD DIM
# -----------------------------
if __name__ == "__main__":
    print(f"🚀 เริ่ม FULL LOAD DIM: {YEAR}-{MONTH}-{DAY} {TIME}")
    token = get_access_token()

    dim_entities = {
        "dimension/channels": "itsm_channels",
        "dimension/kols": "itsm_kols",
        "dimension/pages": "itsm_pages",
        "dimension/products": "itsm_products"
    }

    for folder, api_name in dim_entities.items():
        print(f"\n📥 ดึงข้อมูล {api_name}")
        data = fetch_dataverse_data(token, api_name)
        if not data:
            print("⚠️ ไม่มีข้อมูล")
            continue

        df = pd.DataFrame(data)
        df = clean_columns_for_bq(df)

        filename = f"{folder.split('/')[-1]}.ndjson"
        upload_to_gcs(df, folder, filename)

    print(f"🎉 FULL LOAD DIM เสร็จสมบูรณ์: {YEAR}-{MONTH}-{DAY} {TIME}")
