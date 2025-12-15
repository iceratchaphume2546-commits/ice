import os
import pandas as pd
import requests
from datetime import datetime
import pytz
from google.cloud import storage
import re

# ❌ ไม่ต้อง load_dotenv ใน Cloud Run
# from dotenv import load_dotenv
# load_dotenv()

GCS_BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]
TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
DATAVERSE_URL = os.environ["DATAVERSE_URL"]
SCOPE = os.environ["SCOPE"]


# -----------------------------
# ตั้งค่า path ของ JSON key สำหรับ GCS
# -----------------------------
# ให้แน่ใจว่าไฟล์ gcp-key.json อยู่ใน container /app
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/gcp-key.json"

# -----------------------------
# Environment variables
# -----------------------------
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "hongthai")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")
SCOPE = os.getenv("SCOPE")  # ต้องตั้งค่าเป็น https://yourorg.crm.dynamics.com/.default

# -----------------------------
# ฟังก์ชันเวลาแบบ Bangkokk
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
# ขอ access token จาก Azure ADd
# -----------------------------
def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPE
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
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
# ทำ column ให้ปลอดภัยสำหรับ BigQuery
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
# -----------------------------
def upload_to_gcs(df, folder, filename):
    client = storage.Client()  # จะใช้ key ที่ตั้งค่าไว้ด้านบน
    bucket = client.bucket(GCS_BUCKET_NAME)
    path = f"{folder}/{filename}"
    blob = bucket.blob(path)

    # Save temp file แล้ว upload
    temp_file = "temp_dim.ndjson"
    df.to_json(temp_file, orient="records", lines=True, force_ascii=False)
    blob.upload_from_filename(temp_file)

    print(f" อัปโหลด → gs://{GCS_BUCKET_NAME}/{path}")

# -----------------------------
# MAIN (FULL LOAD DIM)
# -----------------------------
if __name__ == "__main__":
    print(" เริ่ม FULL LOAD DIM")

    # ขอ access token
    token = get_access_token()

    # List ของ DIM entities
    dim_entities = {
        "dimension/channels": "itsm_channels",
        "dimension/kols": "itsm_kols",
        "dimension/pages": "itsm_pages",
        "dimension/products": "itsm_products"
    }

    # Loop ดึงข้อมูลแต่ละ entityy
    for folder, api_name in dim_entities.items():
        print(f"\n📥 ดึงข้อมูล {api_name}")
        data = fetch_dataverse_data(token, api_name)

        df = pd.DataFrame(data)
        if df.empty:
            print(" ไม่มีข้อมูล")
            continue

        df = clean_columns_for_bq(df)
        upload_to_gcs(df, folder, f"{folder.split('/')[-1]}.ndjson")

    print("🎉 FULL LOAD DIM เสร็จสมบูรณ์")
