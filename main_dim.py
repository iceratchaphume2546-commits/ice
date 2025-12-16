import os
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone
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
GCS_KEY_PATH = os.getenv("GCS_CREDENTIAL_JSON")
if not GCS_KEY_PATH or not os.path.exists(GCS_KEY_PATH):
    raise ValueError("❌ ไม่พบไฟล์ GCS_KEY_PATH ใน .env หรือ path ไม่ถูกต้อง")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_KEY_PATH
print(f"✅ ใช้ GCS credentials จาก: {GCS_KEY_PATH}")

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")
SCOPE = os.getenv("SCOPE", f"{DATAVERSE_URL}/.default")

# ----------------------
# เวลาแบบ Bangkok
# ----------------------
def now_th(fmt=None):
    tz = pytz.timezone("Asia/Bangkok")
    now = datetime.now(timezone.utc).astimezone(tz)
    return now.strftime(fmt) if fmt else now

# ----------------------
# ขอ access token
# ----------------------
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

# ----------------------
# ดึงข้อมูลจาก Dataverse
# ----------------------
def fetch_dataverse_data(token, api_name):
    url = f"{DATAVERSE_URL}/api/data/v9.2/{api_name}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    data = []
    while url:
        r = requests.get(url, headers=headers)
        if r.status_code == 404:
            print(f"⚠️ ข้าม {api_name} → 404 Not Found")
            return []
        r.raise_for_status()
        js = r.json()
        data.extend(js.get("value", []))
        url = js.get("@odata.nextLink")
    return data

# ----------------------
# ทำชื่อ column ให้ BigQuery-safe
# ----------------------
def clean_columns_for_bq(df):
    df.columns = [re.sub(r"[^\w]", "_", c).lower() for c in df.columns]
    df.columns = [
        c if c[0].isalpha() or c[0] == "_" else f"col_{i}"
        for i, c in enumerate(df.columns)
    ]
    return df

# ----------------------
# Upload ขึ้น GCS
# ----------------------
def upload_to_gcs(df, folder, filename):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    path = f"{folder}/{now_th('%Y')}/{now_th('%m')}/{now_th('%d')}/{filename}"
    blob = bucket.blob(path)

    temp_file = "temp_dim.ndjson"
    df.to_json(temp_file, orient="records", lines=True, force_ascii=False)
    blob.upload_from_filename(temp_file)
    os.remove(temp_file)

    print(f"✅ อัปโหลด → gs://{GCS_BUCKET_NAME}/{path}")

# ----------------------
# MAIN
# ----------------------
if __name__ == "__main__":
    print(f"🚀 เริ่ม FULL LOAD DIM: {now_th('%Y-%m-%d %H:%M:%S')}")

    token = get_access_token()

    dim_entities = {
        "channels": "itsm_channels",
        "kols": "itsm_kols",
        "pages": "itsm_pages",
        "products": "itsm_products"
    }

    for folder, api_name in dim_entities.items():
        print(f"\n📥 ดึงข้อมูล {api_name}")
        try:
            data = fetch_dataverse_data(token, api_name)
        except Exception as e:
            print(f"⚠️ ข้าม {api_name} → {e}")
            continue

        if not data:
            print("⚠️ ไม่มีข้อมูล")
            continue

        df = pd.DataFrame(data)
        df = clean_columns_for_bq(df)

        filename = f"{folder}_{now_th('%H%M%S')}.ndjson"
        upload_to_gcs(df, folder, filename)

    print("🎉 FULL LOAD DIM เสร็จสมบูรณ์")
