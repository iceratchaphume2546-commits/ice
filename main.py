import os
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime
import pytz
from google.cloud import storage
import re
import json

# ----------------------
# โหลด .env (local ใช้ / Cloud Run ไม่กระทบ)
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
# clean record ให้ BigQuery อ่านได้
# -----------------------------
def clean_record_for_bq(record: dict):
    cleaned = {}
    for k, v in record.items():
        if k is None or k == "":
            continue

        # แปลง nested object เป็น string
        if isinstance(v, (dict, list)):
            cleaned[k] = json.dumps(v, ensure_ascii=False)
        else:
            cleaned[k] = v

    return cleaned

# -----------------------------
# อัปโหลด DataFrame ขึ้น GCS (NDJSON แบบ safe)
# -----------------------------
def upload_to_gcs(df, folder, filename):
    skip_files = ["product_lines.ndjson", "itsm_adses.ndjson"]
    if filename.lower() in skip_files:
        print(f"⏭ ข้ามการอัปโหลด {filename}")
        return

    path = f"{folder}/{YEAR}/{MONTH}/{DAY}/{filename}"

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(path)

    temp_file = "temp.ndjson"

    # เขียน NDJSON ทีละ row (BigQuery-safe)
    with open(temp_file, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            record = clean_record_for_bq(row.to_dict())
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    blob.upload_from_filename(temp_file)
    os.remove(temp_file)

    print(f" อัปโหลดสำเร็จ → gs://{GCS_BUCKET_NAME}/{path}")

# -----------------------------
# MAIN (Cloud Run Job)
# -----------------------------
if __name__ == "__main__":
    print(" เริ่มรัน Dataverse → GCS")

    token = get_access_token()

    entities = {
        "ads/header": "itsm_adses",
        "ads/line": "itsm_ads_product_lines"
    }

    for folder, api_name in entities.items():
        print(f"\n กำลังดึงข้อมูล {api_name}")
        data = fetch_dataverse_data(token, api_name)

        if not data:
            print(" ไม่มีข้อมูล")
            continue

        df = pd.DataFrame(data)
        df = clean_columns_for_bq(df)

        filename = f"{folder.split('/')[-1]}.ndjson"
        upload_to_gcs(df, folder, filename)

    print("🎉 รันเสร็จสมบูรณ์ – Job จบการทำงาน")
