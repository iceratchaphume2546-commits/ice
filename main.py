import os
import pandas as pd
from google.cloud import storage
from test import get_access_token, fetch_dataverse_data, dim  # import ฟังก์ชันจาก test.py
from dotenv import load_dotenv
from datetime import datetime
import pytz
import re

# ----------------------
# โหลด .env
# ----------------------
load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "hongthai")
CREDENTIAL_PATH = os.getenv(
    "GCS_CREDENTIAL_JSON",
    "C:/Users/User/Desktop/gitgit/ice/dataverse/itsm-pipeline-bac89c675c5e.json"
)

# -----------------------------
# ฟังก์ชันเวลาแบบ Bangkok
# -----------------------------
def now_th(fmt=None):
    tz = pytz.timezone("Asia/Bangkok")
    now = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(tz)
    return now.strftime(fmt) if fmt else now

YEAR = now_th("%Y")
MONTH = now_th("%m")
DAY = now_th("%d")

# -----------------------------
# อัปโหลด DataFrame ขึ้น GCS
# -----------------------------
def upload_to_gcs(df, folder, filename):
    # ข้ามการอัปโหลดไฟล์บางไฟล์
    skip_files = ["product_lines.ndjson", "itsm_adses.ndjson"]
    if filename.lower() in skip_files:
        print(f"ข้ามการอัปโหลด {filename}")
        return

    path = f"{folder}/{YEAR}/{MONTH}/{DAY}/{filename}"
    client = storage.Client.from_service_account_json(CREDENTIAL_PATH)
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(path)

    temp = "temp.ndjson"
    df.to_json(temp, orient="records", lines=True, force_ascii=False)
    blob.upload_from_filename(temp)
    print(f"อัปโหลดสำเร็จ → gs://{GCS_BUCKET_NAME}/{path}")

# -----------------------------
# ปรับชื่อคอลัมน์ให้ BigQuery-safe
# -----------------------------
def clean_columns_for_bq(df):
    df.columns = [re.sub(r"[^\w]", "_", c).lower() for c in df.columns]
    df.columns = [c if c[0].isalpha() or c[0]=="_" else f"col_{i}" 
                  for i, c in enumerate(df.columns)]
    return df

# -----------------------------
# MAIN PROCESS สำหรับรัน entity
# -----------------------------
def run(folder_name, api_name):
    print(f"\n===== เริ่มประมวลผล: {folder_name.upper()} =====")
    token = get_access_token()
    data = fetch_dataverse_data(token, api_name)
    df = pd.DataFrame(data)

    df = clean_columns_for_bq(df)

    print(f"จำนวนแถว: {len(df)}, จำนวนคอลัมน์: {len(df.columns)}")
    upload_to_gcs(df, folder_name, f"{folder_name.split('/')[-1]}.ndjson")

# -----------------------------
# รันจริง
# -----------------------------
if __name__ == "__main__":

    # ---------------- HEADER ----------------
    run(
        folder_name="ads/header",
        api_name="itsm_adses"
    )

    # ---------------- LINE ----------------
    run(
        folder_name="ads/line",
        api_name="itsm_ads_product_lines"
    )

    # ---------------- entity ใหม่ทั้งหมด ----------------
    dim()  # ดึง products, channels, pages, kols


