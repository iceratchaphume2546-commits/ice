import os
import requests
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import pytz
from google.cloud import storage

load_dotenv()
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")

# -----------------------------
# ฟังก์ชันเวลาแบบ Bangkok
# -----------------------------
def now_th_iso():
    tz = pytz.timezone("Asia/Bangkok")
    now = datetime.now(tz)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")  # format ISO 8601

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
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

# ============================================================
# ฟังก์ชันดึงข้อมูลจาก Dataverse แบบกำหนด entity ได้
# ============================================================
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
        json_data = r.json()
        data.extend(json_data.get("value", []))
        url = json_data.get("@odata.nextLink")

    return data

# -----------------------------
# ฟังก์ชันปรับชื่อคอลัมน์
# -----------------------------
def clean_columns(df):
    import re
    def fix(name):
        name = name.strip().lower()
        name = re.sub(r"[^\w]+", "_", name)
        name = re.sub(r"_+", "_", name)
        return name.strip("_")
    return df.rename(columns=fix)

# -----------------------------
# ฟังก์ชันอัปโหลดขึ้น GCS
# -----------------------------
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "hongthai")
CREDENTIAL_PATH = os.getenv("GCS_CREDENTIAL_JSON",
                            "C:/Users/User/Desktop/gitgit/ice/dataverse/itsm-pipeline-bac89c675c5e.json")

YEAR = datetime.now().year
MONTH = f"{datetime.now().month:02d}"
DAY = f"{datetime.now().day:02d}"

def upload_to_gcs(df, folder, filename):
    path = f"{folder}/{YEAR}/{MONTH}/{DAY}/{filename}"
    client = storage.Client.from_service_account_json(CREDENTIAL_PATH)
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(path)
    temp = "temp.ndjson"
    df.to_json(temp, orient="records", lines=True, force_ascii=False)
    blob.upload_from_filename(temp)
    print(f"อัปโหลดสำเร็จ → gs://{GCS_BUCKET_NAME}/{path}")

# -----------------------------
# ฟังก์ชันรัน entity เดียว
# -----------------------------
# -----------------------------
# ฟังก์ชันรัน entity เดียว
# -----------------------------
def run_entity(api_name):
    # ใช้ชื่อ entity เป็นโฟลเดอร์หลัก (ตัด "itsm_ads_" ทิ้ง)
    folder_name = api_name.replace("itsm_ads_", "")
    
    print(f"\n===== เริ่มประมวลผล: {folder_name.upper()} =====")
    
    token = get_access_token()
    data = fetch_dataverse_data(token, api_name)
    df = pd.DataFrame(data)
    df = clean_columns(df)
    print(f"จำนวนแถว: {len(df)}, จำนวนคอลัมน์: {len(df.columns)}")
    
    upload_to_gcs(df, folder_name, f"{folder_name}.ndjson")


# -----------------------------
# ฟังก์ชันรันทุก entity ภายใต้ dim()
# -----------------------------
def dim():
    entities = [
        "itsm_ads_products",
        "itsm_ads_channels",
        "itsm_ads_pages",
        "itsm_ads_kols"
    ]

    for api_name in entities:
        run_entity(api_name)


# -----------------------------
# รันจริง
# -----------------------------
if __name__ == "__main__":
    dim()
