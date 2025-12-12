import os
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime
import pytz
from google.cloud import storage
import re
from flask import Flask, jsonify

# ----------------------
# โหลด .env
# ----------------------
load_dotenv()  # ถ้า .env อยู่ใน ice จะเจออัตโนมัติ

# ----------------------
# Environment variables
# ----------------------
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "hongthai")
CREDENTIAL_PATH = os.getenv(
    "GCS_CREDENTIAL_JSON",
    r"C:\Users\User\Desktop\gitgit\ice\itsm-pipeline-bac89c675c5e.json"
)

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
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")  # format ISO 8601

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
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

# -----------------------------
# ดึงข้อมูลจาก Dataverse แบบกำหนด entity
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
        json_data = r.json()
        data.extend(json_data.get("value", []))
        url = json_data.get("@odata.nextLink")

    return data

# -----------------------------
# ฟังก์ชันปรับชื่อคอลัมน์ให้ BigQuery-safe
# -----------------------------
def clean_columns_for_bq(df):
    df.columns = [re.sub(r"[^\w]", "_", c).lower() for c in df.columns]
    df.columns = [c if c[0].isalpha() or c[0]=="_" else f"col_{i}" 
                  for i, c in enumerate(df.columns)]
    return df

def clean_columns(df):
    def fix(name):
        name = name.strip().lower()
        name = re.sub(r"[^\w]+", "_", name)
        name = re.sub(r"_+", "_", name)
        return name.strip("_")
    return df.rename(columns=fix)

# -----------------------------
# ฟังก์ชันอัปโหลด DataFrame ขึ้น GCS
# -----------------------------
def upload_to_gcs(df, folder, filename):
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
# ฟังก์ชันรัน entity เดียว
# -----------------------------
def run_entity(api_name):
    folder_name = api_name.replace("itsm_ads_", "")
    print(f"\n===== เริ่มประมวลผล: {folder_name.upper()} =====")
    token = get_access_token()
    data = fetch_dataverse_data(token, api_name)
    df = pd.DataFrame(data)
    df = clean_columns(df)
    print(f"จำนวนแถว: {len(df)}, จำนวนคอลัมน์: {len(df.columns)}")
    upload_to_gcs(df, folder_name, f"{folder_name}.ndjson")

def run(folder_name, api_name):
    print(f"\n===== เริ่มประมวลผล: {folder_name.upper()} =====")
    token = get_access_token()
    data = fetch_dataverse_data(token, api_name)
    df = pd.DataFrame(data)
    df = clean_columns_for_bq(df)
    print(f"จำนวนแถว: {len(df)}, จำนวนคอลัมน์: {len(df.columns)}")
    upload_to_gcs(df, folder_name, f"{folder_name.split('/')[-1]}.ndjson")

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
# Flask app สำหรับ Cloud Run Scheduler
# -----------------------------
app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def run_all():
    try:
        # รันตาม script เดิม
        run("ads/header", "itsm_adses")
        run("ads/line", "itsm_ads_product_lines")
        dim()
        return jsonify({"status": "success", "message": "Dataverse job finished!"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# -----------------------------
# รัน Flask server
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
