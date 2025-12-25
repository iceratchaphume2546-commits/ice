import os
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
from google.cloud import storage, bigquery
import re
import json

# ----------------------
# LOAD ENV
# ----------------------
load_dotenv()

# ----------------------
# ENV
# ----------------------
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "hongthai")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")

BQ_PROJECT = os.getenv("BQ_PROJECT", "itsm-pipeline")
STAGE_HEADER = "stage.stage_header"
STAGE_LINE = "stage.stage_line"

# -----------------------------
# TIME
# -----------------------------
tz = pytz.timezone("Asia/Bangkok")
yesterday = (datetime.now(tz) - timedelta(days=1)).date()

# -----------------------------
# ACCESS TOKEN
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
# FETCH DATAVERSE (NO FILTER)
# -----------------------------
def fetch_dataverse_data(token, api_name):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    url = f"{DATAVERSE_URL}/api/data/v9.2/{api_name}"
    data = []

    while url:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        js = r.json()
        data.extend(js.get("value", []))
        url = js.get("@odata.nextLink")

    return data

# -----------------------------
# CLEAN COLUMNS
# -----------------------------
def clean_columns_for_bq(df):
    df.columns = [re.sub(r"[^\w]", "_", c).lower() for c in df.columns]
    return df

# -----------------------------
# SANITIZE
# -----------------------------
def sanitize_value(v):
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    if pd.isna(v):
        return None
    return v

def sanitize_df(df):
    for c in df.columns:
        df[c] = df[c].apply(sanitize_value)
    return df

def remove_control_chars(df):
    def clean(x):
        if isinstance(x, str):
            return re.sub(r"[\x00-\x1F\x7F]", "", x)
        return x

    for c in df.columns:
        df[c] = df[c].apply(clean)
    return df

# -----------------------------
# UPLOAD TO GCS (UNCHANGED)
# -----------------------------
def upload_to_gcs(df, folder, filename):
    path = f"{folder}/{filename}"
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(path)

    temp_file = "temp.ndjson"
    with open(temp_file, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")

    blob.upload_from_filename(temp_file)
    os.remove(temp_file)

    print(f"✅ upload → gs://{GCS_BUCKET_NAME}/{path}")

# -----------------------------
# PUSH TO STAGE (APPEND ONLY)
# -----------------------------
def push_to_stage(df, stage_table):
    if df.empty:
        print("⚠️ ไม่มีข้อมูล → ข้าม stage")
        return

    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{stage_table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )
    job.result()

    print(f"✅ append {len(df)} rows → {stage_table}")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print(f"🚀 Dataverse → GCS → Stage (yesterday {yesterday})")

    token = get_access_token()

    entities = {
        "ads/header": "itsm_adses",
        "ads/line": "itsm_ads_product_lines"
    }

    for folder, api_name in entities.items():
        print(f"\n📥 ดึงข้อมูล {api_name}")
        data = fetch_dataverse_data(token, api_name)

        if not data:
            print("⚠️ ไม่มีข้อมูลจาก API")
            continue

        df = pd.DataFrame(data)
        df = clean_columns_for_bq(df)

        # 👉 filter yesterday ที่นี่ (optional)
        df["modifiedon"] = pd.to_datetime(df["modifiedon"], errors="coerce")
        df = df[df["modifiedon"].dt.date == yesterday]

        if df.empty:
            print("⚠️ ไม่มีข้อมูลของเมื่อวาน")
            continue

        df = sanitize_df(df)
        df = remove_control_chars(df)

        filename = f"{folder.split('/')[-1]}.ndjson"
        upload_to_gcs(df, folder, filename)

        stage_table = STAGE_HEADER if "header" in folder else STAGE_LINE
        push_to_stage(df, stage_table)

    print("\n🎉 DONE")
