import os
import re
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import storage, bigquery

# ==============================
# LOAD ENV
# ==============================
load_dotenv()
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "hongthai")
BQ_PROJECT = os.getenv("BQ_PROJECT", "itsm-pipeline")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
SCOPE = os.getenv("SCOPE")

# ==============================
# ENTITIES
# ==============================
ENTITIES = {
    "header": {
        "dataverse_entity": "itsm_adses",
        "gcs_folder": "ads/header",
        "stage_table": "stage.stage_headerheader",
        "filename": "header.ndjson",
        "key": "itsm_adsesid",
    },
    "line": {
        "dataverse_entity": "itsm_ads_product_lines",
        "gcs_folder": "ads/line",
        "stage_table": "stage.stage_headerline",
        "filename": "line.ndjson",
        "key": "itsm_ads_product_linesid",
    }
}

# ==============================
# TIME
# ==============================
def today_date():
    return datetime.now().date()

def yesterday_date():
    return today_date() - timedelta(days=1)

# ==============================
# GET DATAVERSE TOKEN
# ==============================
def get_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPE,
        "grant_type": "client_credentials",
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

# ==============================
# CLEAN DICT
# ==============================
def clean_dict(d):
    new_dict = {}
    for k, v in d.items():
        new_key = k.replace("@", "").replace(".", "_")
        if isinstance(v, dict):
            new_dict[new_key] = clean_dict(v)
        elif isinstance(v, list):
            new_list = [clean_dict(i) if isinstance(i, dict) else i for i in v]
            new_dict[new_key] = new_list
        else:
            new_dict[new_key] = v
    return new_dict

# ==============================
# FETCH DATAVERSE
# ==============================
def fetch_dataverse(entity_name, token):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = f"{DATAVERSE_URL}/api/data/v9.2/{entity_name}"
    rows = []
    while url:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        rows.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
    df = pd.DataFrame(rows)
    return df

# ==============================
# CLEAN & SANITIZE FOR BQ
# ==============================
def sanitize_for_bigquery(df):
    for col in df.columns:
        df[col] = df[col].map(lambda x: x if not isinstance(x, (dict, list)) else json.dumps(x, ensure_ascii=False))
        df[col] = df[col].map(lambda x: re.sub(r"[\x00-\x1F\x7F]", "", x) if isinstance(x, str) else x)
    df.columns = [re.sub(r"[^\w]", "_", c).lower() for c in df.columns]
    df.columns = [c if c[0].isalpha() or c[0] == "_" else f"col_{i}" for i, c in enumerate(df.columns)]
    return df

# ==============================
# UPLOAD GCS
# ==============================
def upload_ndjson_to_gcs(df, gcs_folder, filename):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    today = today_date()
    blob_path = f"{gcs_folder}/{today.year}/{today.month:02}/{today.day:02}/{filename}"
    blob = bucket.blob(blob_path)
    blob.upload_from_string(df.to_json(orient="records", lines=True, force_ascii=False), content_type="application/json")
    print(f"📦 Uploaded to GCS: gs://{GCS_BUCKET_NAME}/{blob_path}")

# ==============================
# PUSH TO BIGQUERY STAGE
# ==============================
def push_to_stage(df, stage_table):
    client = bigquery.Client(project=BQ_PROJECT)
    job = client.load_table_from_dataframe(df, stage_table, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    job.result()
    print(f"⬆️ Pushed to Stage {stage_table}: {len(df)} rows")

# ==============================
# PROCESS ENTITY
# ==============================
def process_entity(name, config, token):
    print(f"\n🚀 PROCESS {name.upper()}")
    df = fetch_dataverse(config["dataverse_entity"], token)
    if df.empty:
        print(f"⚠️ No data for {name}")
        return
    # Clean data
    df = df.applymap(lambda x: clean_dict(x) if isinstance(x, dict) else x)
    df = sanitize_for_bigquery(df)
    # Upload
    upload_ndjson_to_gcs(df, config["gcs_folder"], config["filename"])
    push_to_stage(df, config["stage_table"])
    print(f"✅ {name} DONE")

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    print(f"⏰ RUN DATE: {today_date()}")
    token = get_token()
    for name, cfg in ENTITIES.items():
        process_entity(name, cfg, token)
    print("\n🎉 JOB SUCCESS (Dataverse → GCS → Stage)")
