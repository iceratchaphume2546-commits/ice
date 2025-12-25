import os
import re
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import storage, bigquery
import pytz
import tempfile

# ==============================
# LOAD ENV
# ==============================
load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BQ_PROJECT = os.getenv("BQ_PROJECT", "itsm-pipeline")

STAGE_HEADER = "stage.stage_headerheader"
STAGE_LINE = "stage.stage_headerline"

# ==============================
# TIME (Bangkok)
# ==============================
def today_date():
    tz = pytz.timezone("Asia/Bangkok")
    return datetime.now(tz).date()

def yesterday_date():
    return today_date() - timedelta(days=1)

# ==============================
# GET DATAVERSE TOKEN (NEW VERSION)
# ==============================
def get_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": f"{DATAVERSE_URL}/.default",
        "grant_type": "client_credentials",
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

# ==============================
# CLEAN & SANITIZE
# ==============================
def clean_dict(d):
    new_dict = {}
    for k, v in d.items():
        new_key = k.replace("@", "").replace(".", "_")
        if isinstance(v, dict):
            new_dict[new_key] = clean_dict(v)
        elif isinstance(v, list):
            new_list = []
            for item in v:
                if isinstance(item, dict):
                    new_list.append(clean_dict(item))
                else:
                    new_list.append(item)
            new_dict[new_key] = new_list
        else:
            new_dict[new_key] = v
    return new_dict

def clean_columns_for_bq(df):
    df.columns = [re.sub(r"[^\w]", "_", c).lower() for c in df.columns]
    df.columns = [c if c[0].isalpha() or c[0]=="_" else f"col_{i}" for i,c in enumerate(df.columns)]
    return df

def sanitize_value(v):
    if isinstance(v, (dict,list)):
        return json.dumps(v, ensure_ascii=False)
    if pd.isna(v):
        return None
    return v

def sanitize_for_bigquery(df):
    for col in df.columns:
        df[col] = df[col].apply(sanitize_value)
    return df

def remove_control_chars(df):
    for col in df.columns:
        df[col] = df[col].apply(lambda x: re.sub(r"[\x00-\x1F\x7F]", "", x) if isinstance(x,str) else x)
    return df

# ==============================
# DATAVERSE FUNCTIONS
# ==============================
def fetch_dataverse(entity_set, token):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = f"{DATAVERSE_URL}/api/data/v9.2/{entity_set}"
    rows = []

    while url:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        rows.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    if not rows:
        print(f"⚠️ No data for {entity_set}")
        return pd.DataFrame()
    
    df = pd.DataFrame(rows)
    return df

# ==============================
# GCS FUNCTIONS
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
# BIGQUERY FUNCTIONS
# ==============================
def push_to_stage(df, stage_table):
    client = bigquery.Client(project=BQ_PROJECT)
    df = clean_columns_for_bq(df)
    df = sanitize_for_bigquery(df)
    df = remove_control_chars(df)
    job = client.load_table_from_dataframe(
        df,
        f"{BQ_PROJECT}.{stage_table}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()
    print(f"⬆️ Pushed to Stage {stage_table}: {len(df)} rows")

# ==============================
# PROCESS ENTITY
# ==============================
def process_entity(name, entity_set, stage_table, gcs_folder, filename, token):
    print(f"\n🚀 PROCESS {name.upper()}")
    df = fetch_dataverse(entity_set, token)
    if df.empty:
        print(f"⚠️ No data for {name}")
        return

    # Clean each row
    df = df.applymap(lambda x: x if not isinstance(x,(dict,list)) else json.dumps(x, ensure_ascii=False))
    df = df.applymap(lambda x: re.sub(r"[\x00-\x1F\x7F]", "", x) if isinstance(x,str) else x)

    upload_ndjson_to_gcs(df, gcs_folder, filename)
    push_to_stage(df, stage_table)
    print(f"✅ {name} DONE")

# ==============================
# ENTRYPOINT
# ==============================
if __name__ == "__main__":
    print(f"⏰ RUN DATE: {today_date()}")
    token = get_token()

    entities = [
        {"name":"Header", "entity_set":"itsm_adses", "stage_table":STAGE_HEADER, "gcs_folder":"ads/header", "filename":"header.ndjson"},
        {"name":"Line", "entity_set":"itsm_ads_product_lines", "stage_table":STAGE_LINE, "gcs_folder":"ads/line", "filename":"line.ndjson"}
    ]

    for e in entities:
        process_entity(**e, token=token)

    print("\n🎉 JOB SUCCESS (Dataverse → GCS → Stage)")
