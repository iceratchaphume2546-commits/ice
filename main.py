import os
import pandas as pd
import requests
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv
import pytz

# ================================
# Timezone (Thailand)
# ================================
TH_TZ = pytz.timezone("Asia/Bangkok")

def now_th():
    return datetime.now(TH_TZ)

# ================================
# Load ENV
# ================================
load_dotenv()

DATAVERSE_URL = os.getenv("DATAVERSE_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
GCS_BUCKET = os.getenv("GCS_BUCKET", "hongthai")

# ================================
# Entities Config
# ================================
ENTITIES = [
    {
        "name": "itsm_adses",
        "gcs_folder": "ads/header",
        "file_name": "header.ndjson"
    },
    {
        "name": "itsm_ads_product_lines",
        "gcs_folder": "ads/line",
        "file_name": "line.ndjson"
    }
]

# ================================
# Get Dataverse token
# ================================
def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": f"{DATAVERSE_URL}/.default",
        "grant_type": "client_credentials"
    }
    r = requests.post(url, data=payload)
    r.raise_for_status()
    return r.json()["access_token"]

# ================================
# Fetch data from Dataverse (FULL LOAD)
# ================================
def fetch_dataverse(entity_name, token):
    headers = {
        "Authorization": f"Bearer {token}",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Accept": "application/json"
    }

    url = f"{DATAVERSE_URL}/api/data/v9.2/{entity_name}"
    all_rows = []
    page = 1

    while url:
        r = requests.get(url, headers=headers)
        r.raise_for_status()

        res = r.json()
        rows = res.get("value", [])
        all_rows.extend(rows)

        print(f"📄 {entity_name} page {page} rows: {len(rows)}")

        url = res.get("@odata.nextLink")
        page += 1

    df = pd.DataFrame(all_rows)
    print(f"📊 {entity_name} TOTAL rows fetched: {len(df)}")
    return df

# ================================
# Clean DataFrame (BigQuery-safe)
# ================================
def clean_df(df):
    df = df.copy()
    df.columns = [
        c.replace("@", "_")
         .replace(".", "_")
         .replace("-", "_")
        for c in df.columns
    ]
    df.dropna(how="all", inplace=True)
    return df

# ================================
# Upload NDJSON to GCS
# ================================
def upload_to_gcs(df, folder_path, file_name):
    if df.empty:
        print(f"⚠️ No data to upload for {file_name}")
        return None

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    today = now_th()
    gcs_path = (
        f"{folder_path}/"
        f"{today.year}/"
        f"{today.month:02d}/"
        f"{today.day:02d}/"
        f"{file_name}"
    )

    blob = bucket.blob(gcs_path)
    blob.upload_from_string(
        df.to_json(orient="records", lines=True),
        content_type="application/json"
    )

    print(f"📦 Uploaded NDJSON → gs://{GCS_BUCKET}/{gcs_path}")
    print(f"📏 Rows written: {len(df)}")
    return gcs_path

# ================================
# Main
# ==================================
if __name__ == "__main__":
    print("⏰ RUN PIPELINE (Asia/Bangkok)")

    token = get_access_token()

    for e in ENTITIES:
        print(f"\n🚀 PROCESS {e['name'].upper()}")

        df = fetch_dataverse(e["name"], token)
        df = clean_df(df)

        upload_to_gcs(
            df,
            e["gcs_folder"],
            e["file_name"]
        )

        print(f"✅ {e['name'].upper()} DONE")
