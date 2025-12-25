import os
import json
import requests
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv
import tempfile

# =============================
# LOAD ENV (local)
# =============================
if os.path.exists(".env"):
    load_dotenv()

# =============================
# CONFIG
# =============================
BASE_URL = "https://itsm.crm5.dynamics.com/api/data/v9.2"
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME")

ENTITIES = {
    "products": "itsm_ads_products",
    "channels": "itsm_ads_channels",
    "pages": "itsm_ads_pages",
    "kols": "itsm_ads_kols",
}

# =============================
# GET DATAVERSE TOKEN
# =============================
def get_token():
    url = f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}/oauth2/v2.0/token"
    data = {
        "client_id": os.getenv("CLIENT_ID"),
        "client_secret": os.getenv("CLIENT_SECRET"),
        "scope": "https://itsm.crm5.dynamics.com/.default",
        "grant_type": "client_credentials",
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

# =============================
# CLEAN FIELD NAMES
# =============================
def clean_dict(d):
    """
    Replace invalid characters in keys for BigQuery:
    - remove '@'
    - replace '.' with '_'
    - recursively handle nested dicts
    """
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

# =============================
# FETCH + UPLOAD (FULL LOAD)
# =============================
def full_load(entity_name, entity_set, token):
    print(f"📥 Fetch {entity_name}")

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    url = f"{BASE_URL}/{entity_set}"
    rows = []

    while url:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        rows.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    if not rows:
        print(f"⚠️ No data: {entity_name}")
        return

    df = pd.DataFrame(rows)

    # ใช้ temp folder ของระบบ
    local_path = os.path.join(tempfile.gettempdir(), f"{entity_name}.ndjson")

    with open(local_path, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            clean_row = clean_dict(row.to_dict())  # ทำความสะอาด field names
            f.write(json.dumps(clean_row, ensure_ascii=False) + "\n")

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    # 🔥 อัปโหลดตรงๆ ในโฟลเดอร์ของ entity (ไม่มีปี/เดือน/วัน)
    blob_path = f"{entity_name}/{entity_name}.ndjson"
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)

    print(f"✅ Uploaded {entity_name} ({len(df)} rows) to {blob_path}")

# =============================
# MAIN
# =============================
if __name__ == "__main__":
    print("🚀 START FULL LOAD DIM")

    token = get_token()

    for name, entity in ENTITIES.items():
        full_load(name, entity, token)

    print("🎉 DONE")
