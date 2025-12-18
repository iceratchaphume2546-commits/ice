import os
import json
import requests
import pandas as pd
from datetime import datetime
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
BASE_URL = "https://itsmdev.crm5.dynamics.com/api/data/v9.2"
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
        "scope": "https://itsmdev.crm5.dynamics.com/.default",
        "grant_type": "client_credentials",
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

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

    today = datetime.now().strftime("%Y/%m/%d")

    # ใช้ temp folder ของระบบ
    local_path = os.path.join(tempfile.gettempdir(), f"{entity_name}.ndjson")

    with open(local_path, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    
    # 🔥 แก้ path ตาม request: root folder = entity_name
    blob_path = f"{entity_name}/{today}/{entity_name}.ndjson"
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
