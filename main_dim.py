import os
import pandas as pd
from datetime import datetime
import pytz
from google.cloud import storage
import re

# ----------------------
# CONFIG
# ----------------------
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "hongthai")
SOURCE_FILE = "dataverse_export.ndjson"

# ----------------------
# Time (Asia/Bangkok)
# ----------------------
def now_th(fmt):
    tz = pytz.timezone("Asia/Bangkok")
    return datetime.now(tz).strftime(fmt)

YEAR = now_th("%Y")
MONTH = now_th("%m")
DAY = now_th("%d")

# ----------------------
# Load source (LOCAL เหมือน main.py logic)
# ----------------------
def load_source():
    print("📥 Load dataverse_export.ndjson (local)")

    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"{SOURCE_FILE} not found")

    df = pd.read_json(SOURCE_FILE, lines=True)
    print("🔎 Source columns:", list(df.columns))
    return df

# ----------------------
# Clean column names (BigQuery-safe)
# ----------------------
def clean_columns(df):
    df = df.copy()
    df.columns = [re.sub(r"[^\w]", "_", c).lower() for c in df.columns]
    df.columns = [
        c if c[0].isalpha() or c[0] == "_" else f"col_{i}"
        for i, c in enumerate(df.columns)
    ]
    return df

# ----------------------
# Build DIM (ฟังก์ชันเดียว ใช้ loop)
# ----------------------
DIM_CONFIG = {
    "products": {
        "id": "itsm_ads_product_lineid",
        "name": "itsm_product_name",
    },
    "channels": {
        "id": "_itsm_channel_value",
        "name": "itsm_channel_name",
    },
    "pages": {
        "id": "_itsm_pagename_value",
        "name": "itsm_page_name",
    },
    "kols": {
        "id": "_itsm_agentpost_value",
        "name": "_itsm_agentpost_value",  # ตอนนี้ยังไม่มีชื่อจริง
    },
}

def build_dim(df, dim_name, cfg):
    id_col = cfg["id"]
    name_col = cfg["name"]

    if id_col not in df.columns:
        print(f"⚠️ Skip {dim_name} (missing {id_col})")
        return pd.DataFrame()

    cols = [id_col]
    if name_col in df.columns:
        cols.append(name_col)

    dim_df = (
        df[cols]
        .dropna()
        .drop_duplicates()
        .rename(columns={
            id_col: f"{dim_name[:-1]}_id",
            name_col: f"{dim_name[:-1]}_name"
        })
    )

    return clean_columns(dim_df)

# ----------------------
# Upload to GCS (FULL LOAD, overwrite)
# ----------------------
def upload_to_gcs(df, folder):
    if df.empty:
        print(f"⚠️ Skip {folder} (empty)")
        return

    path = f"{folder}/{YEAR}/{MONTH}/{DAY}/{folder}.ndjson"

    client = storage.Client()  # ADC เหมือน main.py
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(path)

    temp_file = "temp.ndjson"
    df.to_json(
        temp_file,
        orient="records",
        lines=True,
        force_ascii=False
    )

    blob.upload_from_filename(temp_file)
    os.remove(temp_file)

    print(f"✅ Uploaded → gs://{GCS_BUCKET_NAME}/{path}")

# ----------------------
# MAIN
# ----------------------
if __name__ == "__main__":
    print("🚀 Start FULL LOAD DIM")

    df_source = load_source()

    for dim_name, cfg in DIM_CONFIG.items():
        df_dim = build_dim(df_source, dim_name, cfg)
        upload_to_gcs(df_dim, dim_name)

    print("🎉 FULL LOAD DIM FINISHED")
