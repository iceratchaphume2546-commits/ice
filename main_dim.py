import os
from dotenv import load_dotenv
from google.cloud import storage
from datetime import datetime
import pandas as pd
import tempfile
import json

# -----------------------------
# Load env
# -----------------------------
load_dotenv()
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "hongthai")
DATAVERSE_FILE = "dataverse_export.ndjson"

# -----------------------------
# GCS upload function
# -----------------------------
def upload_to_gcs(df: pd.DataFrame, base_folder: str, filename: str):
    if df.empty:
        print(f"⚠️ Skip {base_folder}/{filename} (empty dataframe)")
        return

    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    gcs_path = f"{base_folder}/{year}/{month}/{day}/{filename}"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False, encoding="utf-8") as tmp:
        df.to_json(tmp.name, orient="records", lines=True, force_ascii=False)
        temp_path = tmp.name

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(temp_path)

    os.remove(temp_path)
    print(f"✅ Uploaded → gs://{GCS_BUCKET_NAME}/{gcs_path}")

# -----------------------------
# Build DIM dataframes from source
# -----------------------------
def build_products(df: pd.DataFrame) -> pd.DataFrame:
    columns_map = {
        "itsm_product_name": "product_name",
        "itsm_id": "product_id"
    }
    df = df.rename(columns=columns_map)
    required_cols = ["product_id", "product_name"]
    if all(col in df.columns for col in required_cols):
        return df[required_cols]
    print("⚠️ product columns not found")
    return pd.DataFrame()

def build_kols(df: pd.DataFrame) -> pd.DataFrame:
    columns_map = {
        "itsm_id": "kol_id",
        "itsm_subid2": "kol_name"  # สมมติใช้ subid2 เป็นชื่อ kol
    }
    df = df.rename(columns=columns_map)
    required_cols = ["kol_id", "kol_name"]
    if all(col in df.columns for col in required_cols):
        return df[required_cols]
    print("⚠️ kol columns not found")
    return pd.DataFrame()

def build_channels(df: pd.DataFrame) -> pd.DataFrame:
    columns_map = {
        "itsm_channel_name": "channel_name",
        "itsm_id": "channel_id"
    }
    df = df.rename(columns=columns_map)
    required_cols = ["channel_id", "channel_name"]
    if all(col in df.columns for col in required_cols):
        return df[required_cols]
    print("⚠️ channel columns not found")
    return pd.DataFrame()

def build_pages(df: pd.DataFrame) -> pd.DataFrame:
    columns_map = {
        "itsm_page_name": "page_name",
        "itsm_id": "page_id"
    }
    df = df.rename(columns=columns_map)
    required_cols = ["page_id", "page_name"]
    if all(col in df.columns for col in required_cols):
        return df[required_cols]
    print("⚠️ page columns not found")
    return pd.DataFrame()

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("🚀 Start FULL LOAD 4DIM → GCS")

    if not os.path.exists(DATAVERSE_FILE):
        raise FileNotFoundError(f"{DATAVERSE_FILE} not found in current folder.")

    # Load dataverse_export.ndjson
    print(f"📥 Load {DATAVERSE_FILE}")
    df_source = pd.read_json(DATAVERSE_FILE, lines=True)
    print(f"🔎 Source columns: {list(df_source.columns)}")

    # Build DIMs
    df_products = build_products(df_source)
    df_kols = build_kols(df_source)
    df_channels = build_channels(df_source)
    df_pages = build_pages(df_source)

    # Upload to GCS
    upload_to_gcs(df_products, "products", "products.ndjson")
    upload_to_gcs(df_kols, "kols", "kols.ndjson")
    upload_to_gcs(df_channels, "channels", "channels.ndjson")
    upload_to_gcs(df_pages, "pages", "pages.ndjson")

    print("🎉 FULL LOAD 4DIM FINISHED")

