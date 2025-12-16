import os
from dotenv import load_dotenv
from google.cloud import storage
from datetime import datetime
import pandas as pd
import tempfile

# -----------------------------
# Load env
# -----------------------------
load_dotenv()
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "hongthai")

# -----------------------------
# GCS upload (FULL LOAD)
# -----------------------------
def upload_to_gcs(df: pd.DataFrame, base_folder: str, filename: str):
    if df.empty:
        print(f"⚠️ Skip {filename} (empty dataframe)")
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
# Load dataverse_export.ndjson
# -----------------------------
DATAVERSE_FILE = "dataverse_export.ndjson"
DATAVERSE_GCS_PATH = "dataverse_export.ndjson"

def load_dataverse():
    # 1️⃣ Try local file first
    if os.path.exists(DATAVERSE_FILE):
        print(f"📥 Load {DATAVERSE_FILE} from local")
        return pd.read_json(DATAVERSE_FILE, lines=True)
    # 2️⃣ If not found, try GCS
    print(f"📥 Load {DATAVERSE_GCS_PATH} from GCS")
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(DATAVERSE_GCS_PATH)
    with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as tmp:
        try:
            blob.download_to_file(tmp)
        except Exception as e:
            raise FileNotFoundError(f"{DATAVERSE_GCS_PATH} not found or cannot be loaded. {e}")
        tmp_path = tmp.name
    return pd.read_json(tmp_path, lines=True)

# -----------------------------
# BUILD DIM DATAFRAMES
# -----------------------------
def build_products(df):
    col_map = {"itsm_product_name": "product_name", "itsm_id": "product_id"}
    if all(k in df.columns for k in col_map):
        return df.rename(columns=col_map)[list(col_map.values())]
    print("⚠️ product columns not found")
    return pd.DataFrame()

def build_kols(df):
    col_map = {"itsm_affiliatelink": "kol_name", "itsm_subid4": "kol_id"}
    if all(k in df.columns for k in col_map):
        return df.rename(columns=col_map)[list(col_map.values())]
    print("⚠️ kol columns not found")
    return pd.DataFrame()

def build_channels(df):
    col_map = {"itsm_channel_name": "channel_name", "itsm_subid2": "channel_id"}
    if all(k in df.columns for k in col_map):
        return df.rename(columns=col_map)[list(col_map.values())]
    print("⚠️ channel columns not found")
    return pd.DataFrame()

def build_pages(df):
    col_map = {"itsm_page_name": "page_name", "itsm_subid3": "page_id"}
    if all(k in df.columns for k in col_map):
        return df.rename(columns=col_map)[list(col_map.values())]
    print("⚠️ page columns not found")
    return pd.DataFrame()

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("🚀 Start FULL LOAD 4DIM → GCS")

    df_source = load_dataverse()
    print(f"🔎 Source columns: {list(df_source.columns)}")

    df_products = build_products(df_source)
    df_kols = build_kols(df_source)
    df_channels = build_channels(df_source)
    df_pages = build_pages(df_source)

    upload_to_gcs(df_products, "products", "products.ndjson")
    upload_to_gcs(df_kols, "kols", "kols.ndjson")
    upload_to_gcs(df_channels, "channels", "channels.ndjson")
    upload_to_gcs(df_pages, "pages", "pages.ndjson")

    print("🎉 FULL LOAD 4DIM FINISHED")

