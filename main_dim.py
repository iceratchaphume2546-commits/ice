import os
from dotenv import load_dotenv

load_dotenv()  # 👈 ต้องอยู่ก่อนใช้ storage.Client()

from google.cloud import storage
from datetime import datetime
import pandas as pd
import tempfile


# -----------------------------
# Load env
# -----------------------------
load_dotenv()
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

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

    # ✅ temp file ที่ใช้ได้ทุก OS
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".ndjson",
        delete=False,
        encoding="utf-8"
    ) as tmp:
        df.to_json(tmp.name, orient="records", lines=True, force_ascii=False)
        temp_path = tmp.name

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(temp_path)

    os.remove(temp_path)
    print(f"✅ Uploaded → gs://{GCS_BUCKET_NAME}/{gcs_path}")

# -----------------------------
# FETCH FUNCTIONS (DIM)
# -----------------------------
def fetch_products() -> pd.DataFrame:
    print("📥 Fetch products")
    return pd.DataFrame([
        {"product_id": 1, "product_name": "Product A"},
        {"product_id": 2, "product_name": "Product B"},
    ])

def fetch_kols() -> pd.DataFrame:
    print("📥 Fetch kols")
    return pd.DataFrame([
        {"kol_id": 101, "kol_name": "KOL A"},
        {"kol_id": 102, "kol_name": "KOL B"},
    ])

def fetch_channels() -> pd.DataFrame:
    print("📥 Fetch channels")
    return pd.DataFrame([
        {"channel_id": 201, "channel_name": "Facebook"},
        {"channel_id": 202, "channel_name": "TikTok"},
    ])

def fetch_pages() -> pd.DataFrame:
    print("📥 Fetch pages")
    return pd.DataFrame([
        {"page_id": 301, "page_name": "Page A"},
        {"page_id": 302, "page_name": "Page B"},
    ])

# -----------------------------
# MAIN (FULL LOAD ONCE)
# -----------------------------
if __name__ == "__main__":
    print("🚀 Start FULL LOAD DIM → GCS")

    df_products = fetch_products()
    df_kols = fetch_kols()
    df_channels = fetch_channels()
    df_pages = fetch_pages()

    upload_to_gcs(df_products, "products", "products.ndjson")
    upload_to_gcs(df_kols, "kols", "kols.ndjson")
    upload_to_gcs(df_channels, "channels", "channels.ndjson")
    upload_to_gcs(df_pages, "pages", "pages.ndjson")

    print("🎉 FULL LOAD DIM FINISHED")
