import os
from dotenv import load_dotenv
from google.cloud import storage
from datetime import datetime
import pandas as pd
import tempfile

# -----------------------------
# ENV
# -----------------------------
load_dotenv()
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

# -----------------------------
# GCS upload
# -----------------------------
def upload_to_gcs(df: pd.DataFrame, base_folder: str, filename: str):
    now = datetime.now()
    gcs_path = f"{base_folder}/{now:%Y/%m/%d}/{filename}"

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
    bucket.blob(gcs_path).upload_from_filename(temp_path)

    os.remove(temp_path)
    print(f"✅ Uploaded → gs://{GCS_BUCKET_NAME}/{gcs_path}")

# -----------------------------
# LOAD SOURCE
# -----------------------------
def load_source(path: str) -> pd.DataFrame:
    print("📥 Load dataverse_export.ndjson")
    df = pd.read_json(path, lines=True)
    print("🔎 Source columns:", list(df.columns))
    return df

# -----------------------------
# BUILD DIMs
# -----------------------------
def build_products(df):
    if "itsm_product_name" not in df.columns:
        print("⚠️ product columns not found")
        return pd.DataFrame(columns=["product_id", "product_name"])

    return (
        df[["itsm_product_name"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(product_id=lambda x: x.index + 1)
        .rename(columns={"itsm_product_name": "product_name"})
        [["product_id", "product_name"]]
    )

def build_channels(df):
    if "itsm_channel_name" not in df.columns:
        print("⚠️ channel columns not found")
        return pd.DataFrame(columns=["channel_id", "channel_name"])

    return (
        df[["itsm_channel_name"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(channel_id=lambda x: x.index + 1)
        .rename(columns={"itsm_channel_name": "channel_name"})
        [["channel_id", "channel_name"]]
    )

def build_pages(df):
    if "itsm_page_name" not in df.columns:
        print("⚠️ page columns not found")
        return pd.DataFrame(columns=["page_id", "page_name"])

    return (
        df[["itsm_page_name"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(page_id=lambda x: x.index + 1)
        .rename(columns={"itsm_page_name": "page_name"})
        [["page_id", "page_name"]]
    )

def build_kols(df):
    """
    ตอนนี้ยังไม่มี kol ใน source
    → สร้าง DIM เปล่าพร้อม schema ไว้ก่อน
    """
    print("🧩 Build kols (prepare for future)")
    return pd.DataFrame(columns=["kol_id", "kol_name"])

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("🚀 Start FULL LOAD DIM → GCS")

    df_source = load_source("dataverse_export.ndjson")

    df_products = build_products(df_source)
    df_kols = build_kols(df_source)
    df_channels = build_channels(df_source)
    df_pages = build_pages(df_source)

    upload_to_gcs(df_products, "products", "products.ndjson")
    upload_to_gcs(df_kols, "kols", "kols.ndjson")
    upload_to_gcs(df_channels, "channels", "channels.ndjson")
    upload_to_gcs(df_pages, "pages", "pages.ndjson")

    print("🎉 FULL LOAD DIM FINISHED")

