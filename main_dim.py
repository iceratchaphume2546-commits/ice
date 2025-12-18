import os
import pandas as pd
from datetime import datetime
import tempfile

# เปิด GCS เฉพาะตอนจำเป็น
try:
    from google.cloud import storage
except ImportError:
    storage = None

# =========================
# CONFIG
# =========================
MODE = os.getenv("MODE", "LOCAL")   # LOCAL | GCS
GCS_BUCKET = "hongthai"
SOURCE_FILE = "dataverse_export.ndjson"

# =========================
# DIM CONFIG
# =========================
DIM_CONFIG = {
    "products": {
        "cols": {
            "itsm_ads_product_lineid": "product_id",
            "itsm_product_name": "product_name",
        }
    },
    "channels": {
        "cols": {
            "_itsm_channel_value": "channel_id",
            "itsm_channel_name": "channel_name",
        }
    },
    "pages": {
        "cols": {
            "_itsm_pagename_value": "page_id",
            "itsm_page_name": "page_name",
        }
    },
    "kols": {
        "cols": {
            "_itsm_agentpost_value": "kol_id",
        }
    },
}

# =========================
# LOAD SOURCE (LOCAL ONLY)
# =========================
def load_source():
    print(f"📥 Load {SOURCE_FILE} (local)")
    df = pd.read_json(SOURCE_FILE, lines=True)
    print("🔎 Source columns:", list(df.columns))
    return df

# =========================
# BUILD DIM (GENERIC)
# =========================
def build_dim(df, config):
    if not all(c in df.columns for c in config["cols"]):
        return pd.DataFrame()

    out = df.rename(columns=config["cols"])
    out = out[list(config["cols"].values())]
    return out.drop_duplicates()

# =========================
# WRITE OUTPUT (FULL LOAD)
# =========================
def write_output(df, dim_name):
    if df.empty:
        print(f"⚠️ Skip {dim_name} (empty)")
        return

    today = datetime.now()
    path = f"{dim_name}/{today:%Y/%m/%d}/{dim_name}.ndjson"

    if MODE == "LOCAL":
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_json(path, orient="records", lines=True, force_ascii=False)
        print(f"💾 Saved local → {path}")

    elif MODE == "GCS":
        if storage is None:
            raise RuntimeError("google-cloud-storage not installed")

        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(path)

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, encoding="utf-8"
        ) as tmp:
            df.to_json(tmp.name, orient="records", lines=True, force_ascii=False)
            blob.upload_from_filename(tmp.name)

        print(f"☁️ Uploaded → gs://{GCS_BUCKET}/{path}")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    print("🚀 Start FULL LOAD DIM")

    df_source = load_source()

    for dim_name, cfg in DIM_CONFIG.items():
        df_dim = build_dim(df_source, cfg)
        write_output(df_dim, dim_name)

    print("🎉 DONE")
