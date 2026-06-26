"""
Phase 1 — Step 1: S3 Setup & Raw Data Upload
=============================================
What this does:
- Creates an S3 bucket in us-east-1
- Subsamples the full Kaggle dataset to ~10K rows (keeps costs low)
- Uploads the subsampled data to S3 with date-based partitioning

Why date partitioning?
In production, transactions arrive daily. Partitioning by date lets Glue/Athena
scan only the relevant partitions instead of the full dataset. The exam tests
this concept — you'll see questions about partition strategies.

Run: python src/pipeline/s3_upload.py
"""

import boto3
import pandas as pd
import os
from datetime import datetime, timedelta
import random

# -----------------------------------------------
# Config
# -----------------------------------------------
BUCKET_NAME = "fraud-detection-mlac01-project"
REGION = "us-east-1"
RAW_DATA_PATH = os.path.expanduser("~/fraud-detection-ml/data/raw/creditcard.csv")
S3_PREFIX = "raw/transactions"

# -----------------------------------------------
# 1. Create the S3 bucket
# -----------------------------------------------
s3 = boto3.client("s3", region_name=REGION)

try:
    # us-east-1 doesn't use LocationConstraint — this is an AWS quirk the exam loves
    if REGION == "us-east-1":
        s3.create_bucket(Bucket=BUCKET_NAME)
    else:
        s3.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
    print(f"[OK] Bucket created: {BUCKET_NAME}")
except s3.exceptions.BucketAlreadyOwnedByYou:
    print(f"[OK] Bucket already exists and is yours: {BUCKET_NAME}")
except s3.exceptions.BucketAlreadyExists:
    print(f"[ERROR] Bucket name '{BUCKET_NAME}' is taken globally. Change BUCKET_NAME and rerun.")
    exit(1)

# -----------------------------------------------
# 2. Load and subsample the dataset
# -----------------------------------------------
print("Loading dataset...")
df = pd.read_csv(RAW_DATA_PATH)
print(f"   Full dataset: {len(df)} rows, {len(df.columns)} columns")

# Subsample to ~10K rows, preserving the fraud/non-fraud ratio
# The original dataset is ~0.17% fraud — we keep that ratio
fraud = df[df["Class"] == 1]
non_fraud = df[df["Class"] == 0].sample(n=10000 - len(fraud), random_state=42)
df_sample = pd.concat([fraud, non_fraud]).sample(frac=1, random_state=42).reset_index(drop=True)
print(f"   Subsampled: {len(df_sample)} rows ({len(fraud)} fraud, {len(non_fraud)} non-fraud)")

# -----------------------------------------------
# 3. Assign fake dates and partition
# -----------------------------------------------
# The original dataset has no date column — we simulate 7 days of transactions
# so we can demonstrate date-based partitioning
random.seed(42)
base_date = datetime(2025, 1, 1)
df_sample["transaction_date"] = [
    (base_date + timedelta(days=random.randint(0, 6))).strftime("%Y-%m-%d")
    for _ in range(len(df_sample))
]

# -----------------------------------------------
# 4. Upload partitioned CSVs to S3
# -----------------------------------------------
print("Uploading to S3 with date partitions...")

for date_str, group in df_sample.groupby("transaction_date"):
    # S3 key: raw/transactions/date=2025-01-01/data.csv
    # This Hive-style partitioning (key=value) is what Glue crawlers auto-detect
    s3_key = f"{S3_PREFIX}/date={date_str}/data.csv"

    csv_buffer = group.to_csv(index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer)
    print(f"   [OK] Uploaded {len(group)} rows -> s3://{BUCKET_NAME}/{s3_key}")

print(f"\n[DONE] {len(df_sample)} rows uploaded across {df_sample['transaction_date'].nunique()} partitions.")
print(f"\nVerify in console: https://s3.console.aws.amazon.com/s3/buckets/{BUCKET_NAME}")
