"""
Phase 1 — Step 4: Catalog the Processed Parquet Output (second crawler)
=======================================================================
The ETL job (Step 3) wrote Parquet to s3://.../processed/transactions/
partitioned by date. Athena queries CATALOG TABLES, not raw S3 files,
so that output is invisible until something catalogs it. This crawler
does that — same pattern as Step 2, retargeted.

Differences vs the Step-2 raw crawler:
  1. Target is the PROCESSED prefix, not raw.
  2. A TablePrefix ('processed_') is set so the new table is named
     'processed_transactions' instead of 'transactions' — otherwise it
     would COLLIDE with the raw table already in the same database.
  3. Reuses the EXISTING GlueCrawlerRole-FraudDetection role. That role
     already has GetObject on /* and (after the ETL-job edit) ListBucket
     scoped to include 'processed/*'. No new IAM needed — but if the
     crawler finds zero tables, the #1 suspect is that ListBucket prefix.

Run: python src/pipeline/glue_crawler_processed.py
"""

import boto3
import time

# -----------------------------------------------
# Config
# -----------------------------------------------
REGION = "us-east-1"
BUCKET_NAME = "fraud-detection-mlac01-project"
S3_TARGET = f"s3://{BUCKET_NAME}/processed/transactions/"
DATABASE_NAME = "fraud_detection_db"          # same DB as raw
CRAWLER_NAME = "fraud-processed-crawler"      # distinct crawler name
ROLE_NAME = "GlueCrawlerRole-FraudDetection"  # REUSE existing role
TABLE_PREFIX = "processed_"                    # -> table 'processed_transactions'

iam = boto3.client("iam")
glue = boto3.client("glue", region_name=REGION)

# -----------------------------------------------
# 1. Resolve the existing role ARN (do NOT recreate it)
# -----------------------------------------------
try:
    role_arn = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
    print(f"[OK] Reusing existing role: {role_arn}")
except iam.exceptions.NoSuchEntityException:
    print(f"[ERROR] Role {ROLE_NAME} not found. Run the Step-2 crawler script")
    print("        first (it creates the role), or check the name.")
    raise

# Sanity reminder, not an automated check: the role's inline S3 policy
# must allow ListBucket on the 'processed/*' prefix. If this crawler
# finishes with zero tables, that prefix condition is the usual cause.
print("[INFO] Ensure role's ListBucket policy includes prefix 'processed/*'.")

# -----------------------------------------------
# 2. Create the crawler (idempotent)
# -----------------------------------------------
try:
    glue.create_crawler(
        Name=CRAWLER_NAME,
        Role=role_arn,
        DatabaseName=DATABASE_NAME,
        Targets={"S3Targets": [{"Path": S3_TARGET}]},
        TablePrefix=TABLE_PREFIX,
        SchemaChangePolicy={
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "LOG",
        },
        Description="Crawls processed Parquet output, detects schema + date partitions",
    )
    print(f"[OK] Crawler created: {CRAWLER_NAME}")
except glue.exceptions.AlreadyExistsException:
    print(f"[OK] Crawler already exists: {CRAWLER_NAME}")

# -----------------------------------------------
# 3. Run the crawler (race-safe: wait to START, then to FINISH)
# -----------------------------------------------
print(f"Starting crawler '{CRAWLER_NAME}'...")
try:
    glue.start_crawler(Name=CRAWLER_NAME)
except glue.exceptions.CrawlerRunningException:
    print("   Already running — will wait for it to finish.")

# Phase A: wait until it LEAVES 'READY' (actually started)
print("   Waiting for crawler to start...")
for _ in range(20):  # up to ~100s
    state = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"]["State"]
    if state != "READY":
        break
    time.sleep(5)

# Phase B: wait until it returns to 'READY' (RUNNING -> STOPPING -> READY)
print("   Waiting for crawler to finish...")
while True:
    state = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"]["State"]
    print(f"   Crawler state: {state}")
    if state == "READY":
        break
    time.sleep(15)

# -----------------------------------------------
# 4. Inspect the crawl result — FAILED != success
# -----------------------------------------------
last = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"].get("LastCrawl", {})
last_status = last.get("Status", "UNKNOWN")
print(f"\nLast crawl status: {last_status}")
if last_status != "SUCCEEDED":
    print(f"[ERROR] Crawl did not succeed. Status={last_status}")
    if last.get("ErrorMessage"):
        print(f"        Error: {last['ErrorMessage']}")
    print("        Likely cause: role missing ListBucket on 'processed/*',")
    print("        or the processed/ prefix is empty (ETL didn't write).")

# -----------------------------------------------
# 5. Verify the new table
# -----------------------------------------------
print("\n--- Data Catalog Result ---")
expected_table = f"{TABLE_PREFIX}transactions"
try:
    table = glue.get_table(DatabaseName=DATABASE_NAME, Name=expected_table)["Table"]
    sd = table["StorageDescriptor"]
    print(f"\nTable name: {table['Name']}")
    print(f"Location:   {sd['Location']}")
    print(f"Format:     {sd['InputFormat']}")
    print("Columns:")
    for col in sd["Columns"]:
        print(f"   {col['Name']:38s} {col['Type']}")
    if table.get("PartitionKeys"):
        print("Partition keys:")
        for pk in table["PartitionKeys"]:
            print(f"   {pk['Name']:38s} {pk['Type']}")
    else:
        print("[WARN] No partition keys detected — expected 'date'.")
except glue.exceptions.EntityNotFoundException:
    print(f"[ERROR] Table '{expected_table}' not found.")
    print("        Crawler ran but cataloged nothing. Check status above and")
    print("        confirm Parquet exists under processed/transactions/.")
    # Dump whatever tables DO exist, to aid debugging.
    tables = glue.get_tables(DatabaseName=DATABASE_NAME)["TableList"]
    print(f"        Tables currently in {DATABASE_NAME}: {[t['Name'] for t in tables]}")

print("\n[DONE] Processed data cataloged. Ready for Athena validation.")
print(f"Verify: https://{REGION}.console.aws.amazon.com/glue/home?region={REGION}#/v2/data-catalog/tables")
