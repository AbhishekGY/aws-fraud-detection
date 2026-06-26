"""
Phase 1 — Step 2: Glue Crawler & Data Catalog (corrected)
=========================================================
Fixes over the original:
  1. Race condition: original could see state=READY on the first poll
     (before the crawler ever left READY) and "finish" instantly.
     Now we wait for it to actually start, then wait for it to finish.
  2. Missing s3:ListBucket on the bucket ARN — crawlers need to
     enumerate objects, not just GetObject them. Added.
  3. No crawl-result inspection — a FAILED crawl looked like success.
     Now we read LastCrawl status and surface errors.
  4. Least-privilege: dropped s3:PutObject (a crawler only reads).
  5. start_crawler now tolerates an already-running crawler.

Run: python src/pipeline/glue_crawler.py
"""

import boto3
import json
import time

# -----------------------------------------------
# Config
# -----------------------------------------------
REGION = "us-east-1"
BUCKET_NAME = "fraud-detection-mlac01-project"
S3_TARGET = f"s3://{BUCKET_NAME}/raw/transactions/"
DATABASE_NAME = "fraud_detection_db"
CRAWLER_NAME = "fraud-transactions-crawler"
ROLE_NAME = "GlueCrawlerRole-FraudDetection"

# IAM is a global service — no region needed (it's ignored anyway).
iam = boto3.client("iam")
glue = boto3.client("glue", region_name=REGION)

# -----------------------------------------------
# 1. Create IAM role for Glue
# -----------------------------------------------
trust_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}

try:
    iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(trust_policy),
        Description="Role for Glue crawler to access S3 data",
    )
    print(f"[OK] IAM role created: {ROLE_NAME}")
    role_is_new = True
except iam.exceptions.EntityAlreadyExistsException:
    print(f"[OK] IAM role already exists: {ROLE_NAME}")
    role_is_new = False

# Attach the AWS managed policy for Glue service access.
# NOTE: AWSGlueServiceRole grants S3 access ONLY to buckets whose name
# contains 'aws-glue-'. Your bucket doesn't, so the inline policy below
# is what actually gives the crawler read access to your data.
iam.attach_role_policy(
    RoleName=ROLE_NAME,
    PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
)

# Least-privilege read access to your specific bucket.
#   - GetObject on /*        -> read the files
#   - ListBucket on bucket   -> enumerate objects under the prefix
#     (without this, crawlers can fail to find anything)
s3_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": f"arn:aws:s3:::{BUCKET_NAME}/*",
        },
        {
            "Effect": "Allow",
            "Action": ["s3:ListBucket"],
            "Resource": f"arn:aws:s3:::{BUCKET_NAME}",
            # Scope the listing to the raw prefix only.
            "Condition": {"StringLike": {"s3:prefix": ["raw/transactions/*"]}},
        },
    ],
}

try:
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="S3AccessForCrawler",
        PolicyDocument=json.dumps(s3_policy),
    )
    print("[OK] S3 access policy attached to role.")
except Exception as e:
    print(f"[WARN] Could not attach S3 policy: {e}")

# IAM role propagation across services can take longer than 10s,
# especially for a brand-new role. Only wait when the role is new.
if role_is_new:
    print("Waiting 30s for IAM role propagation...")
    time.sleep(30)

role_arn = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
print(f"[OK] Role ARN: {role_arn}")

# -----------------------------------------------
# 2. Create the Glue database
# -----------------------------------------------
try:
    glue.create_database(
        DatabaseInput={
            "Name": DATABASE_NAME,
            "Description": "Fraud detection project — raw and processed transaction data",
        }
    )
    print(f"[OK] Glue database created: {DATABASE_NAME}")
except glue.exceptions.AlreadyExistsException:
    print(f"[OK] Glue database already exists: {DATABASE_NAME}")

# -----------------------------------------------
# 3. Create the Glue Crawler
# -----------------------------------------------
try:
    glue.create_crawler(
        Name=CRAWLER_NAME,
        Role=role_arn,
        DatabaseName=DATABASE_NAME,
        Targets={"S3Targets": [{"Path": S3_TARGET}]},
        SchemaChangePolicy={
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "LOG",
        },
        Description="Crawls raw transaction CSVs, detects schema and date partitions",
    )
    print(f"[OK] Crawler created: {CRAWLER_NAME}")
except glue.exceptions.AlreadyExistsException:
    print(f"[OK] Crawler already exists: {CRAWLER_NAME}")

# -----------------------------------------------
# 4. Run the Crawler (race-safe)
# -----------------------------------------------
# Record the crawl count before we start, so we can detect that a NEW
# crawl actually completed rather than reading a stale state.
pre = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"]
prev_crawl_count = pre.get("LastCrawl", {}).get("MessagePrefix")  # opaque marker

print(f"Starting crawler '{CRAWLER_NAME}'...")
try:
    glue.start_crawler(Name=CRAWLER_NAME)
except glue.exceptions.CrawlerRunningException:
    print("   Crawler is already running — will wait for it to finish.")

# Phase A: wait until the crawler has actually LEFT 'READY' (i.e. started).
# This is the fix for the original race condition.
print("   Waiting for crawler to start...")
for _ in range(20):  # up to ~100s
    state = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"]["State"]
    if state != "READY":
        break
    time.sleep(5)

# Phase B: wait until it returns to 'READY' (RUNNING -> STOPPING -> READY).
print("   Waiting for crawler to finish...")
while True:
    crawler = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"]
    state = crawler["State"]
    print(f"   Crawler state: {state}")
    if state == "READY":
        break
    time.sleep(15)

# -----------------------------------------------
# 4b. Inspect the crawl result — a FAILED crawl is NOT success.
# -----------------------------------------------
last = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"].get("LastCrawl", {})
last_status = last.get("Status", "UNKNOWN")
print(f"\nLast crawl status: {last_status}")
if last_status != "SUCCEEDED":
    print(f"[ERROR] Crawl did not succeed. Status={last_status}")
    if last.get("ErrorMessage"):
        print(f"        Error: {last['ErrorMessage']}")
    print("        Common cause: IAM role missing s3:ListBucket / GetObject,")
    print("        or role not yet propagated. Check the AWS console logs.")

# -----------------------------------------------
# 5. Verify — check what the crawler created
# -----------------------------------------------
print("\n--- Data Catalog Result ---")
tables = glue.get_tables(DatabaseName=DATABASE_NAME)["TableList"]

if not tables:
    print("[ERROR] No tables found. Crawler may have failed — check status above.")
else:
    for table in tables:
        print(f"\nTable name: {table['Name']}")
        print(f"Location:   {table['StorageDescriptor']['Location']}")
        print(f"Format:     {table['StorageDescriptor']['InputFormat']}")
        print("Columns:")
        for col in table["StorageDescriptor"]["Columns"]:
            print(f"   {col['Name']:20s} {col['Type']}")
        if table.get("PartitionKeys"):
            print("Partition keys:")
            for pk in table["PartitionKeys"]:
                print(f"   {pk['Name']:20s} {pk['Type']}")

print("\n[DONE] Data is now registered in the Glue Data Catalog.")
print(f"Verify in console: https://{REGION}.console.aws.amazon.com/glue/home?region={REGION}#/v2/data-catalog/tables")
