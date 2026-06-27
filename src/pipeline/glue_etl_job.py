"""
Phase 1 — Step 3: Glue ETL Job (PySpark)
========================================
What this does:
  1. Reads the raw transactions table FROM THE GLUE DATA CATALOG
     (the table the crawler created in Step 2 — not a raw S3 path).
  2. Trivial cleaning: drop fully-null rows, dedupe.
  3. Engineers the three project "features".
  4. Writes Parquet back to S3, partitioned by date.
  5. Re-catalogs the output so Athena can query it.

-------------------------------------------------------------------
HONESTY NOTE — read this, do not skip it
-------------------------------------------------------------------
The Kaggle creditcard.csv is already PCA-anonymised (V1..V28, Time,
Amount, Class). It has:
  - NO nulls           -> "clean nulls" is a no-op in practice
  - NO categoricals    -> "encode categoricals" is a no-op
  - NO user id / geo    -> the 3 required features CANNOT be derived

So the three features below are FABRICATED deterministically from
Amount and Time. They are reproducible and internally consistent, but
they are NOT real behavioural features. They exist so the downstream
Feature Store / training phases have something to consume and so you
practise the Glue + Parquet + Athena mechanics the exam tests.
Do not present these as genuine engineered signals.
-------------------------------------------------------------------

This script is written to run AS A GLUE JOB (Spark), not on your laptop.
Upload it to S3 and create a Glue job pointing at it, OR paste it into a
Glue Studio script editor. Local execution will fail (no GlueContext).

Glue job parameters to set (Job details -> Job parameters):
  --DATABASE_NAME      fraud_detection_db
  --SOURCE_TABLE       transactions          (check actual name, see note)
  --OUTPUT_PATH        s3://fraud-detection-mlac01-project/processed/transactions/
  --additional-python-modules   (leave empty)

NOTE on SOURCE_TABLE: the crawler names the table after the prefix it
crawled. For target s3://.../raw/transactions/ the table is usually
named 'transactions'. If your Step-2 output printed a different name,
pass that via --SOURCE_TABLE.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import Window

# -----------------------------------------------
# Resolve job arguments (with sane defaults)
# -----------------------------------------------
# getResolvedOptions requires JOB_NAME; the rest we default if absent.
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "DATABASE_NAME", "SOURCE_TABLE", "OUTPUT_PATH"],
)

DATABASE_NAME = args["DATABASE_NAME"]
SOURCE_TABLE = args["SOURCE_TABLE"]
OUTPUT_PATH = args["OUTPUT_PATH"].rstrip("/") + "/"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"[INFO] DB={DATABASE_NAME} TABLE={SOURCE_TABLE} OUT={OUTPUT_PATH}")

# -----------------------------------------------
# 1. Read from the Glue Data Catalog (NOT a raw S3 path)
# -----------------------------------------------
# This is the point of having run the crawler. Using the catalog means
# schema + partitions are already known.
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=SOURCE_TABLE,
    transformation_ctx="source_dyf",
)
df = dyf.toDF()
print(f"[INFO] Rows read from catalog: {df.count()}")
print("[INFO] Schema as cataloged:")
df.printSchema()

# The crawler lower-cases column names. Kaggle's are 'Time','Amount','Class'
# plus V1..V28. After crawling they're typically 'time','amount','class','v1'...
# Normalise defensively so the rest of the script is case-stable.
df = df.toDF(*[c.lower() for c in df.columns])

# The partition column from the path is 'date' (from date=YYYY-MM-DD).
# But s3_upload.py ALSO wrote a 'transaction_date' column INSIDE the file.
# They're the same value. Keep 'date' (the partition), drop the duplicate.
if "transaction_date" in df.columns and "date" in df.columns:
    df = df.drop("transaction_date")
elif "transaction_date" in df.columns and "date" not in df.columns:
    # Fallback: crawler didn't pick up the path partition for some reason.
    df = df.withColumnRenamed("transaction_date", "date")

# -----------------------------------------------
# 2. Cleaning (mostly a no-op for this dataset — done for completeness)
# -----------------------------------------------
before = df.count()
df = df.dropna(how="all")          # drop rows that are entirely null
df = df.dropDuplicates()           # dedupe exact duplicate rows
after = df.count()
print(f"[INFO] Cleaning removed {before - after} rows ({before} -> {after})")

# Cast numerics defensively — crawler sometimes infers Amount/Time as string.
for col in ["amount", "time", "class"]:
    if col in df.columns:
        df = df.withColumn(col, F.col(col).cast("double"))

# -----------------------------------------------
# 3. Feature engineering  (FABRICATED — see honesty note at top)
# -----------------------------------------------
# There is no user id and no real timestamp, so genuine windowed
# behavioural features are impossible. We synthesise deterministic
# stand-ins from 'amount' and 'time' so they're reproducible.

# avg_transaction_amount_last_7_days:
#   real version = per-user rolling 7d mean. We have no user.
#   stand-in = mean amount within the same date partition (a daily avg).
date_window = Window.partitionBy("date")
df = df.withColumn(
    "avg_transaction_amount_last_7_days",
    F.avg("amount").over(date_window),
)

# transaction_frequency_last_hour:
#   real version = count of a user's txns in the trailing hour.
#   stand-in = count of txns sharing the same integer hour bucket of 'time'.
#   'time' is seconds-from-first-txn in the Kaggle set, so //3600 = hour bucket.
df = df.withColumn("hour_bucket", (F.col("time") / 3600).cast("int"))
hour_window = Window.partitionBy("date", "hour_bucket")
df = df.withColumn(
    "transaction_frequency_last_hour",
    F.count("*").over(hour_window),
)

# distance_from_home:
#   real version = geo distance between txn and the user's home.
#   stand-in = deterministic pseudo-distance derived from amount, so the
#   SAME amount always yields the SAME 'distance'. Purely synthetic.
df = df.withColumn(
    "distance_from_home",
    F.round(F.abs(F.sin(F.col("amount"))) * 100, 2),
)

df = df.drop("hour_bucket")

print("[INFO] Post-feature schema:")
df.printSchema()
df.select(
    "date",
    "amount",
    "avg_transaction_amount_last_7_days",
    "transaction_frequency_last_hour",
    "distance_from_home",
    "class",
).show(5, truncate=False)

# -----------------------------------------------
# 4. Write Parquet to S3, partitioned by date
# -----------------------------------------------
# Parquet = columnar, compressed, far cheaper for Athena to scan than CSV.
# Partition by 'date' so Athena can prune partitions.
(
    df.write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(OUTPUT_PATH)
)
print(f"[OK] Parquet written to {OUTPUT_PATH}")

# -----------------------------------------------
# 5. (Optional but recommended) register output in the catalog
# -----------------------------------------------
# Easiest path: run a SECOND crawler over OUTPUT_PATH, or add a catalog
# table via Glue. Writing straight to the catalog from the job is also
# possible with getSink(...). Kept out here to stay simple — point a
# crawler at the processed/ prefix, or define an Athena external table
# (DDL provided separately).

job.commit()
print("[DONE] ETL complete.")
