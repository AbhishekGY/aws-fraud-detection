-- =====================================================================
-- Phase 1 — Step 5: Athena Validation Queries
-- =====================================================================
-- Purpose: confirm the ETL output is SANE, not just that the job ran.
-- Target table: fraud_detection_db.processed_transactions
--               (the Parquet output cataloged by the Step-4 crawler)

--
-- NOTE on column names: the crawler lower-cases everything, and the ETL
-- already lower-cased too, so columns are: time, amount, v1..v28,
-- avg_transaction_amount_last_7_days, transaction_frequency_last_hour,
-- distance_from_home, class, and the partition key 'date'.
-- =====================================================================


-- ---------------------------------------------------------------------
-- Q1. Does the table read at all, and how many rows?
-- ---------------------------------------------------------------------
-- Expect: ~10,000 rows (your subsample size). If this errors, the
-- crawler cataloged the wrong location or the table name differs.
SELECT COUNT(*) AS total_rows
FROM processed_transactions;


-- ---------------------------------------------------------------------
-- Q2. Eyeball the data — first 10 rows with the engineered features
-- ---------------------------------------------------------------------
-- Sanity-check that the 3 features are populated (not all null/zero).
SELECT
    "date",
    amount,
    avg_transaction_amount_last_7_days,
    transaction_frequency_last_hour,
    distance_from_home,
    class
FROM processed_transactions
LIMIT 10;


-- ---------------------------------------------------------------------
-- Q3. CLASS IMBALANCE — the single most important check
-- ---------------------------------------------------------------------
-- The raw Kaggle set is ~0.17% fraud. Your subsample kept ALL fraud rows
-- and sampled non-fraud down to hit ~10K, so your ratio will be HIGHER
-- than 0.17% (the fraud count is fixed at 492, denominator shrank).
-- What matters: fraud is still a small minority. This justifies
-- scale_pos_weight / recall-focused tuning in Phase 2.
SELECT
    class,
    COUNT(*)                                             AS row_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 3)   AS pct_of_total
FROM processed_transactions
GROUP BY class
ORDER BY class;


-- ---------------------------------------------------------------------
-- Q4. PARTITION INTEGRITY — did all 7 days land?
-- ---------------------------------------------------------------------
-- s3_upload.py spread rows across 7 fake dates (2025-01-01 .. 01-07).
-- Expect 7 partitions, each with a chunk of rows. A missing date means
-- the ETL or crawler dropped a partition.
SELECT
    "date",
    COUNT(*) AS rows_in_partition
FROM processed_transactions
GROUP BY "date"
ORDER BY "date";


-- ---------------------------------------------------------------------
-- Q5. AMOUNT distribution — basic numeric sanity
-- ---------------------------------------------------------------------
-- Amounts should be non-negative, with a wide spread (min near 0,
-- max in the thousands). NULLs here would mean a cast/parse problem.
SELECT
    MIN(amount)                          AS min_amount,
    ROUND(AVG(amount), 2)                AS avg_amount,
    APPROX_PERCENTILE(amount, 0.50)      AS median_amount,
    MAX(amount)                          AS max_amount,
    COUNT(*) - COUNT(amount)             AS null_amounts   -- should be 0
FROM processed_transactions;


-- ---------------------------------------------------------------------
-- Q6. Do fraud transactions differ in amount? (real signal check)
-- ---------------------------------------------------------------------
-- This uses REAL columns (amount, class), not the fabricated features.
-- In the Kaggle set, fraud amounts often skew differently from legit.
-- This is the kind of feature-vs-target relationship Phase 2 exploits.
SELECT
    class,
    COUNT(*)                 AS n,
    ROUND(AVG(amount), 2)    AS avg_amount,
    ROUND(MIN(amount), 2)    AS min_amount,
    ROUND(MAX(amount), 2)    AS max_amount
FROM processed_transactions
GROUP BY class
ORDER BY class;


-- ---------------------------------------------------------------------
-- Q7. FABRICATED FEATURES — confirm they're in their expected ranges
-- ---------------------------------------------------------------------
-- Reminder: these 3 are synthetic stand-ins, NOT real signals.
--   distance_from_home: built as abs(sin(amount))*100 -> bounded 0..100
--   transaction_frequency_last_hour: a count -> >= 1
--   avg_transaction_amount_last_7_days: a per-date mean -> > 0
-- This query just proves the ETL math produced sane bounds, nothing more.
SELECT
    ROUND(MIN(distance_from_home), 2)                 AS min_distance,
    ROUND(MAX(distance_from_home), 2)                 AS max_distance,   -- expect <= 100
    MIN(transaction_frequency_last_hour)              AS min_freq,        -- expect >= 1
    MAX(transaction_frequency_last_hour)              AS max_freq,
    ROUND(MIN(avg_transaction_amount_last_7_days), 2) AS min_avg7d,
    ROUND(MAX(avg_transaction_amount_last_7_days), 2) AS max_avg7d
FROM processed_transactions;


-- ---------------------------------------------------------------------
-- Q8. PARTITION PRUNING demo — what filtering on 'date' actually does
-- ---------------------------------------------------------------------
-- Run this, then check the "Data scanned" figure under Query stats.
-- Because the table is partitioned by date and stored as Parquet,
-- Athena reads ONLY the 2025-01-03 folder, not all 7. Compare the
-- bytes-scanned here vs Q1 (which scans everything). This is the
-- partitioning payoff the exam tests.
SELECT COUNT(*) AS rows_on_jan_3
FROM processed_transactions
WHERE "date" = '2025-01-03';
