import argparse
import hashlib
import json
import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def compute_output_hash(output_path):
    """Computes a SHA-256 hash of all parquet files in the output directory for verification."""
    sha256 = hashlib.sha256()
    manifest = {}

    for dirpath, _, filenames in sorted(os.walk(output_path)):
        for filename in sorted(filenames):
            if filename.endswith(".parquet"):
                filepath = os.path.join(dirpath, filename)
                with open(filepath, "rb") as f:
                    file_bytes = f.read()
                    file_hash = hashlib.sha256(file_bytes).hexdigest()
                    manifest[os.path.relpath(filepath, output_path)] = file_hash
                    sha256.update(file_bytes)

    combined_hash = sha256.hexdigest()
    manifest["_combined_hash"] = combined_hash
    return combined_hash, manifest


def run_pipeline(input_path, output_path, is_local=True):
    """Executes the distributed feature engineering pipeline."""
    start_time = time.time()

    # Initialize Spark Session
    builder = SparkSession.builder.appName("IDS568_Milestone4_FeatureEngineering")

    if is_local:
        builder = builder.master("local[1]")
        logging.info("Running in LOCAL mode (1 core)")
    else:
        builder = builder.master("local[*]")
        logging.info("Running in DISTRIBUTED mode (all available cores)")

    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    logging.info(f"Reading data from {input_path}")
    df = spark.read.parquet(input_path)

    # --- Distributed Feature Engineering Logic ---

    # 1. Log transformation of transaction amount (narrow dependency)
    df = df.withColumn("log_transaction", F.log1p(F.col("transaction_amount")))

    # 2. Temporal features extracted from event_timestamp
    df = df.withColumn("transaction_hour", F.hour(F.col("event_timestamp")))
    df = df.withColumn("transaction_day_of_week", F.dayofweek(F.col("event_timestamp")))

    # 3. Per-user window features (wide dependency — forces a shuffle)
    window_spec = Window.partitionBy("user_id").orderBy("event_timestamp")

    df = df.withColumn("rolling_avg_amount", F.avg("transaction_amount").over(window_spec))
    df = df.withColumn("amount_diff_from_avg", F.col("transaction_amount") - F.col("rolling_avg_amount"))
    df = df.withColumn("cumulative_spend", F.sum("transaction_amount").over(window_spec))
    df = df.withColumn("rolling_txn_count", F.count("transaction_amount").over(window_spec))

    # 4. Global z-score normalization of transaction_amount
    stats = df.select(
        F.mean("transaction_amount").alias("mean"),
        F.stddev("transaction_amount").alias("stddev")
    ).collect()[0]
    global_mean = stats["mean"]
    global_stddev = stats["stddev"]
    df = df.withColumn(
        "transaction_amount_zscore",
        (F.col("transaction_amount") - global_mean) / global_stddev
    )

    # 5. High-value flag: top 10% of transaction amounts
    high_value_threshold = df.approxQuantile("transaction_amount", [0.90], 0.01)[0]
    df = df.withColumn(
        "is_high_value",
        (F.col("transaction_amount") >= high_value_threshold).cast("integer")
    )

    # 6. Aggregation summary by category
    summary_df = df.groupBy("category_code").agg(
        F.count("user_id").alias("total_transactions"),
        F.sum("transaction_amount").alias("total_volume"),
        F.avg("transaction_amount").alias("avg_transaction_amount"),
        F.sum("is_high_value").alias("high_value_count"),
        F.avg("transaction_amount_zscore").alias("avg_zscore")
    )

    logging.info(f"Writing transformed data to {output_path}")

    summary_df.orderBy("category_code").write.mode("overwrite").parquet(f"{output_path}/summary")
    df.orderBy("user_id", "event_timestamp").write.mode("overwrite").parquet(f"{output_path}/features")

    spark.stop()

    end_time = time.time()
    elapsed = end_time - start_time
    logging.info(f"Pipeline completed in {elapsed:.2f} seconds")

    # --- Output Hash Verification ---
    logging.info("Computing output hash for verification...")
    combined_hash, manifest = compute_output_hash(output_path)
    manifest_path = os.path.join(output_path, "output_manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    logging.info(f"Output SHA-256 combined hash: {combined_hash}")
    logging.info(f"Hash manifest saved to: {manifest_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Feature Engineering Pipeline")
    parser.add_argument("--input", type=str, required=True, help="Input data directory")
    parser.add_argument("--output", type=str, required=True, help="Output data directory")
    parser.add_argument("--mode", type=str, choices=["local", "distributed"], default="local",
                        help="Execution mode for performance comparison")

    args = parser.parse_args()
    is_local_mode = args.mode == "local"

    try:
        run_pipeline(args.input, args.output, is_local_mode)
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
