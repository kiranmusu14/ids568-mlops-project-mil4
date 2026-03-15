import argparse
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline(input_path, output_path, is_local=True):
    """Executes the distributed feature engineering pipeline."""
    start_time = time.time()
    
    # Initialize Spark Session
    builder = SparkSession.builder.appName("IDS568_Milestone4_FeatureEngineering")
    
    if is_local:
        # True local baseline: restricts processing to exactly 1 core 
        builder = builder.master("local[1]") 
        logging.info("Running in LOCAL mode (1 core)")
    else:
        # Distributed simulation: uses all available cores on the machine 
        builder = builder.master("local[*]") 
        logging.info("Running in DISTRIBUTED mode (all available cores)")
        
    spark = builder.getOrCreate()
    
    # Set shuffle partitions (tune this for your REPORT.md trade-off analysis)
    spark.conf.set("spark.sql.shuffle.partitions", "200") 
    
    logging.info(f"Reading data from {input_path}")
    df = spark.read.parquet(input_path)
    
    # --- Distributed Feature Engineering Logic ---
    
    # 1. Simple Transformation (Narrow Dependency)
    df = df.withColumn("log_transaction", F.log1p(F.col("transaction_amount")))
    
    # 2. Stateful/Windowed Transformation (Wide Dependency - Forces a Shuffle)
    # This ensures you have 'Shuffle Volume' data to report [cite: 221]
    window_spec = Window.partitionBy("user_id").orderBy("event_timestamp")
    
    df = df.withColumn("rolling_avg_amount", F.avg("transaction_amount").over(window_spec))
    df = df.withColumn("amount_diff_from_avg", F.col("transaction_amount") - F.col("rolling_avg_amount"))
    
    # 3. Aggregation
    summary_df = df.groupBy("category_code").agg(
        F.count("user_id").alias("total_transactions"),
        F.sum("transaction_amount").alias("total_volume")
    )

    logging.info(f"Writing transformed data to {output_path}")
    
    # Write output deterministically by ordering before saving [cite: 213]
    summary_df.orderBy("category_code").write.mode("overwrite").parquet(f"{output_path}/summary")
    df.orderBy("user_id", "event_timestamp").write.mode("overwrite").parquet(f"{output_path}/features")
    
    spark.stop()
    
    end_time = time.time()
    logging.info(f"Pipeline completed in {end_time - start_time:.2f} seconds")

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