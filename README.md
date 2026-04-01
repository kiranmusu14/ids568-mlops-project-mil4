# IDS 568 MLOps - Milestone 4: Distributed & Streaming Pipeline
[![Python application](https://github.com/kiranmusu14/ids568-mlops-project-mil4/actions/workflows/python-app.yml/badge.svg)](https://github.com/kiranmusu14/ids568-mlops-project-mil4/actions/workflows/python-app.yml)

## Project Overview

This repository contains a distributed feature engineering pipeline built with PySpark, alongside a streaming ingestion pipeline. The goal is to evaluate scaling behavior, throughput optimization, and architectural trade-offs between single-machine (local) execution and multi-worker (distributed) execution on datasets exceeding 10 million rows.

---

## Repository Structure

```
ids568-mlops-project-mil4/
├── pipeline.py            # Distributed feature engineering pipeline (PySpark)
├── generate_data.py       # Synthetic data generation script (10M+ rows, seeded)
├── producer.py            # Streaming event producer (TCP socket, burst simulation)
├── consumer.py            # Streaming consumer (5-second tumbling windows, latency metrics)
├── REPORT.md              # Performance analysis: local vs. distributed metrics + cost
├── STREAMING_REPORT.md    # Streaming load testing: p50/p95/p99 latency + failure analysis
├── requirements.txt       # Python dependencies with pinned versions
└── .gitignore             # Excludes data/, mlops_env/, __pycache__, etc.
```

---

## Prerequisites

- **Python 3.9+**
- **Java JDK 17** (required by PySpark/Spark)
  - macOS: `brew install openjdk@17`
  - Ubuntu: `sudo apt install openjdk-17-jdk`
  - Verify: `java -version`
- **Git**

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/kiranmusu14/ids568-mlops-project-mil4.git
cd ids568-mlops-project-mil4
```

### 2. Create and activate a virtual environment

```bash
python3 -m venv mlops_env
source mlops_env/bin/activate      # macOS/Linux
# mlops_env\Scripts\activate       # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

**Pinned versions** (`requirements.txt`):
```
pyspark==3.5.0
pandas==2.1.0
numpy==1.26.0
pyarrow==14.0.0
```

---

## Running the Distributed Pipeline

### Step 1 — Generate synthetic data (10M rows, seed=42)

```bash
python generate_data.py --rows 10000000 --output data/raw/ --seed 42
```

> **Reproducibility note:** The `--seed 42` flag ensures `numpy.random` produces identical data across all runs. Any user running this command will generate the exact same dataset, enabling independent verification of all reported metrics.

### Step 2 — Run in LOCAL mode (1-core baseline)

```bash
python pipeline.py --input data/raw/ --output data/processed_local/ --mode local
```

Expected output:
```
Running in LOCAL mode (1 core)
Pipeline completed in ~28.27 seconds
Output SHA-256 combined hash: <hash>
Hash manifest saved to: data/processed_local/output_manifest.json
```

### Step 3 — Run in DISTRIBUTED mode (all available cores)

```bash
python pipeline.py --input data/raw/ --output data/processed_dist/ --mode distributed
```

Expected output:
```
Running in DISTRIBUTED mode (all available cores)
Pipeline completed in ~18.59 seconds
Output SHA-256 combined hash: <hash>
Hash manifest saved to: data/processed_dist/output_manifest.json
```

### Step 4 — Verify output integrity

The pipeline writes `output_manifest.json` to each output directory. To verify a run is reproducible:

```bash
# Re-run with same seed and compare manifests
python pipeline.py --input data/raw/ --output data/verify/ --mode local
diff data/processed_local/output_manifest.json data/verify/output_manifest.json
# No output = identical results
```

---

## Features Engineered

The pipeline derives **8 features** from the 4-column raw schema (`user_id`, `transaction_amount`, `event_timestamp`, `category_code`):

| Feature | Source Column | Type |
|---|---|---|
| `log_transaction` | `transaction_amount` | Narrow transform |
| `transaction_hour` | `event_timestamp` | Temporal |
| `transaction_day_of_week` | `event_timestamp` | Temporal |
| `rolling_avg_amount` | `user_id` + `transaction_amount` | Wide (shuffle) |
| `amount_diff_from_avg` | derived | Wide (shuffle) |
| `cumulative_spend` | `user_id` + `transaction_amount` | Wide (shuffle) |
| `rolling_txn_count` | `user_id` | Wide (shuffle) |
| `transaction_amount_zscore` | `transaction_amount` | Global normalization |
| `is_high_value` | `transaction_amount` | Binary flag (top 10%) |

---

## Running the Streaming Pipeline (Bonus)

The streaming pipeline uses raw TCP sockets. Run the producer and consumer in **two separate terminals**.

### Terminal 1 — Start the producer

```bash
# Default: 100 msg/s
python producer.py --rate 100

# Load test variants:
python producer.py --rate 1000    # 1K msg/s
python producer.py --rate 10000   # 10K msg/s
```

The producer simulates a **10% burst probability** that temporarily multiplies the send rate by 5×.

### Terminal 2 — Start the consumer

```bash
python consumer.py
```

The consumer outputs per-window metrics every 5 seconds:
```
--- Window Closed (5.0s) ---
Throughput: 100.00 msg/s
Latencies - p50: 0.01ms | p95: 0.02ms | p99: 0.02ms
State: {'total_purchase_volume': 24318.45}
```

### Load test results summary

| Load Level | Observed Throughput | p50 | p95 | p99 |
|---|---|---|---|---|
| 100 msg/s | ~100 msg/s | ~0.01 ms | ~0.02 ms | ~0.02 ms |
| 1K msg/s | ~1,000 msg/s | ~0.01 ms | ~0.02 ms | ~0.02 ms |
| 10K msg/s | ~7,780 msg/s | 0.02 ms | 0.02 ms | 0.03 ms |
| 1M msg/s | Socket crash | — | — | — |

---

## Sanity Checks

Run these before submitting to verify the environment:

```bash
# Check all required files exist
for file in pipeline.py generate_data.py README.md REPORT.md requirements.txt; do
  [ -f "$file" ] && echo "✓ $file" || echo "✗ $file MISSING"
done

# Validate Python syntax
for file in *.py; do
  python -m py_compile "$file" && echo "✓ $file syntax OK" || echo "✗ $file has errors"
done

# Test data generation (small sample)
python generate_data.py --rows 1000 --output test_data/ --seed 42
[ $? -eq 0 ] && echo "✓ Data generation OK" && rm -rf test_data/ || echo "✗ Data generation failed"

# Test pipeline (small sample)
python generate_data.py --rows 1000 --output test_data/ --seed 42
python pipeline.py --input test_data/ --output test_output/ --mode local
[ $? -eq 0 ] && echo "✓ Pipeline OK" && rm -rf test_data/ test_output/ || echo "✗ Pipeline failed"

# Verify reproducibility
python generate_data.py --rows 100 --seed 42 --output run1/
python generate_data.py --rows 100 --seed 42 --output run2/
diff -r run1/ run2/ > /dev/null && echo "✓ Reproducible" || echo "✗ NOT reproducible"
rm -rf run1/ run2/
```

---

## Performance Summary

Full analysis, trade-offs, and cloud cost estimates are in [REPORT.md](REPORT.md).

| Metric | Local (1 core) | Distributed (8 cores) |
|---|---|---|
| Total Runtime | 28.27 s | 18.59 s |
| Speedup | 1.00× | 1.52× |
| Shuffle Volume | 0 MB | ~450 MB |
| Peak Memory | ~1.5 GB | ~1.02 GB/worker |
| Partitions | 1 | 200 |
