import argparse
import numpy as np
import pandas as pd
import os
import logging

# Set up logging for execution tracking
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_synthetic_data(num_rows, output_dir, seed, chunk_size=1000000):
    """Generates synthetic data and saves it in chunked Parquet files for distributed processing."""
    # Seeded randomness is required so results can be independently verified [cite: 218]
    np.random.seed(seed)
    os.makedirs(output_dir, exist_ok=True)
    
    logging.info(f"Starting data generation: {num_rows} rows to {output_dir} with seed {seed}")
    
    rows_generated = 0
    file_index = 0
    
    while rows_generated < num_rows:
        current_chunk_size = min(chunk_size, num_rows - rows_generated)
        
        # Simulate realistic features: IDs, numerical metrics, and categorical skew
        data = {
            'user_id': np.random.randint(1000, 9999, size=current_chunk_size),
            'transaction_amount': np.round(np.random.exponential(scale=50.0, size=current_chunk_size), 2),
            'event_timestamp': pd.date_range(start='2024-01-01', periods=current_chunk_size, freq='S'),
            'category_code': np.random.choice(['A', 'B', 'C', 'D'], size=current_chunk_size, p=[0.1, 0.2, 0.5, 0.2])
        }
        
        df = pd.DataFrame(data)
        
        # --- THE FIX: Convert nanoseconds to microseconds for PySpark compatibility ---
        df['event_timestamp'] = df['event_timestamp'].astype('datetime64[us]')
        
        output_file = os.path.join(output_dir, f"data_part_{file_index}.parquet")
        df.to_parquet(output_file, index=False)
        
        rows_generated += current_chunk_size
        file_index += 1
        logging.info(f"Generated {rows_generated}/{num_rows} rows...")

    logging.info("Data generation complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic data for Milestone 4")
    parser.add_argument("--rows", type=int, default=10000000, help="Number of rows to generate")
    parser.add_argument("--output", type=str, required=True, help="Output directory path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    
    args = parser.parse_args()
    generate_synthetic_data(args.rows, args.output, args.seed)