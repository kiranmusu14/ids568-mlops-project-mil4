import socket
import json
import time
import logging
import numpy as np
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(message)s')

def process_stream(host='localhost', port=9999):
    """Consumes the stream and applies a stateful tumbling window aggregation."""
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Retry connection logic to handle producer restarts or delays
    while True:
        try:
            client_socket.connect((host, port))
            logging.info(f"Connected to producer at {host}:{port}")
            break
        except ConnectionRefusedError:
            logging.warning("Producer not found. Retrying in 2 seconds...")
            time.sleep(2)

    window_size = 5.0  # 5-second tumbling window
    current_window_start = time.time()
    
    latencies = []
    event_count = 0
    state = defaultdict(float)
    buffer = ""
    
    logging.info("Consumer started. Waiting for events...")
    
    try:
        while True:
            data = client_socket.recv(8192).decode('utf-8')
            if not data:
                break
                
            buffer += data
            # Process complete JSON lines from the buffer
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if not line.strip(): continue
                
                event = json.loads(line)
                current_time = time.time()
                
                # Latency calculation
                latency_ms = (current_time - event["timestamp"]) * 1000
                latencies.append(latency_ms)
                event_count += 1
                
                # Stateful Logic (Aggregation)
                if event["type"] == "purchase":
                    state["total_purchase_volume"] += event["amount"]
                
                # Windowing Logic
                if current_time - current_window_start >= window_size:
                    if latencies:
                        # Calculate required metrics
                        p50 = np.percentile(latencies, 50)
                        p95 = np.percentile(latencies, 95)
                        p99 = np.percentile(latencies, 99)
                        throughput = event_count / window_size
                        
                        print(f"\n--- Window Closed ({window_size}s) ---")
                        print(f"Throughput: {throughput:.2f} msg/s")
                        print(f"Latencies - p50: {p50:.2f}ms | p95: {p95:.2f}ms | p99: {p99:.2f}ms")
                        print(f"State: {dict(state)}")
                        
                    # Reset window state
                    latencies.clear()
                    event_count = 0
                    state.clear()
                    current_window_start = current_time
                    
    except KeyboardInterrupt:
        logging.info("\nConsumer manually stopped.")
    except Exception as e:
        logging.error(f"Consumer error: {e}")
    finally:
        client_socket.close()

if __name__ == "__main__":
    process_stream()