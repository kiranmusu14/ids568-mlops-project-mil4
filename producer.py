import argparse
import json
import time
import random
import socket
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def generate_stream(rate_per_sec, host='localhost', port=9999):
    """Generates streaming events with realistic patterns (steady-state and bursts)."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(1)
    
    logging.info(f"Producer listening on {host}:{port} at {rate_per_sec} msgs/sec...")

    while True:
        logging.info("Waiting for consumer to connect...")
        conn, addr = server_socket.accept()
        logging.info(f"Consumer connected from {addr}")
        
        try:
            while True:
                # Simulate bursts: 10% chance to temporarily multiply load by 5
                current_rate = rate_per_sec * 5 if random.random() < 0.1 else rate_per_sec
                sleep_time = 1.0 / current_rate if current_rate > 0 else 0
                
                event = {
                    "event_id": random.randint(10000, 99999),
                    "timestamp": time.time(), # float for accurate latency math
                    "amount": round(random.uniform(5.0, 500.0), 2),
                    "type": random.choice(["purchase", "refund", "view"])
                }
                
                # Serialize to JSON and send over the socket
                message = json.dumps(event) + "\n"
                conn.sendall(message.encode('utf-8'))
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
        except (BrokenPipeError, ConnectionResetError):
            logging.warning("Consumer disconnected. Waiting for reconnect to simulate recovery...")
        except KeyboardInterrupt:
            logging.info("Producer shutting down.")
            break
            
    server_socket.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Streaming Event Producer")
    parser.add_argument("--rate", type=int, default=100, help="Target messages per second")
    args = parser.parse_args()
    
    generate_stream(args.rate)