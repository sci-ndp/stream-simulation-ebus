import threading
import urllib.parse as up
import csv
import requests
import io
import json
import os
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import time
from helper_func import generate_resource_name
from pathlib import Path

# ---- Configuration ----
load_dotenv(override=True)

# Kafka Configuration
KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_PORT = os.getenv("KAFKA_PORT")
BOOTSTRAP = f"{KAFKA_HOST}:{KAFKA_PORT}"

def stream_register(filepath: str): 
    filename = Path(filepath).name
    topic = generate_resource_name(filename)
    print(f"Streaming to topic: {topic}")

    # Configure Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    try:
        # Open and stream file using context manager
        while True:
            print("Started sending rows again!")
            with open(filepath, mode="r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                sent = 0
                for row in reader:
                    sent += 1
                    producer.send(topic, value=row)
                    # print(f"Sent: {row}")
                    if sent % 100000 == 0:
                        print(f"sent {sent} rows")
                    time.sleep(0.0001)
            print("All rows sent. Flushing and closing producer...")
            producer.flush()
    except (KeyboardInterrupt, Exception) as e:
        print(f"Error during streaming: {e}")
    finally:
        producer.close()



def main():
    # Read URLs from bus13_url.txt
    folder_path = "files"
    files = [
        os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".csv") and os.path.isfile(os.path.join(folder_path, f))
    ]

    threads = []
    for name in files:
        if name.endswith("ebus_min_2025_01.csv"):
            t = threading.Thread(target=stream_register, args=(name,))
            t.start()
            threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
