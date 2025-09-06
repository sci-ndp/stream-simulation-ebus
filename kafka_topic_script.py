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
import re

# ---- Configuration ----
load_dotenv(override=True)

# Kafka Configuration
KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_PORT = os.getenv("KAFKA_PORT")
BOOTSTRAP = f"{KAFKA_HOST}:{KAFKA_PORT}"
CHUNK_SIZE = 25_000  # starting rows per message
SOFT_CAP_BYTES = 950_000  # stay under common 1MB broker limit

def generate_resource_name(url: str) -> str:
    path = up.urlparse(url).path
    fname = path.split('/')[-1] or "resource"
    # lowercase + ascii-only (drop non-ascii)
    fname = fname.encode("ascii", "ignore").decode("ascii").lower()
    # replace any disallowed char with '-'
    fname = re.sub(r'[^a-z0-9_-]+', '-', fname)
    # collapse repeats and trim separators
    fname = re.sub(r'[-_]{2,}', '-', fname).strip('-_')
    # fallback if empty after sanitization
    if not fname:
        fname = "resource"
    return fname


def stream_register(url: str): 
    topic = generate_resource_name(url)   
    # Configure Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    try:
        while True:
            # Fetch and stream CSV rows from remote URL
            response = requests.get(url)
            response.raise_for_status()  # raises error if download fails
            f = io.StringIO(response.text)
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                producer.send(topic, value=row)
                print(f"Sent: {row}")
                time.sleep(1.0)
            time.sleep(10)
    except KeyboardInterrupt:
        print("Producer interrupted. Flushing and closing...")
        producer.flush()
        producer.close()

def main():
    # Read URLs from bus13_url.txt
    bus13_urls = [
        "https://horel.chpc.utah.edu/data/meop/data/BUS13_2024_04.csv",
        "https://horel.chpc.utah.edu/data/meop/data/BUS13_2024_05.csv",
        "https://horel.chpc.utah.edu/data/meop/data/BUS13_2024_06.csv",
        "https://horel.chpc.utah.edu/data/meop/data/BUS13_2024_07.csv",
        "https://horel.chpc.utah.edu/data/meop/data/BUS13_2024_08.csv",
        "https://horel.chpc.utah.edu/data/meop/data/BUS13_2024_09.csv",
        "https://horel.chpc.utah.edu/data/meop/data/BUS13_2024_10.csv",
        "https://horel.chpc.utah.edu/data/meop/data/BUS13_2024_11.csv",
        "https://horel.chpc.utah.edu/data/meop/data/BUS13_2024_12.csv",
        "https://horel.chpc.utah.edu/data/meop/data/BUS13_2025_01.csv",
        "https://horel.chpc.utah.edu/data/meop/data/BUS13_2025_02.csv",
        "https://horel.chpc.utah.edu/data/meop/data/BUS13_2025_03.csv",
    ]


    # Start a thread for each URL to stream data concurrently
    threads = []
    for url in bus13_urls:
        t = threading.Thread(target=stream_register, args=(url,))
        t.start()
        threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
