# # producer.py
# import requests
# import csv
# import time
# from kafka import KafkaProducer

# # Kafka setup
# producer = KafkaProducer(bootstrap_servers='155.101.6.191:9092')

# # CSV URL
# csv_url = 'https://horel.chpc.utah.edu/data/meop/level3/trx_2025/trx_min_2025_05.csv'

# # Stream CSV from URL
# response = requests.get(csv_url, stream=True)
# lines = (line.decode('utf-8') for line in response.iter_lines())

# # Parse CSV
# reader = csv.reader(lines)
# header = next(reader)  # Skip header if needed

# # Stream each row as a Kafka message
# for row in reader:
#     message = ','.join(row)
#     producer.send('test-topic', value=message.encode('utf-8'))
#     print(f"Produced: {message}")
#     time.sleep(1)  # Optional: throttle to simulate live stream

# producer.flush()

#!/usr/bin/env python3
#!/usr/bin/env python3
import pandas as pd
import msgpack
import blosc
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka import KafkaConsumer

# Configuration
CSV_URL = "https://horel.chpc.utah.edu/data/meop/level3/trx_2025/trx_min_2025_05.csv"
TOPIC = "test-topic"
BOOTSTRAP = "155.101.6.191:9092"
CHUNK_SIZE = 25_000  # rows per message

def compress_data(data: dict) -> bytes:
    packed = msgpack.packb(data, use_bin_type=True)
    return blosc.compress(packed, cname="zstd", clevel=5, shuffle=blosc.SHUFFLE)

def main():
    # Load CSV (simple: all into memory)
    df = pd.read_csv(CSV_URL)
    total_rows = len(df)
    print(f"Loaded CSV with {total_rows} rows and {len(df.columns)} columns")

    # Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        linger_ms=0,
        max_request_size=5 * 1024 * 1024,  # keep under your broker/topic limits
    )

    # Stable key so all chunks go to the same partition (ordered delivery)
    key = CSV_URL.encode("utf-8")

    # Chunk and send
    for i in range(0, total_rows, CHUNK_SIZE):
        chunk = df.iloc[i:i + CHUNK_SIZE]
        payload = {
            "values": chunk.to_dict(orient="list"),
            "stream_info": {
                "source_url": CSV_URL,
                "rows": len(chunk),
                "cols": list(chunk.columns),
                "chunk_index": i // CHUNK_SIZE,
                "start_row": i,
                "end_row": i + len(chunk) - 1,
                "encoding": "msgpack+blosc(zstd5,shuffle)",
            },
        }
        blob = compress_data(payload)
        producer.send(TOPIC, key=key, value=blob).get(timeout=30)
        print(f"Sent chunk {i // CHUNK_SIZE} with {len(chunk)} rows "
              f"(compressed: {len(blob)} bytes)")
    producer.flush()
    producer.close()
    print(f"All chunks sent to topic '{TOPIC}'")
    c = KafkaConsumer(bootstrap_servers=BOOTSTRAP, metadata_max_age_ms=1000)
    print("Topics:", sorted(c.topics()))
    admin = KafkaAdminClient(bootstrap_servers='155.101.6.191:9092')
    admin.delete_topics(['test-topic'])
    print("Topics:", sorted(c.topics()))
    # md = admin.list_topics()
    # print(sorted(md.topics.keys()))
if __name__ == "__main__":
    main()



