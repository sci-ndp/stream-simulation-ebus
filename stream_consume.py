import msgpack
import blosc
from kafka import KafkaConsumer

# Kafka setup
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='155.101.6.191:9092',
    auto_offset_reset='earliest',
    group_id=None,
    value_deserializer=lambda x: x  # Raw bytes; decode manually
)

def try_decompress(blob: bytes):
    try:
        unpacked = blosc.decompress(blob)
        return msgpack.unpackb(unpacked, raw=False)
    except Exception:
        return None  # Not a compressed binary message

print("🔄 Listening to Kafka stream...")

for message in consumer:
    raw = message.value

    # Try decompressing as binary
    data = try_decompress(raw)
    if data:
        info = data.get("stream_info", {})
        print(f"\n📦 Binary chunk received: {info.get('rows', '?')} rows")
        print(f"🔗 Source: {info.get('source_url', 'N/A')} | Chunk index: {info.get('chunk_index', 'N/A')}")
        preview = list(zip(*data["values"].values()))[:3]
        for row in preview:
            print("→", row)
    else:
        # Fallback: treat as plain UTF-8 text
        try:
            text = raw.decode("utf-8")
            print(f"📝 Text message: {text}")
        except UnicodeDecodeError as e:
            print(f"❌ Unrecognized message format: {e}")
