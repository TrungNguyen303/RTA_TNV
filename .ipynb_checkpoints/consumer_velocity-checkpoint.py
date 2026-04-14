from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    group_id='velocity-group-2',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_timestamps = defaultdict(list)

print("Listening for velocity anomalies...")

for message in consumer:
    tx = message.value
    user_id = tx["user_id"]
    tx_time = datetime.fromisoformat(tx["timestamp"])

    user_timestamps[user_id].append(tx_time)

    cutoff = tx_time - timedelta(seconds=60)
    user_timestamps[user_id] = [
        t for t in user_timestamps[user_id] if t >= cutoff
    ]

    if len(user_timestamps[user_id]) > 3:
        print(
            f"ALERT: Velocity anomaly detected for {user_id} "
            f"| {len(user_timestamps[user_id])} transactions in 60 seconds "
            f"| latest tx: {tx['tx_id']}"
        )