from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {
    "count": 0,
    "total_revenue": 0,
    "min_amount": float('inf'),
    "max_amount": float('-inf')
})

msg_count = 0

print("Listening for category statistics...")

for message in consumer:
    tx = message.value
    category = tx["category"]
    amount = tx["amount"]

    stats[category]["count"] += 1
    stats[category]["total_revenue"] += amount
    stats[category]["min_amount"] = min(stats[category]["min_amount"], amount)
    stats[category]["max_amount"] = max(stats[category]["max_amount"], amount)

    msg_count += 1

    if msg_count % 10 == 0:
        print("\nCategory | Count | Total Revenue | Min Amount | Max Amount")
        for c, s in stats.items():
            print(
                f"{c} | {s['count']} | {round(s['total_revenue'], 2)} | "
                f"{round(s['min_amount'], 2)} | {round(s['max_amount'], 2)}"
            )