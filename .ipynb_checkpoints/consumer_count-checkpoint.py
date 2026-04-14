from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

print("Listening for transactions...")

for message in consumer:
    tx = message.value
    store = tx["store"]
    amount = tx["amount"]

    store_counts[store] += 1

    if store not in total_amount:
        total_amount[store] = 0
    total_amount[store] += amount

    msg_count += 1

    if msg_count % 10 == 0:
        print("\nStore | Count | Total Amount | Avg Amount")
        for s in store_counts:
            avg_amount = total_amount[s] / store_counts[s]
            print(f"{s} | {store_counts[s]} | {round(total_amount[s], 2)} | {round(avg_amount, 2)}")