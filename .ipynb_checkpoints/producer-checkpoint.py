from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

tx_counter = 1

def generate_transaction():
    global tx_counter

    transaction = {
        "tx_id": f"TX{tx_counter:04d}",
        "user_id": f"u{random.randint(1,20):02d}",
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(["Warsaw", "Krakow", "Gdansk", "Wroclaw"]),
        "category": random.choice(["electronics", "clothing", "food", "books"]),
        "timestamp": datetime.now().isoformat()
    }

    tx_counter += 1
    return transaction

for _ in range(50):
    tx = generate_transaction()
    producer.send('transactions', tx)
    print(tx)
    time.sleep(1)

producer.flush()