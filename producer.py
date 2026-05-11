from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092", 
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    data = {
        "id": random.randint(1, 1000),
        "value": random.randint(1000, 9999),
        "amount": round(random.uniform(10.5, 500.0), 2),
        "currency": random.choice(["USD", "EUR", "EGP", "SAR"]),
        "timestamp": int(time.time()),
    }

    producer.send("transaction", data)  
    print("Sent:", data)
    time.sleep(1)
