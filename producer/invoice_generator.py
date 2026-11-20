#!/usr/bin/env python3
"""Runs on your HOST machine. Sends one fake invoice to Kafka every 2 seconds."""
import json, random, time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "invoices"
categories = ["Electronics", "Clothing", "Food", "Books", "Furniture"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def generate_invoice():
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "category": random.choice(categories),
        "price": round(random.uniform(10, 1000), 2),
    }

while True:
    invoice = generate_invoice()
    producer.send(TOPIC, value=invoice).get(timeout=10)
    print(f"Invoice sent: {invoice}")
    time.sleep(2)
