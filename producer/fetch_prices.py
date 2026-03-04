import requests
import time
import json
from datetime import datetime
from kafka import KafkaProducer

COINS = ["bitcoin", "ethereum", "solana", "cardano"]
KAFKA_TOPIC = "crypto_prices"
KAFKA_BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_and_send():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": ",".join(COINS),
        "vs_currencies": "usd",
        "include_24hr_change": "true"
    }
    response = requests.get(url, params=params)
    data = response.json()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for coin, values in data.items():
        record = {
            "coin": coin,
            "price_usd": values["usd"],
            "change_24h": round(values["usd_24h_change"], 2),
            "event_time": timestamp
        }
        producer.send(KAFKA_TOPIC, value=record)
        print(f"SENT → {coin.upper():<12} ${record['price_usd']:>10,.2f}   ({record['change_24h']}%)   at {timestamp}")

    producer.flush()

if __name__ == "__main__":
    print("Producer started. Sending to Kafka every 30 seconds...")
    while True:
        fetch_and_send()
        time.sleep(30)