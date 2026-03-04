import os
import json
import time
from dotenv import load_dotenv
from kafka import KafkaConsumer
import snowflake.connector

load_dotenv()

TOPIC = "crypto_prices"

def sf_connect():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BROKER"),
        group_id="snowflake_sink",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8"))
    )

    insert_sql = """
        INSERT INTO PRICES (coin, price_usd, change_24h, event_time)
        VALUES (%s, %s, %s, %s)
    """

    print("Consumer started. Waiting for messages...")

    con = sf_connect()
    cur = con.cursor()

    buffer = []
    last_flush = time.time()

    for msg in consumer:
        v = msg.value
        buffer.append((v["coin"], v["price_usd"], v["change_24h"], v["event_time"]))
        print(f"Received → {v['coin'].upper():<12} ${v['price_usd']:>10,.2f}  ({v['change_24h']}%)")

        if len(buffer) >= 20 or (time.time() - last_flush) >= 10:
            cur.executemany(insert_sql, buffer)
            print(f"✅ Flushed {len(buffer)} records to Snowflake")
            buffer.clear()
            last_flush = time.time()

if __name__ == "__main__":
    main()
