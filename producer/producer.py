import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "trades"

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
SIDES = ["BUY", "SELL"]


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def generate_trade():
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "symbol": random.choice(SYMBOLS),
        "price": round(random.uniform(100, 1000), 2),
        "quantity": random.randint(1, 500),
        "side": random.choice(SIDES),
        "source": "python-producer",
    }


try:
    print("Starting Kafka producer...")

    while True:
        trade = generate_trade()

        future = producer.send(
            KAFKA_TOPIC,
	    key=trade["symbol"].encode("utf-8"),
            value=trade,
        )

        metadata = future.get(timeout=10)

        print(
            f"Sent event_id={trade['event_id']} "
            f"symbol={trade['symbol']} "
            f"partition={metadata.partition} "
            f"offset={metadata.offset}"
        )

        time.sleep(2)

except KeyboardInterrupt:
    print("\nProducer stopped.")

finally:
    producer.flush()
    producer.close()