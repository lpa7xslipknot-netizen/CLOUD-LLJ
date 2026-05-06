"""
kafka_producer.py — Simulates real-time data ingestion for all 4 e-commerce
data sources into Apache Kafka topics.

Data Sources:
  1. Weblogs       → 2 TB/day   (Unstructured JSON)
  2. Transactions  → 500 GB/day (Structured JSON)
  3. Reviews       → 200 GB/day (Semi-structured JSON)
  4. Social Media  → 100 GB/day (Unstructured JSON)
"""

import json
import time
import random
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

fake = Faker()

# ── Kafka Configuration ────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"

TOPICS = {
    "weblogs":      "weblogs",
    "transactions": "transactions",
    "reviews":      "customer_reviews",
    "social":       "social_feeds",
}

PAGES = ["/home", "/product/123", "/cart", "/checkout", "/search",
         "/category/electronics", "/deals", "/account", "/wishlist"]
DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
COUNTRIES = ["US", "IN", "UK", "DE", "FR", "AU", "CA", "BR", "JP", "SG"]
PRODUCTS = [
    {"id": "P001", "name": "Laptop Pro 15",     "price": 1299.99, "category": "Electronics"},
    {"id": "P002", "name": "Wireless Headphones","price": 199.99, "category": "Electronics"},
    {"id": "P003", "name": "Running Shoes",      "price": 89.99,  "category": "Fashion"},
    {"id": "P004", "name": "Coffee Maker",       "price": 49.99,  "category": "Home"},
    {"id": "P005", "name": "Python Book",        "price": 39.99,  "category": "Books"},
]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "UPI", "net_banking"]
STATUS_LIST = ["delivered", "shipped", "processing", "cancelled", "returned"]
PLATFORMS = ["facebook", "twitter", "instagram", "reddit"]
SENTIMENTS = ["positive", "negative", "neutral"]


# ── Data Generators ────────────────────────────────────────────────────────────

def generate_weblog() -> dict:
    """Generate a single web log entry."""
    return {
        "event_id":    fake.uuid4(),
        "timestamp":   datetime.utcnow().isoformat(),
        "session_id":  fake.uuid4(),
        "customer_id": f"C{random.randint(1000, 9999)}",
        "ip_address":  fake.ipv4(),
        "page":        random.choice(PAGES),
        "action":      random.choice(["click", "search", "view", "scroll", "hover"]),
        "device":      random.choice(DEVICES),
        "browser":     random.choice(BROWSERS),
        "country":     random.choice(COUNTRIES),
        "duration_sec": random.randint(1, 300),
        "referrer":    random.choice(["google", "direct", "email", "social", "none"]),
    }


def generate_transaction() -> dict:
    """Generate a single transaction record."""
    product = random.choice(PRODUCTS)
    qty = random.randint(1, 5)
    return {
        "order_id":       f"ORD{random.randint(100000, 999999)}",
        "timestamp":      datetime.utcnow().isoformat(),
        "customer_id":    f"C{random.randint(1000, 9999)}",
        "product_id":     product["id"],
        "product_name":   product["name"],
        "category":       product["category"],
        "quantity":       qty,
        "unit_price":     product["price"],
        "total_amount":   round(product["price"] * qty, 2),
        "discount_pct":   random.choice([0, 5, 10, 15, 20]),
        "payment_method": random.choice(PAYMENT_METHODS),
        "status":         random.choice(STATUS_LIST),
        "country":        random.choice(COUNTRIES),
        "delivery_days":  random.randint(1, 10),
    }


def generate_review() -> dict:
    """Generate a customer review entry."""
    rating = random.randint(1, 5)
    product = random.choice(PRODUCTS)
    return {
        "review_id":   fake.uuid4(),
        "timestamp":   datetime.utcnow().isoformat(),
        "customer_id": f"C{random.randint(1000, 9999)}",
        "product_id":  product["id"],
        "product_name":product["name"],
        "rating":      rating,
        "title":       fake.sentence(nb_words=6),
        "body":        fake.paragraph(nb_sentences=3),
        "sentiment":   "positive" if rating >= 4 else ("negative" if rating <= 2 else "neutral"),
        "has_image":   random.choice([True, False]),
        "helpful_votes": random.randint(0, 50),
        "verified_purchase": random.choice([True, False]),
    }


def generate_social_feed() -> dict:
    """Generate a social media feed entry."""
    return {
        "post_id":     fake.uuid4(),
        "timestamp":   datetime.utcnow().isoformat(),
        "platform":    random.choice(PLATFORMS),
        "user_handle": fake.user_name(),
        "customer_id": f"C{random.randint(1000, 9999)}" if random.random() > 0.3 else None,
        "content":     fake.sentence(nb_words=15),
        "hashtags":    [f"#{fake.word()}" for _ in range(random.randint(0, 4))],
        "sentiment":   random.choice(SENTIMENTS),
        "likes":       random.randint(0, 10000),
        "shares":      random.randint(0, 1000),
        "brand_mention": random.choice([True, False]),
        "country":     random.choice(COUNTRIES),
    }


# ── Kafka Producer ─────────────────────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    """Initialize and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        batch_size=16384,
        linger_ms=10,
        compression_type="gzip",
    )


def send_message(producer: KafkaProducer, topic: str, key: str, data: dict):
    """Send a single message to a Kafka topic."""
    future = producer.send(topic, key=key, value=data)
    future.get(timeout=10)   # block until confirmed
    logger.debug(f"Sent to {topic}: {key}")


def run_producer(messages_per_source: int = 100, delay_sec: float = 0.05):
    """
    Main producer loop — sends messages for all 4 data sources.

    Args:
        messages_per_source: How many messages to produce per source
        delay_sec: Delay between each batch (simulate real-time flow)
    """
    logger.info("Starting Kafka producer for all 4 data sources…")

    try:
        producer = create_producer()
    except Exception as e:
        logger.error(f"Could not connect to Kafka broker at {KAFKA_BROKER}: {e}")
        logger.info("Running in DEMO mode — printing sample messages instead.")
        _demo_mode(messages_per_source)
        return

    counts = {k: 0 for k in TOPICS}

    for i in range(messages_per_source):
        # 1. Weblog
        wlog = generate_weblog()
        send_message(producer, TOPICS["weblogs"], wlog["event_id"], wlog)
        counts["weblogs"] += 1

        # 2. Transaction
        txn = generate_transaction()
        send_message(producer, TOPICS["transactions"], txn["order_id"], txn)
        counts["transactions"] += 1

        # 3. Review
        review = generate_review()
        send_message(producer, TOPICS["reviews"], review["review_id"], review)
        counts["reviews"] += 1

        # 4. Social
        social = generate_social_feed()
        send_message(producer, TOPICS["social"], social["post_id"], social)
        counts["social"] += 1

        if (i + 1) % 10 == 0:
            logger.info(f"Progress: {i+1}/{messages_per_source} batches sent | {counts}")

        time.sleep(delay_sec)

    producer.flush()
    producer.close()
    logger.info(f"✅ Producer finished. Total messages sent: {counts}")


def _demo_mode(n: int = 5):
    """Print sample messages when Kafka is not available."""
    print("\n=== DEMO MODE — Sample Generated Messages ===\n")

    print("--- WEBLOG SAMPLE ---")
    print(json.dumps(generate_weblog(), indent=2))

    print("\n--- TRANSACTION SAMPLE ---")
    print(json.dumps(generate_transaction(), indent=2))

    print("\n--- REVIEW SAMPLE ---")
    print(json.dumps(generate_review(), indent=2))

    print("\n--- SOCIAL FEED SAMPLE ---")
    print(json.dumps(generate_social_feed(), indent=2))


if __name__ == "__main__":
    # Run with 50 messages per source; set delay to 0 for fastest demo
    run_producer(messages_per_source=50, delay_sec=0.01)
