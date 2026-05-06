"""
spark_pipeline.py — Apache Spark processing pipeline for the e-commerce
Big Data case study.

Pipelines:
  1. Weblog Analysis      — Session metrics, page popularity, bounce rate
  2. Transaction Analysis — Revenue, top products, customer spend
  3. Review Analysis      — Sentiment summary, rating distribution
  4. Social Media Analysis— Trending hashtags, brand sentiment
  5. Customer 360 View    — Unified table joining all 4 sources
"""

import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Paths (local fallback for demo / GitHub) ───────────────────────────────────
BASE_PATH     = os.path.join(os.path.dirname(__file__), "..", "..", "data")
RAW_PATH      = os.path.join(BASE_PATH, "raw")
PROCESSED_PATH= os.path.join(BASE_PATH, "processed")


# ══════════════════════════════════════════════════════════════════════════════
# 1. Spark Session
# ══════════════════════════════════════════════════════════════════════════════

def create_spark_session(app_name: str = "EcommerceAnalytics") -> SparkSession:
    """Create and return a configured SparkSession."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    logger.info(f"Spark session created: {app_name}")
    return spark


# ══════════════════════════════════════════════════════════════════════════════
# 2. Data Loading
# ══════════════════════════════════════════════════════════════════════════════

def load_json(spark: SparkSession, path: str) -> DataFrame:
    """Load JSON data from path."""
    logger.info(f"Loading JSON from: {path}")
    return spark.read.json(path)


def load_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Load Parquet data from path."""
    logger.info(f"Loading Parquet from: {path}")
    return spark.read.parquet(path)


def load_csv(spark: SparkSession, path: str) -> DataFrame:
    """Load CSV data with header."""
    logger.info(f"Loading CSV from: {path}")
    return spark.read.csv(path, header=True, inferSchema=True)


# ══════════════════════════════════════════════════════════════════════════════
# 3. Weblog Analysis
# ══════════════════════════════════════════════════════════════════════════════

def analyze_weblogs(df: DataFrame) -> dict:
    """
    Analyze web log data.

    Returns dict of result DataFrames:
      - page_popularity
      - device_breakdown
      - country_traffic
      - session_stats
    """
    logger.info("Running weblog analysis…")

    # Top pages by views
    page_popularity = (
        df.groupBy("page")
        .agg(
            F.count("event_id").alias("total_views"),
            F.countDistinct("session_id").alias("unique_sessions"),
            F.avg("duration_sec").alias("avg_duration_sec"),
        )
        .orderBy(F.desc("total_views"))
    )

    # Device breakdown
    device_breakdown = (
        df.groupBy("device")
        .agg(F.count("event_id").alias("events"))
        .withColumn("pct", F.round(
            F.col("events") / F.sum("events").over(Window.rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing
            )) * 100, 2
        ))
    )

    # Traffic by country
    country_traffic = (
        df.groupBy("country")
        .agg(
            F.count("event_id").alias("events"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy(F.desc("events"))
    )

    # Session-level stats
    session_stats = (
        df.groupBy("session_id", "customer_id")
        .agg(
            F.count("event_id").alias("clicks"),
            F.sum("duration_sec").alias("total_time_sec"),
            F.min("timestamp").alias("session_start"),
            F.max("timestamp").alias("session_end"),
        )
    )

    return {
        "page_popularity": page_popularity,
        "device_breakdown": device_breakdown,
        "country_traffic": country_traffic,
        "session_stats": session_stats,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 4. Transaction Analysis
# ══════════════════════════════════════════════════════════════════════════════

def analyze_transactions(df: DataFrame) -> dict:
    """
    Analyze transaction/order data.

    Returns dict of result DataFrames:
      - revenue_by_category
      - top_products
      - customer_spend
      - payment_method_stats
      - daily_revenue
    """
    logger.info("Running transaction analysis…")

    # Apply discount to get net amount
    df = df.withColumn(
        "net_amount",
        F.round(F.col("total_amount") * (1 - F.col("discount_pct") / 100), 2)
    )

    # Revenue by product category
    revenue_by_category = (
        df.filter(F.col("status") != "cancelled")
        .groupBy("category")
        .agg(
            F.sum("net_amount").alias("total_revenue"),
            F.count("order_id").alias("orders"),
            F.avg("net_amount").alias("avg_order_value"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    # Top products
    top_products = (
        df.filter(F.col("status") != "cancelled")
        .groupBy("product_id", "product_name")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("net_amount").alias("revenue"),
        )
        .orderBy(F.desc("revenue"))
        .limit(10)
    )

    # Customer spend — Customer 360 building block
    customer_spend = (
        df.filter(F.col("status").isin("delivered", "shipped"))
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("net_amount").alias("total_spend"),
            F.avg("net_amount").alias("avg_order_value"),
            F.max("timestamp").alias("last_order_date"),
        )
    )

    # Payment method breakdown
    payment_method_stats = (
        df.groupBy("payment_method")
        .agg(
            F.count("order_id").alias("transactions"),
            F.sum("net_amount").alias("total_amount"),
        )
        .orderBy(F.desc("transactions"))
    )

    return {
        "revenue_by_category": revenue_by_category,
        "top_products": top_products,
        "customer_spend": customer_spend,
        "payment_method_stats": payment_method_stats,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 5. Review Analysis
# ══════════════════════════════════════════════════════════════════════════════

def analyze_reviews(df: DataFrame) -> dict:
    """
    Analyze customer review data.

    Returns dict of result DataFrames:
      - sentiment_summary
      - rating_distribution
      - product_ratings
    """
    logger.info("Running review analysis…")

    # Overall sentiment
    sentiment_summary = (
        df.groupBy("sentiment")
        .agg(
            F.count("review_id").alias("count"),
            F.avg("rating").alias("avg_rating"),
        )
        .orderBy(F.desc("count"))
    )

    # Rating distribution (1-5 stars)
    rating_distribution = (
        df.groupBy("rating")
        .agg(F.count("review_id").alias("reviews"))
        .orderBy("rating")
    )

    # Product-level ratings
    product_ratings = (
        df.groupBy("product_id", "product_name")
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.count("review_id").alias("total_reviews"),
            F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("positive_reviews"),
            F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("negative_reviews"),
        )
        .withColumn("avg_rating", F.round("avg_rating", 2))
        .orderBy(F.desc("avg_rating"))
    )

    return {
        "sentiment_summary": sentiment_summary,
        "rating_distribution": rating_distribution,
        "product_ratings": product_ratings,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 6. Social Media Analysis
# ══════════════════════════════════════════════════════════════════════════════

def analyze_social_media(df: DataFrame) -> dict:
    """
    Analyze social media feed data.

    Returns dict of result DataFrames:
      - platform_stats
      - brand_sentiment
      - top_customers_social
    """
    logger.info("Running social media analysis…")

    # Platform engagement
    platform_stats = (
        df.groupBy("platform")
        .agg(
            F.count("post_id").alias("posts"),
            F.sum("likes").alias("total_likes"),
            F.sum("shares").alias("total_shares"),
            F.avg("likes").alias("avg_likes"),
        )
        .orderBy(F.desc("posts"))
    )

    # Brand sentiment across platforms
    brand_sentiment = (
        df.filter(F.col("brand_mention") == True)
        .groupBy("platform", "sentiment")
        .agg(F.count("post_id").alias("mentions"))
        .orderBy("platform", F.desc("mentions"))
    )

    # Social-active known customers
    top_customers_social = (
        df.filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            F.count("post_id").alias("posts"),
            F.sum("likes").alias("total_likes"),
        )
        .orderBy(F.desc("posts"))
        .limit(20)
    )

    return {
        "platform_stats": platform_stats,
        "brand_sentiment": brand_sentiment,
        "top_customers_social": top_customers_social,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 7. Customer 360 — Unified View  (Solves "No Single Customer View" problem)
# ══════════════════════════════════════════════════════════════════════════════

def build_customer_360(
    weblog_df: DataFrame,
    transaction_df: DataFrame,
    review_df: DataFrame,
    social_df: DataFrame,
) -> DataFrame:
    """
    Build a unified Customer 360 view by joining all 4 data sources
    on customer_id.

    This directly solves the 'No Single Customer View' problem.
    """
    logger.info("Building Customer 360 unified view…")

    # Web engagement per customer
    web_stats = (
        weblog_df.groupBy("customer_id")
        .agg(
            F.count("event_id").alias("web_clicks"),
            F.countDistinct("session_id").alias("sessions"),
            F.avg("duration_sec").alias("avg_session_sec"),
        )
    )

    # Transaction stats per customer
    txn_stats = (
        transaction_df
        .filter(F.col("status").isin("delivered", "shipped"))
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_spend"),
            F.avg("total_amount").alias("avg_order_value"),
            F.max("timestamp").alias("last_purchase_date"),
        )
    )

    # Review behaviour per customer
    review_stats = (
        review_df.groupBy("customer_id")
        .agg(
            F.count("review_id").alias("reviews_written"),
            F.avg("rating").alias("avg_rating_given"),
        )
        .withColumn("avg_rating_given", F.round("avg_rating_given", 2))
    )

    # Social activity per customer
    social_stats = (
        social_df.filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            F.count("post_id").alias("social_posts"),
            F.sum("likes").alias("social_likes"),
        )
    )

    # Join all four
    customer_360 = (
        web_stats
        .join(txn_stats,    on="customer_id", how="outer")
        .join(review_stats, on="customer_id", how="outer")
        .join(social_stats, on="customer_id", how="outer")
        .fillna(0)
        .orderBy(F.desc("total_spend"))
    )

    logger.info(f"Customer 360 built with {customer_360.count()} customers.")
    return customer_360


# ══════════════════════════════════════════════════════════════════════════════
# 8. Save Results
# ══════════════════════════════════════════════════════════════════════════════

def save_results(df: DataFrame, name: str, fmt: str = "parquet"):
    """Save a result DataFrame to the processed directory."""
    out_path = os.path.join(PROCESSED_PATH, name)
    logger.info(f"Saving '{name}' to {out_path} as {fmt}…")
    if fmt == "parquet":
        df.write.mode("overwrite").parquet(out_path)
    elif fmt == "csv":
        df.write.mode("overwrite").option("header", True).csv(out_path)
    else:
        df.write.mode("overwrite").json(out_path)
    logger.info(f"  ✅ Saved: {name}")


# ══════════════════════════════════════════════════════════════════════════════
# 9. Main Pipeline
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    """Execute the full end-to-end processing pipeline."""
    spark = create_spark_session()

    # ── Generate sample data if no raw files exist ─────────────────────────
    _generate_sample_data_if_needed(spark)

    # ── Load raw data ──────────────────────────────────────────────────────
    weblog_df     = load_json(spark, os.path.join(RAW_PATH, "weblogs"))
    transaction_df= load_json(spark, os.path.join(RAW_PATH, "transactions"))
    review_df     = load_json(spark, os.path.join(RAW_PATH, "reviews"))
    social_df     = load_json(spark, os.path.join(RAW_PATH, "social_media"))

    # ── Run analyses ───────────────────────────────────────────────────────
    wlog_results  = analyze_weblogs(weblog_df)
    txn_results   = analyze_transactions(transaction_df)
    rev_results   = analyze_reviews(review_df)
    soc_results   = analyze_social_media(social_df)
    customer_360  = build_customer_360(weblog_df, transaction_df, review_df, social_df)

    # ── Show sample results ────────────────────────────────────────────────
    print("\n" + "="*60)
    print("  TOP PAGES BY VIEWS")
    print("="*60)
    wlog_results["page_popularity"].show(5)

    print("\n" + "="*60)
    print("  REVENUE BY CATEGORY")
    print("="*60)
    txn_results["revenue_by_category"].show()

    print("\n" + "="*60)
    print("  REVIEW SENTIMENT SUMMARY")
    print("="*60)
    rev_results["sentiment_summary"].show()

    print("\n" + "="*60)
    print("  SOCIAL MEDIA PLATFORM STATS")
    print("="*60)
    soc_results["platform_stats"].show()

    print("\n" + "="*60)
    print("  CUSTOMER 360 VIEW (Top 10)")
    print("="*60)
    customer_360.show(10)

    # ── Save outputs ───────────────────────────────────────────────────────
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    save_results(wlog_results["page_popularity"],    "page_popularity",    "csv")
    save_results(txn_results["revenue_by_category"], "revenue_by_category","csv")
    save_results(txn_results["customer_spend"],      "customer_spend",     "parquet")
    save_results(rev_results["product_ratings"],     "product_ratings",    "csv")
    save_results(soc_results["platform_stats"],      "platform_stats",     "csv")
    save_results(customer_360,                       "customer_360",       "parquet")

    logger.info("✅ Full pipeline complete!")
    spark.stop()


def _generate_sample_data_if_needed(spark: SparkSession):
    """Create small sample JSON files so the pipeline runs without Kafka/HDFS."""
    import random, json
    from faker import Faker
    fake = Faker()

    paths = {
        "weblogs":     os.path.join(RAW_PATH, "weblogs"),
        "transactions":os.path.join(RAW_PATH, "transactions"),
        "reviews":     os.path.join(RAW_PATH, "reviews"),
        "social_media":os.path.join(RAW_PATH, "social_media"),
    }

    PAGES     = ["/home","/product","/cart","/checkout","/search"]
    PRODUCTS  = [{"id":"P001","name":"Laptop","price":1299.99,"category":"Electronics"},
                 {"id":"P002","name":"Shoes","price":89.99,"category":"Fashion"}]
    COUNTRIES = ["US","IN","UK","DE","AU"]

    for name, path in paths.items():
        os.makedirs(path, exist_ok=True)
        out_file = os.path.join(path, "data.json")
        if os.path.exists(out_file):
            continue   # already generated

        rows = []
        for _ in range(200):
            if name == "weblogs":
                rows.append({
                    "event_id": fake.uuid4(), "timestamp": "2024-01-15T10:00:00",
                    "session_id": fake.uuid4(), "customer_id": f"C{random.randint(1000,1050)}",
                    "page": random.choice(PAGES), "action": "click",
                    "device": random.choice(["desktop","mobile"]),
                    "country": random.choice(COUNTRIES), "duration_sec": random.randint(5,300),
                })
            elif name == "transactions":
                p = random.choice(PRODUCTS)
                qty = random.randint(1,3)
                rows.append({
                    "order_id": f"ORD{random.randint(1000,9999)}", "timestamp": "2024-01-15T11:00:00",
                    "customer_id": f"C{random.randint(1000,1050)}", "product_id": p["id"],
                    "product_name": p["name"], "category": p["category"],
                    "quantity": qty, "total_amount": round(p["price"]*qty,2),
                    "discount_pct": random.choice([0,5,10]),
                    "payment_method": "credit_card", "status": random.choice(["delivered","shipped"]),
                    "country": random.choice(COUNTRIES),
                })
            elif name == "reviews":
                rating = random.randint(1,5)
                rows.append({
                    "review_id": fake.uuid4(), "timestamp": "2024-01-15T12:00:00",
                    "customer_id": f"C{random.randint(1000,1050)}",
                    "product_id": random.choice(PRODUCTS)["id"],
                    "product_name": random.choice(PRODUCTS)["name"],
                    "rating": rating, "sentiment": "positive" if rating>=4 else ("negative" if rating<=2 else "neutral"),
                    "title": "Good product", "body": fake.sentence(),
                    "helpful_votes": random.randint(0,20), "has_image": False, "verified_purchase": True,
                })
            else:  # social
                rows.append({
                    "post_id": fake.uuid4(), "timestamp": "2024-01-15T13:00:00",
                    "platform": random.choice(["facebook","twitter","instagram"]),
                    "user_handle": fake.user_name(),
                    "customer_id": f"C{random.randint(1000,1050)}" if random.random()>0.3 else None,
                    "content": fake.sentence(nb_words=10), "hashtags": [],
                    "sentiment": random.choice(["positive","neutral","negative"]),
                    "likes": random.randint(0,500), "shares": random.randint(0,50),
                    "brand_mention": random.choice([True,False]), "country": random.choice(COUNTRIES),
                })

        with open(out_file, "w") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

        logger.info(f"  Generated {len(rows)} sample records → {out_file}")


if __name__ == "__main__":
    run_pipeline()
