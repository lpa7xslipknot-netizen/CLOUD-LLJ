"""
config.py — Utility to load and access configuration from config.yaml
"""

import yaml
import os
import logging

# ── Setup logging ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger(__name__)

# ── Default config path ────────────────────────────────────────────────────────
DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "configs", "config.yaml"
)


def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> dict:
    """
    Load the YAML configuration file.

    Args:
        config_path: Path to config.yaml

    Returns:
        dict: Parsed configuration dictionary
    """
    config_path = os.path.abspath(config_path)

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    logger.info(f"Configuration loaded from: {config_path}")
    return config


def get_kafka_config(config: dict) -> dict:
    """Return Kafka-specific config."""
    return config.get("kafka", {})


def get_hdfs_config(config: dict) -> dict:
    """Return HDFS-specific config."""
    return config.get("hdfs", {})


def get_spark_config(config: dict) -> dict:
    """Return Spark-specific config."""
    return config.get("spark", {})


def get_security_config(config: dict) -> dict:
    """Return security config."""
    return config.get("security", {})


# ── Convenience: load once and expose ─────────────────────────────────────────
try:
    CONFIG = load_config()
except FileNotFoundError:
    logger.warning("config.yaml not found — using empty config. Run from project root.")
    CONFIG = {}


if __name__ == "__main__":
    cfg = load_config()
    print("=== Kafka Topics ===")
    for k, v in cfg["kafka"]["topics"].items():
        print(f"  {k}: {v}")
    print("\n=== HDFS Paths ===")
    for k, v in cfg["hdfs"]["paths"].items():
        print(f"  {k}: {v}")
    print("\n=== Spark Settings ===")
    for k, v in cfg["spark"].items():
        print(f"  {k}: {v}")
