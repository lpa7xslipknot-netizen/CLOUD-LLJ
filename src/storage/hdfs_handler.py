"""
hdfs_handler.py — Handles reading and writing data to/from HDFS (or local
filesystem as fallback). Also includes AWS S3 upload support.

In production: uses hdfs Python client to talk to Hadoop NameNode.
In demo/dev:   falls back to local filesystem paths.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Try importing hdfs; fall back to local mode ────────────────────────────────
try:
    from hdfs import InsecureClient
    HDFS_AVAILABLE = True
except ImportError:
    HDFS_AVAILABLE = False
    logger.warning("hdfs package not available — running in LOCAL mode.")

# ── Try importing boto3 for S3 ─────────────────────────────────────────────────
try:
    import boto3
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False


# ══════════════════════════════════════════════════════════════════════════════
# HDFS Client
# ══════════════════════════════════════════════════════════════════════════════

class HDFSHandler:
    """
    Wrapper around the HDFS client.
    Falls back to local filesystem when HDFS is not available.
    """

    def __init__(self, namenode_url: str = "http://localhost:9870", user: str = "hadoop"):
        self.namenode_url = namenode_url
        self.user = user
        self.client = None
        self.local_mode = not HDFS_AVAILABLE

        if HDFS_AVAILABLE:
            try:
                self.client = InsecureClient(namenode_url, user=user)
                logger.info(f"Connected to HDFS: {namenode_url}")
            except Exception as e:
                logger.warning(f"HDFS connection failed ({e}) — switching to LOCAL mode.")
                self.local_mode = True
        else:
            logger.info("Running in LOCAL mode (no HDFS).")

    # ── Write ────────────────────────────────────────────────────────────────

    def write_json(self, data: list, hdfs_path: str, overwrite: bool = True):
        """
        Write a list of dicts as newline-delimited JSON to HDFS or local.

        Args:
            data: List of dict records
            hdfs_path: Target path (HDFS path or local relative path)
            overwrite: Overwrite existing file
        """
        content = "\n".join(json.dumps(record) for record in data)

        if self.local_mode:
            local_path = self._to_local_path(hdfs_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "w") as f:
                f.write(content)
            logger.info(f"[LOCAL] Written {len(data)} records → {local_path}")
        else:
            with self.client.write(hdfs_path, encoding="utf-8", overwrite=overwrite) as writer:
                writer.write(content)
            logger.info(f"[HDFS] Written {len(data)} records → {hdfs_path}")

    def write_parquet_via_spark(self, spark_df, hdfs_path: str):
        """Write a Spark DataFrame to HDFS as Parquet (handled by Spark itself)."""
        spark_df.write.mode("overwrite").parquet(hdfs_path)
        logger.info(f"[HDFS/Spark] Written Parquet → {hdfs_path}")

    # ── Read ─────────────────────────────────────────────────────────────────

    def read_json(self, hdfs_path: str) -> list:
        """
        Read newline-delimited JSON from HDFS or local.

        Returns:
            List of dict records
        """
        if self.local_mode:
            local_path = self._to_local_path(hdfs_path)
            if not os.path.exists(local_path):
                logger.warning(f"[LOCAL] File not found: {local_path}")
                return []
            with open(local_path, "r") as f:
                records = [json.loads(line) for line in f if line.strip()]
            logger.info(f"[LOCAL] Read {len(records)} records ← {local_path}")
            return records
        else:
            with self.client.read(hdfs_path, encoding="utf-8") as reader:
                records = [json.loads(line) for line in reader if line.strip()]
            logger.info(f"[HDFS] Read {len(records)} records ← {hdfs_path}")
            return records

    # ── Directory Ops ─────────────────────────────────────────────────────────

    def list_files(self, hdfs_path: str) -> list:
        """List files in an HDFS (or local) directory."""
        if self.local_mode:
            local_path = self._to_local_path(hdfs_path)
            if not os.path.exists(local_path):
                return []
            return os.listdir(local_path)
        else:
            return self.client.list(hdfs_path)

    def make_dirs(self, hdfs_path: str):
        """Create directory (and parents) in HDFS or local."""
        if self.local_mode:
            local_path = self._to_local_path(hdfs_path)
            os.makedirs(local_path, exist_ok=True)
            logger.info(f"[LOCAL] Created directory: {local_path}")
        else:
            self.client.makedirs(hdfs_path)
            logger.info(f"[HDFS] Created directory: {hdfs_path}")

    def delete(self, hdfs_path: str, recursive: bool = False):
        """Delete a file or directory."""
        if self.local_mode:
            import shutil
            local_path = self._to_local_path(hdfs_path)
            if os.path.isdir(local_path) and recursive:
                shutil.rmtree(local_path)
            elif os.path.exists(local_path):
                os.remove(local_path)
            logger.info(f"[LOCAL] Deleted: {local_path}")
        else:
            self.client.delete(hdfs_path, recursive=recursive)
            logger.info(f"[HDFS] Deleted: {hdfs_path}")

    # ── Utility ───────────────────────────────────────────────────────────────

    def _to_local_path(self, hdfs_path: str) -> str:
        """Convert an HDFS-style path to a local filesystem path."""
        # Strip hdfs://host:port prefix if present
        if hdfs_path.startswith("hdfs://"):
            hdfs_path = "/" + hdfs_path.split("/", 3)[-1]
        # Map to project data directory
        base = os.path.join(os.path.dirname(__file__), "..", "..", "data")
        return os.path.normpath(os.path.join(base, hdfs_path.lstrip("/")))

    def get_status(self) -> dict:
        """Return handler status info."""
        return {
            "mode": "LOCAL" if self.local_mode else "HDFS",
            "namenode": self.namenode_url,
            "hdfs_available": HDFS_AVAILABLE,
        }


# ══════════════════════════════════════════════════════════════════════════════
# Data Lake Zones
# ══════════════════════════════════════════════════════════════════════════════

class DataLakeManager:
    """
    Manages the two-zone data lake structure:

    raw/       ← All incoming data stored as-is
    processed/ ← Cleaned, transformed, aggregated data

    Follows naming: /<zone>/<source>/<YYYY-MM-DD>/data.json
    """

    ZONES = ["raw", "processed"]
    SOURCES = ["weblogs", "transactions", "reviews", "social_media"]

    def __init__(self, hdfs_handler: HDFSHandler):
        self.hdfs = hdfs_handler
        self._ensure_zones()

    def _ensure_zones(self):
        for zone in self.ZONES:
            for source in self.SOURCES:
                self.hdfs.make_dirs(f"/{zone}/{source}")

    def ingest_raw(self, source: str, records: list, date: str = None) -> str:
        """
        Write records to the RAW zone.

        Args:
            source: One of weblogs / transactions / reviews / social_media
            records: List of dict records
            date: Date partition (defaults to today)

        Returns:
            Path where data was written
        """
        if source not in self.SOURCES:
            raise ValueError(f"Unknown source: {source}. Must be one of {self.SOURCES}")
        date = date or datetime.utcnow().strftime("%Y-%m-%d")
        path = f"/raw/{source}/{date}/data.json"
        self.hdfs.write_json(records, path)
        logger.info(f"Ingested {len(records)} raw {source} records for {date}")
        return path

    def promote_to_processed(self, source: str, records: list, date: str = None) -> str:
        """Write cleaned/processed data to the PROCESSED zone."""
        date = date or datetime.utcnow().strftime("%Y-%m-%d")
        path = f"/processed/{source}/{date}/data.json"
        self.hdfs.write_json(records, path)
        logger.info(f"Promoted {len(records)} processed {source} records for {date}")
        return path

    def read_raw(self, source: str, date: str) -> list:
        """Read raw data for a given source and date."""
        path = f"/raw/{source}/{date}/data.json"
        return self.hdfs.read_json(path)

    def read_processed(self, source: str, date: str) -> list:
        """Read processed data for a given source and date."""
        path = f"/processed/{source}/{date}/data.json"
        return self.hdfs.read_json(path)

    def list_partitions(self, zone: str, source: str) -> list:
        """List all date partitions for a source in a zone."""
        return self.hdfs.list_files(f"/{zone}/{source}")


# ══════════════════════════════════════════════════════════════════════════════
# S3 Upload (optional)
# ══════════════════════════════════════════════════════════════════════════════

class S3Handler:
    """
    Optional: Upload processed results to AWS S3.
    Only works if boto3 is installed and AWS credentials are configured.
    """

    def __init__(self, bucket_name: str, region: str = "us-east-1"):
        if not S3_AVAILABLE:
            raise ImportError("boto3 is not installed. Run: pip install boto3")
        self.bucket = bucket_name
        self.s3 = boto3.client("s3", region_name=region)
        logger.info(f"S3Handler initialized for bucket: {bucket_name}")

    def upload_file(self, local_path: str, s3_key: str):
        """Upload a local file to S3."""
        self.s3.upload_file(local_path, self.bucket, s3_key)
        logger.info(f"Uploaded {local_path} → s3://{self.bucket}/{s3_key}")

    def download_file(self, s3_key: str, local_path: str):
        """Download a file from S3."""
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        self.s3.download_file(self.bucket, s3_key, local_path)
        logger.info(f"Downloaded s3://{self.bucket}/{s3_key} → {local_path}")

    def list_objects(self, prefix: str = "") -> list:
        """List objects in S3 bucket under a prefix."""
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]


# ══════════════════════════════════════════════════════════════════════════════
# Demo / Test
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    handler = HDFSHandler()
    print("Status:", handler.get_status())

    manager = DataLakeManager(handler)

    # Write sample records
    sample_transactions = [
        {"order_id": "ORD001", "customer_id": "C1001", "total_amount": 299.99, "status": "delivered"},
        {"order_id": "ORD002", "customer_id": "C1002", "total_amount": 149.50, "status": "shipped"},
    ]
    path = manager.ingest_raw("transactions", sample_transactions)
    print(f"Written to: {path}")

    # Read back
    records = manager.read_raw("transactions", datetime.utcnow().strftime("%Y-%m-%d"))
    print(f"Read back {len(records)} records:")
    for r in records:
        print(" ", r)

    # List partitions
    print("\nPartitions:", manager.list_partitions("raw", "transactions"))
