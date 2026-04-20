"""
ingestion/coingecko_poller.py
────────────────────────────────────────────────────────────────────────────────
Continuously polls the CoinGecko /coins/markets endpoint and writes raw JSON
responses to the Bronze landing zone with Hive-style date partitioning.

Run locally:
    python ingestion/coingecko_poller.py

Run once (for testing):
    python ingestion/coingecko_poller.py --once
────────────────────────────────────────────────────────────────────────────────
"""

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.helpers import load_config, get_logger, get_partition_path, ensure_dir, utc_now_iso


# ──────────────────────────────────────────────
# CoinGecko Client
# ──────────────────────────────────────────────

class CoinGeckoClient:
    """Thin wrapper around the CoinGecko public REST API."""

    def __init__(self, config: dict, logger):
        self.base_url = config["api"]["base_url"]
        self.endpoint = config["api"]["endpoints"]["markets"]
        self.params = dict(config["api"]["params"])
        self.max_retries = config["api"]["max_retries"]
        self.backoff = config["api"]["retry_backoff_seconds"]
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "CryptoPipeline/1.0"
        })

    def fetch_markets(self) -> list[dict]:
        """
        Fetch live market data with retry logic.
        Returns list of coin market dicts.
        """
        url = f"{self.base_url}{self.endpoint}"
        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Fetching markets (attempt {attempt}/{self.max_retries}) ...")
                resp = self.session.get(url, params=self.params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                self.logger.info(f"Fetched {len(data)} coins successfully.")
                return data

            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response else "?"
                if status == 429:
                    wait = self.backoff * attempt * 2   # exponential on rate limit
                    self.logger.warning(f"Rate limited (429). Waiting {wait}s ...")
                    time.sleep(wait)
                else:
                    self.logger.error(f"HTTP error {status}: {e}")
                last_error = e

            except requests.exceptions.ConnectionError as e:
                self.logger.warning(f"Connection error: {e}. Retrying in {self.backoff}s ...")
                time.sleep(self.backoff)
                last_error = e

            except requests.exceptions.Timeout as e:
                self.logger.warning(f"Request timed out. Retrying in {self.backoff}s ...")
                time.sleep(self.backoff)
                last_error = e

        self.logger.error(f"All {self.max_retries} attempts failed. Last error: {last_error}")
        raise RuntimeError(f"CoinGecko fetch failed after {self.max_retries} retries.") from last_error


# ──────────────────────────────────────────────
# Bronze Writer
# ──────────────────────────────────────────────

class BronzeWriter:
    """Writes raw API payloads to the Bronze landing zone as JSON files."""

    def __init__(self, bronze_base: str, logger):
        self.bronze_base = bronze_base
        self.logger = logger

    def write(self, raw_data: list[dict]) -> str:
        """
        Wrap raw list in an envelope with metadata and write to partitioned path.
        Returns the full file path written.
        """
        ts = datetime.now(timezone.utc)
        partition_path = get_partition_path(self.bronze_base, ts)
        ensure_dir(partition_path)

        envelope = {
            "ingestion_id": str(uuid.uuid4()),
            "source": "coingecko_markets_api",
            "ingested_at": utc_now_iso(),
            "record_count": len(raw_data),
            "api_params": {
                "vs_currency": "usd",
                "per_page": len(raw_data)
            },
            "data": raw_data
        }

        filename = f"markets_{ts.strftime('%Y%m%d_%H%M%S')}_{envelope['ingestion_id'][:8]}.json"
        filepath = os.path.join(partition_path, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(envelope, f, ensure_ascii=False, indent=2)

        self.logger.info(f"Bronze file written → {filepath} ({len(raw_data)} records)")
        return filepath


# ──────────────────────────────────────────────
# Poller Orchestrator
# ──────────────────────────────────────────────

class CoinGeckoPoller:
    """Orchestrates continuous polling: fetch → write bronze → sleep → repeat."""

    def __init__(self, config: dict):
        self.config = config
        self.logger = get_logger("CoinGeckoPoller", config)
        self.client = CoinGeckoClient(config, self.logger)
        self.writer = BronzeWriter(config["storage"]["bronze_path"], self.logger)
        self.poll_interval = config["api"]["poll_interval_seconds"]

    def run_once(self) -> str:
        """Single fetch-and-write cycle. Returns written file path."""
        raw_data = self.client.fetch_markets()
        return self.writer.write(raw_data)

    def run_continuous(self):
        """Infinite polling loop. Ctrl+C to stop."""
        self.logger.info(
            f"Starting continuous polling every {self.poll_interval}s. "
            f"Bronze path: {self.config['storage']['bronze_path']}"
        )
        cycle = 0
        while True:
            cycle += 1
            self.logger.info(f"──── Poll Cycle #{cycle} ────")
            try:
                filepath = self.run_once()
                self.logger.info(f"Cycle #{cycle} complete. Next poll in {self.poll_interval}s.")
            except RuntimeError as e:
                self.logger.error(f"Cycle #{cycle} failed: {e}. Continuing after sleep ...")
            except KeyboardInterrupt:
                self.logger.info("Polling stopped by user (KeyboardInterrupt).")
                break

            time.sleep(self.poll_interval)


# ──────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CoinGecko Market Data Poller")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single fetch cycle and exit (useful for testing)."
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to pipeline_config.yaml (default: auto-detected)."
    )
    args = parser.parse_args()

    config = load_config(args.config)
    poller = CoinGeckoPoller(config)

    if args.once:
        path = poller.run_once()
        print(f"\n✅ Single run complete. File written: {path}")
    else:
        poller.run_continuous()


if __name__ == "__main__":
    main()
