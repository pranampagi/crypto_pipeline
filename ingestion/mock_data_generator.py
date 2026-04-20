"""
ingestion/mock_data_generator.py
────────────────────────────────────────────────────────────────────────────────
Generates realistic synthetic CoinGecko API responses for local testing.
Use this when:
  - You don't have internet access
  - You hit CoinGecko rate limits
  - You want deterministic data for unit tests

Usage:
    python ingestion/mock_data_generator.py               # generate 5 batches
    python ingestion/mock_data_generator.py --batches 10  # generate 10 batches
    python ingestion/mock_data_generator.py --batches 1 --preview  # preview JSON
────────────────────────────────────────────────────────────────────────────────
"""

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.helpers import load_config, get_logger, get_partition_path, ensure_dir, utc_now_iso


# ──────────────────────────────────────────────
# Seed coin universe (realistic base prices)
# ──────────────────────────────────────────────

COIN_UNIVERSE = [
    {"id": "bitcoin",       "symbol": "btc",   "name": "Bitcoin",        "base_price": 65000.0,  "rank": 1},
    {"id": "ethereum",      "symbol": "eth",   "name": "Ethereum",       "base_price": 3200.0,   "rank": 2},
    {"id": "tether",        "symbol": "usdt",  "name": "Tether",         "base_price": 1.0,      "rank": 3},
    {"id": "binancecoin",   "symbol": "bnb",   "name": "BNB",            "base_price": 580.0,    "rank": 4},
    {"id": "solana",        "symbol": "sol",   "name": "Solana",         "base_price": 155.0,    "rank": 5},
    {"id": "ripple",        "symbol": "xrp",   "name": "XRP",            "base_price": 0.55,     "rank": 6},
    {"id": "usd-coin",      "symbol": "usdc",  "name": "USD Coin",       "base_price": 1.0,      "rank": 7},
    {"id": "staked-ether",  "symbol": "steth", "name": "Lido Staked ETH","base_price": 3190.0,   "rank": 8},
    {"id": "dogecoin",      "symbol": "doge",  "name": "Dogecoin",       "base_price": 0.145,    "rank": 9},
    {"id": "cardano",       "symbol": "ada",   "name": "Cardano",        "base_price": 0.44,     "rank": 10},
    {"id": "avalanche-2",   "symbol": "avax",  "name": "Avalanche",      "base_price": 36.0,     "rank": 11},
    {"id": "chainlink",     "symbol": "link",  "name": "Chainlink",      "base_price": 14.5,     "rank": 12},
    {"id": "polkadot",      "symbol": "dot",   "name": "Polkadot",       "base_price": 7.2,      "rank": 13},
    {"id": "tron",          "symbol": "trx",   "name": "TRON",           "base_price": 0.12,     "rank": 14},
    {"id": "polygon",       "symbol": "matic", "name": "Polygon",        "base_price": 0.85,     "rank": 15},
    {"id": "litecoin",      "symbol": "ltc",   "name": "Litecoin",       "base_price": 82.0,     "rank": 16},
    {"id": "shiba-inu",     "symbol": "shib",  "name": "Shiba Inu",      "base_price": 0.000025, "rank": 17},
    {"id": "dai",           "symbol": "dai",   "name": "Dai",            "base_price": 1.0,      "rank": 18},
    {"id": "uniswap",       "symbol": "uni",   "name": "Uniswap",        "base_price": 8.9,      "rank": 19},
    {"id": "cosmos",        "symbol": "atom",  "name": "Cosmos Hub",     "base_price": 9.4,      "rank": 20},
    {"id": "stellar",       "symbol": "xlm",   "name": "Stellar",        "base_price": 0.12,     "rank": 21},
    {"id": "ethereum-classic","symbol":"etc",  "name": "Ethereum Classic","base_price": 26.0,    "rank": 22},
    {"id": "monero",        "symbol": "xmr",   "name": "Monero",         "base_price": 165.0,    "rank": 23},
    {"id": "filecoin",      "symbol": "fil",   "name": "Filecoin",       "base_price": 5.6,      "rank": 24},
    {"id": "vechain",       "symbol": "vet",   "name": "VeChain",        "base_price": 0.038,    "rank": 25},
    {"id": "near",          "symbol": "near",  "name": "NEAR Protocol",  "base_price": 6.8,      "rank": 26},
    {"id": "algorand",      "symbol": "algo",  "name": "Algorand",       "base_price": 0.18,     "rank": 27},
    {"id": "hedera",        "symbol": "hbar",  "name": "Hedera",         "base_price": 0.095,    "rank": 28},
    {"id": "quant-network", "symbol": "qnt",   "name": "Quant",          "base_price": 105.0,    "rank": 29},
    {"id": "fantom",        "symbol": "ftm",   "name": "Fantom",         "base_price": 0.68,     "rank": 30},
    {"id": "aave",          "symbol": "aave",  "name": "Aave",           "base_price": 96.0,     "rank": 31},
    {"id": "the-graph",     "symbol": "grt",   "name": "The Graph",      "base_price": 0.18,     "rank": 32},
    {"id": "elrond-erd-2",  "symbol": "egld",  "name": "MultiversX",     "base_price": 42.0,     "rank": 33},
    {"id": "eos",           "symbol": "eos",   "name": "EOS",            "base_price": 0.78,     "rank": 34},
    {"id": "tezos",         "symbol": "xtz",   "name": "Tezos",          "base_price": 1.02,     "rank": 35},
    {"id": "maker",         "symbol": "mkr",   "name": "Maker",          "base_price": 2800.0,   "rank": 36},
    {"id": "theta-token",   "symbol": "theta", "name": "Theta Network",  "base_price": 1.35,     "rank": 37},
    {"id": "neo",           "symbol": "neo",   "name": "NEO",            "base_price": 13.5,     "rank": 38},
    {"id": "klaytn",        "symbol": "klay",  "name": "Klaytn",         "base_price": 0.19,     "rank": 39},
    {"id": "flow",          "symbol": "flow",  "name": "Flow",           "base_price": 0.82,     "rank": 40},
    {"id": "decentraland",  "symbol": "mana",  "name": "Decentraland",   "base_price": 0.41,     "rank": 41},
    {"id": "axie-infinity",  "symbol": "axs",  "name": "Axie Infinity",  "base_price": 7.4,      "rank": 42},
    {"id": "pancakeswap-token","symbol":"cake", "name": "PancakeSwap",   "base_price": 2.6,      "rank": 43},
    {"id": "chiliz",        "symbol": "chz",   "name": "Chiliz",         "base_price": 0.096,    "rank": 44},
    {"id": "gala",          "symbol": "gala",  "name": "Gala",           "base_price": 0.032,    "rank": 45},
    {"id": "curve-dao-token","symbol":"crv",   "name": "Curve DAO Token","base_price": 0.52,     "rank": 46},
    {"id": "zcash",         "symbol": "zec",   "name": "Zcash",          "base_price": 22.0,     "rank": 47},
    {"id": "compound-governance-token","symbol":"comp","name":"Compound","base_price": 48.0,     "rank": 48},
    {"id": "sushi",         "symbol": "sushi", "name": "SushiSwap",      "base_price": 1.1,      "rank": 49},
    {"id": "1inch",         "symbol": "1inch", "name": "1inch Network",  "base_price": 0.38,     "rank": 50},
]


# ──────────────────────────────────────────────
# Synthetic Record Generator
# ──────────────────────────────────────────────

def _fluctuate(base: float, pct_range: float = 0.05) -> float:
    """Apply a small random fluctuation to a base price."""
    delta = base * random.uniform(-pct_range, pct_range)
    return round(base + delta, 8)


def generate_coin_record(coin: dict, ts: datetime) -> dict:
    """
    Generate one realistic synthetic coin market record.
    Prices drift slightly from the base to simulate real movement.
    """
    rng = random.Random(coin["id"] + ts.strftime("%Y%m%d%H"))  # reproducible per hour

    price = _fluctuate(coin["base_price"], pct_range=0.03)
    high  = price * rng.uniform(1.001, 1.08)
    low   = price * rng.uniform(0.92,  0.999)
    price_change_24h     = price - coin["base_price"]
    price_change_pct_24h = round((price_change_24h / coin["base_price"]) * 100, 4)
    price_change_pct_7d  = round(rng.uniform(-15.0, 20.0), 4)

    market_cap           = round(price * rng.uniform(1e7, 2e10), 0)
    total_volume         = round(market_cap * rng.uniform(0.02, 0.35), 0)
    circ_supply          = round(market_cap / max(price, 1e-8), 0)

    ath          = coin["base_price"] * rng.uniform(1.5, 5.0)
    atl          = coin["base_price"] * rng.uniform(0.01, 0.3)
    ath_change   = round(((price - ath) / ath) * 100, 4)
    atl_change   = round(((price - atl) / atl) * 100, 4)

    ath_date = (ts - timedelta(days=rng.randint(100, 900))).isoformat()
    atl_date = (ts - timedelta(days=rng.randint(900, 2000))).isoformat()

    return {
        "id":                                    coin["id"],
        "symbol":                                coin["symbol"],
        "name":                                  coin["name"],
        "image":                                 f"https://assets.coingecko.com/coins/images/{coin['rank']}/large/logo.png",
        "current_price":                         round(price, 8),
        "market_cap":                            market_cap,
        "market_cap_rank":                       coin["rank"],
        "fully_diluted_valuation":               round(market_cap * 1.15, 0),
        "total_volume":                          total_volume,
        "high_24h":                              round(high, 8),
        "low_24h":                               round(low, 8),
        "price_change_24h":                      round(price_change_24h, 8),
        "price_change_percentage_24h":           price_change_pct_24h,
        "price_change_percentage_7d_in_currency": price_change_pct_7d,
        "market_cap_change_24h":                 round(market_cap * (price_change_pct_24h / 100), 0),
        "market_cap_change_percentage_24h":      price_change_pct_24h,
        "circulating_supply":                    circ_supply,
        "total_supply":                          round(circ_supply * 1.1, 0),
        "max_supply":                            round(circ_supply * 1.25, 0) if rng.random() > 0.3 else None,
        "ath":                                   round(ath, 8),
        "ath_change_percentage":                 ath_change,
        "ath_date":                              ath_date,
        "atl":                                   round(atl, 8),
        "atl_change_percentage":                 atl_change,
        "atl_date":                              atl_date,
        "last_updated":                          ts.isoformat(),
    }


def generate_batch(ts: datetime = None, n_coins: int = None) -> list:
    """Generate one full batch of coin records (default: all 50 coins)."""
    ts = ts or datetime.now(timezone.utc)
    coins = COIN_UNIVERSE[:n_coins] if n_coins else COIN_UNIVERSE
    return [generate_coin_record(c, ts) for c in coins]


# ──────────────────────────────────────────────
# Bronze Writer (reused from poller)
# ──────────────────────────────────────────────

def write_mock_bronze(raw_data: list, bronze_base: str, logger) -> str:
    """Write synthetic batch to Bronze zone."""
    from datetime import timezone
    ts = datetime.now(timezone.utc)
    partition_path = get_partition_path(bronze_base, ts)
    ensure_dir(partition_path)

    ingestion_id = str(uuid.uuid4())
    envelope = {
        "ingestion_id": ingestion_id,
        "source": "mock_coingecko_generator",
        "ingested_at": utc_now_iso(),
        "record_count": len(raw_data),
        "data": raw_data
    }

    filename = f"mock_markets_{ts.strftime('%Y%m%d_%H%M%S')}_{ingestion_id[:8]}.json"
    filepath = os.path.join(partition_path, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=2)

    logger.info(f"Mock Bronze written → {filepath} ({len(raw_data)} records)")
    return filepath


# ──────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Crypto Mock Data Generator")
    parser.add_argument("--batches",  type=int, default=5,   help="Number of batches to generate (default: 5)")
    parser.add_argument("--interval", type=int, default=2,   help="Seconds between batches (default: 2)")
    parser.add_argument("--coins",    type=int, default=None, help="Number of coins per batch (default: all 50)")
    parser.add_argument("--preview",  action="store_true",   help="Print first record to stdout and exit")
    args = parser.parse_args()

    config  = load_config()
    logger  = get_logger("MockDataGenerator", config)
    bronze  = config["storage"]["bronze_path"]
    ensure_dir(bronze)

    if args.preview:
        batch = generate_batch()
        print(json.dumps(batch[0], indent=2))
        return

    logger.info(f"Generating {args.batches} mock batch(es) → {bronze}")
    for i in range(1, args.batches + 1):
        logger.info(f"── Batch {i}/{args.batches} ──")
        batch = generate_batch(n_coins=args.coins)
        write_mock_bronze(batch, bronze, logger)
        if i < args.batches:
            time.sleep(args.interval)

    logger.info(f"✅ Done. {args.batches} mock batches written to: {bronze}")


if __name__ == "__main__":
    main()
