import random
from datetime import datetime, timezone
from typing import Any

# ── Account profiles ──────────────────────────────────────────────
# Each account has a home location, typical spend range, and
# preferred merchant categories. This creates realistic baselines
# so Z-score detection has meaningful signal to work with.

ACCOUNTS = {
    f"ACC-{i:04d}": {
        "home_location": loc,
        "typical_amount_range": amt_range,
        "preferred_categories": cats,
        "name": name,
    }
    for i, (loc, amt_range, cats, name) in enumerate(
        [
            (
                "New York, US",
                (10, 200),
                ["grocery", "restaurant", "transport"],
                "Alice Johnson",
            ),
            (
                "Los Angeles, US",
                (20, 500),
                ["electronics", "restaurant", "retail"],
                "Bob Smith",
            ),
            (
                "Chicago, US",
                (5, 150),
                ["grocery", "pharmacy", "transport"],
                "Carol White",
            ),
            ("Houston, US", (15, 300), ["gas", "grocery", "restaurant"], "David Brown"),
            (
                "Phoenix, US",
                (10, 250),
                ["retail", "grocery", "restaurant"],
                "Eve Davis",
            ),
            (
                "Philadelphia, US",
                (8, 180),
                ["transport", "grocery", "pharmacy"],
                "Frank Miller",
            ),
            (
                "San Antonio, US",
                (12, 220),
                ["restaurant", "retail", "grocery"],
                "Grace Wilson",
            ),
            (
                "San Diego, US",
                (25, 450),
                ["restaurant", "retail", "electronics"],
                "Henry Moore",
            ),
            ("Dallas, US", (10, 280), ["grocery", "gas", "retail"], "Iris Taylor"),
            (
                "San Jose, US",
                (30, 600),
                ["electronics", "restaurant", "retail"],
                "Jack Anderson",
            ),
        ],
        start=0,
    )
}

MERCHANTS = {
    "grocery": ["Whole Foods", "Kroger", "Safeway", "Trader Joe's", "Walmart"],
    "restaurant": ["McDonald's", "Chipotle", "Olive Garden", "Subway", "Starbucks"],
    "electronics": ["Best Buy", "Apple Store", "Micro Center", "B&H Photo"],
    "retail": ["Target", "Amazon", "Macy's", "TJ Maxx", "Nordstrom"],
    "transport": ["Uber", "Lyft", "Metro Card", "Delta Airlines", "Amtrak"],
    "pharmacy": ["CVS", "Walgreens", "Rite Aid"],
    "gas": ["Shell", "ExxonMobil", "BP", "Chevron"],
    "luxury": ["Rolex", "Louis Vuitton", "Tiffany & Co", "Gucci", "Prada"],
    "crypto": ["Coinbase", "Binance", "Kraken"],
}

FOREIGN_LOCATIONS = [
    "London, UK",
    "Tokyo, Japan",
    "Lagos, Nigeria",
    "Moscow, Russia",
    "Beijing, China",
    "Dubai, UAE",
    "São Paulo, Brazil",
    "Mumbai, India",
]


def normal_transaction(account_id: str, ts: datetime) -> dict[str, Any]:
    """Generate a realistic normal transaction for an account."""
    profile = ACCOUNTS[account_id]
    category = random.choice(profile["preferred_categories"])
    merchant = random.choice(MERCHANTS[category])
    low, high = profile["typical_amount_range"]

    return {
        "account_id": account_id,
        "amount": round(random.uniform(low, high), 2),
        "merchant": merchant,
        "merchant_category": category,
        "location": profile["home_location"],
        "transaction_type": "purchase",
        "timestamp": ts.isoformat(),
        "is_simulated_fraud": False,
    }


# ── Fraud patterns ────────────────────────────────────────────────
# Each returns a transaction dict with is_simulated_fraud=True.
# Multiple patterns exist to test different detection rules.


def large_amount_fraud(account_id: str, ts: datetime) -> dict[str, Any]:
    """
    Amount 8-15x the account's typical maximum.
    Triggers: Z-score anomaly + amount threshold rule.
    """
    profile = ACCOUNTS[account_id]
    _, high = profile["typical_amount_range"]
    return {
        "account_id": account_id,
        "amount": round(random.uniform(high * 8, high * 15), 2),
        "merchant": random.choice(MERCHANTS["electronics"]),
        "merchant_category": "electronics",
        "location": profile["home_location"],
        "transaction_type": "purchase",
        "timestamp": ts.isoformat(),
        "is_simulated_fraud": True,
    }


def foreign_location_fraud(account_id: str, ts: datetime) -> dict[str, Any]:
    """
    Transaction from a foreign location outside account's home country.
    Triggers: location anomaly rule.
    """
    profile = ACCOUNTS[account_id]
    _, high = profile["typical_amount_range"]
    return {
        "account_id": account_id,
        "amount": round(random.uniform(high * 2, high * 6), 2),
        "merchant": random.choice(MERCHANTS["luxury"]),
        "merchant_category": "luxury",
        "location": random.choice(FOREIGN_LOCATIONS),
        "transaction_type": "purchase",
        "timestamp": ts.isoformat(),
        "is_simulated_fraud": True,
    }


def late_night_fraud(account_id: str, ts: datetime) -> dict[str, Any]:
    """
    High-value purchase between 2AM-4AM local time.
    Triggers: off-hours large transaction rule.
    """
    profile = ACCOUNTS[account_id]
    _, high = profile["typical_amount_range"]
    # Force hour to 2-4 AM
    late_ts = ts.replace(hour=random.randint(2, 4))
    return {
        "account_id": account_id,
        "amount": round(random.uniform(high * 3, high * 8), 2),
        "merchant": random.choice(MERCHANTS["luxury"]),
        "merchant_category": "luxury",
        "location": profile["home_location"],
        "transaction_type": "purchase",
        "timestamp": late_ts.isoformat(),
        "is_simulated_fraud": True,
    }


def category_mismatch_fraud(account_id: str, ts: datetime) -> dict[str, Any]:
    """
    Luxury or crypto purchase from account that only does groceries.
    Triggers: category anomaly + amount anomaly.
    """
    profile = ACCOUNTS[account_id]
    _, high = profile["typical_amount_range"]
    fraud_category = random.choice(["luxury", "crypto"])
    return {
        "account_id": account_id,
        "amount": round(random.uniform(high * 4, high * 10), 2),
        "merchant": random.choice(MERCHANTS[fraud_category]),
        "merchant_category": fraud_category,
        "location": profile["home_location"],
        "transaction_type": "purchase",
        "timestamp": ts.isoformat(),
        "is_simulated_fraud": True,
    }


FRAUD_PATTERNS = [
    large_amount_fraud,
    foreign_location_fraud,
    late_night_fraud,
    category_mismatch_fraud,
]


def generate_transaction(account_id: str) -> dict[str, Any]:
    """
    Main entry point. 15% chance of fraud, 85% normal.
    Randomly selects fraud pattern when fraud is triggered.
    """
    ts = datetime.now(timezone.utc)
    if random.random() < 0.15:
        pattern = random.choice(FRAUD_PATTERNS)
        return pattern(account_id, ts)
    return normal_transaction(account_id, ts)
