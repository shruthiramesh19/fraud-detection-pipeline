import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

# ── Per-account rolling window ────────────────────────────────────
# Stores the last N transaction amounts per account in memory.
# deque with maxlen automatically drops oldest entries —
# no manual cleanup needed. This is an O(1) append and O(n) mean/std.
#
# Trade-off: this state lives in memory only. If the processor
# restarts, the windows reset and Z-scores won't be meaningful
# until enough history accumulates again (~50 transactions).
# Production fix: bootstrap from PostgreSQL on startup.

WINDOW_SIZE = 50
_account_windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

# Velocity tracking: timestamps of recent transactions per account
# Used to detect bursts (many transactions in a short time window)
VELOCITY_WINDOW_SECONDS = 60
_account_timestamps: dict[str, deque] = defaultdict(lambda: deque(maxlen=100))

# Detection thresholds
ZSCORE_THRESHOLD = 3.0  # flag if amount is 3+ std devs from mean
VELOCITY_THRESHOLD = 15  # flag if >15 txns in 60 seconds
LARGE_AMOUNT_USD = 5000.0  # absolute large amount threshold
LARGE_AMOUNT_MEAN_MULTIPLIER = 10  # flag if amount > 10x account mean
OFF_HOURS = {2, 3, 4}  # 2AM-4AM UTC
OFF_HOURS_MULTIPLIER = 2.0  # flag if amount > 2x mean during off hours

# Confidence weights — must sum to 1.0
WEIGHTS = {
    "zscore": 0.45,
    "velocity": 0.20,
    "large": 0.25,
    "off_hours": 0.10,
}


def _mean(values: deque) -> float:
    return sum(values) / len(values) if values else 0.0


def _std(values: deque) -> float:
    if len(values) < 2:
        return 0.0
    m = _mean(values)
    variance = sum((x - m) ** 2 for x in values) / len(values)
    return variance**0.5


def _zscore(amount: float, window: deque) -> float:
    """
    Z-score = (value - mean) / std_dev
    Measures how many standard deviations a value is from the mean.
    Z > 3.0 means the value is in the outermost 0.3% of a normal
    distribution — statistically very unusual.
    """
    if len(window) < 5:
        # Not enough history for reliable Z-score
        return 0.0
    std = _std(window)
    if std == 0:
        return 0.0
    return (amount - _mean(window)) / std


def _velocity(account_id: str, ts: datetime) -> int:
    """Count transactions for this account in the last 60 seconds."""
    timestamps = _account_timestamps[account_id]
    cutoff = ts.timestamp() - VELOCITY_WINDOW_SECONDS
    return sum(1 for t in timestamps if t > cutoff)


def analyze(txn: dict[str, Any]) -> dict[str, Any]:
    """
    Run all detection rules against a transaction.
    Returns a result dict with confidence score and triggered reasons.

    The confidence score is a weighted sum of triggered rules.
    This is simpler than a trained ML model but fully explainable —
    you can always tell a customer exactly why a transaction was flagged.
    Trade-off: static weights require manual tuning; a trained model
    would learn optimal weights from labeled data automatically.
    """
    account_id = txn["account_id"]
    amount = float(txn["amount"])
    window = _account_windows[account_id]

    # Parse timestamp
    try:
        ts = datetime.fromisoformat(txn["timestamp"].replace("Z", "+00:00"))
    except Exception:
        ts = datetime.now(timezone.utc)

    triggered = {}  # rule_name → True if fired
    reasons = []  # human-readable explanations

    # ── Rule 1: Z-score ───────────────────────────────────────────
    z = _zscore(amount, window)
    if z > ZSCORE_THRESHOLD:
        triggered["zscore"] = True
        reasons.append(
            f"Amount ${amount:.2f} is {z:.1f} standard deviations above "
            f"account mean ${_mean(window):.2f}"
        )
        log.debug(f"[{account_id}] Z-score={z:.2f} (threshold={ZSCORE_THRESHOLD})")

    # ── Rule 2: Velocity ──────────────────────────────────────────
    recent_count = _velocity(account_id, ts)
    if recent_count > VELOCITY_THRESHOLD:
        triggered["velocity"] = True
        reasons.append(
            f"Velocity: {recent_count} transactions in last {VELOCITY_WINDOW_SECONDS}s "
            f"(threshold: {VELOCITY_THRESHOLD})"
        )

    # ── Rule 3: Absolute large amount ────────────────────────────
    mean_amount = _mean(window)
    if amount > LARGE_AMOUNT_USD and (
        mean_amount == 0 or amount > mean_amount * LARGE_AMOUNT_MEAN_MULTIPLIER
    ):
        triggered["large"] = True
        reasons.append(
            f"Large amount: ${amount:.2f} exceeds ${LARGE_AMOUNT_USD:.0f} "
            f"and is {amount / mean_amount:.1f}x account mean"
            if mean_amount > 0
            else f"Large amount: ${amount:.2f} exceeds ${LARGE_AMOUNT_USD:.0f}"
        )

    # ── Rule 4: Off-hours large transaction ──────────────────────
    if (
        ts.hour in OFF_HOURS
        and mean_amount > 0
        and amount > mean_amount * OFF_HOURS_MULTIPLIER
    ):
        triggered["off_hours"] = True
        reasons.append(
            f"Off-hours transaction: ${amount:.2f} at {ts.hour:02d}:00 UTC "
            f"is {amount / mean_amount:.1f}x account mean"
        )

    # ── Confidence score ──────────────────────────────────────────
    confidence = sum(WEIGHTS[rule] for rule in triggered)
    confidence = min(confidence, 1.0)  # cap at 1.0

    # ── Update rolling windows ────────────────────────────────────
    # Always update AFTER scoring — we don't want this transaction
    # to influence its own Z-score calculation
    window.append(amount)
    _account_timestamps[account_id].append(ts.timestamp())

    return {
        "is_anomaly": confidence >= 0.35,
        "confidence": round(confidence, 4),
        "reasons": reasons,
        "zscore": round(z, 3),
        "account_mean": round(mean_amount, 2),
        "window_size": len(window),
    }
