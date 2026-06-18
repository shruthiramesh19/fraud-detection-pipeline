from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter()


@router.get("/transactions")
def list_transactions(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    account_id: str | None = Query(None),
    is_fraud: bool | None = Query(None),
    db: Session = Depends(get_db),
):
    """
    List transactions with optional filters.
    Supports pagination via skip/limit.
    account_id filter enables per-account investigation.
    is_fraud filter lets you compare flagged vs normal transactions.
    """
    filters = []
    params: dict = {"skip": skip, "limit": limit}

    if account_id:
        filters.append("account_id = :account_id")
        params["account_id"] = account_id

    if is_fraud is not None:
        filters.append("is_simulated_fraud = :is_fraud")
        params["is_fraud"] = is_fraud

    where = "WHERE " + " AND ".join(filters) if filters else ""

    rows = db.execute(
        text(f"""
            SELECT id, transaction_id, account_id, amount, merchant,
                   merchant_category, location, transaction_type,
                   is_simulated_fraud, timestamp, created_at
            FROM transactions
            {where}
            ORDER BY timestamp DESC
            LIMIT :limit OFFSET :skip
        """),
        params,
    ).fetchall()

    total = db.execute(
        text(f"SELECT COUNT(*) FROM transactions {where}"),
        {k: v for k, v in params.items() if k not in ("skip", "limit")},
    ).scalar()

    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "transactions": [dict(r._mapping) for r in rows],
    }


@router.get("/transactions/{transaction_id}")
def get_transaction(transaction_id: str, db: Session = Depends(get_db)):
    """Get a single transaction with its associated alert if any."""
    row = db.execute(
        text("""
            SELECT t.*, a.confidence_score, a.detection_tier,
                   a.anomaly_reasons, a.gpt_is_fraud,
                   a.gpt_reasoning, a.gpt_risk_level
            FROM transactions t
            LEFT JOIN alerts a ON a.transaction_id = t.transaction_id
            WHERE t.transaction_id = :txn_id
        """),
        {"txn_id": transaction_id},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Transaction not found")

    return dict(row._mapping)
