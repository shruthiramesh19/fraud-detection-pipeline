from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter()


@router.get("/alerts")
def list_alerts(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    risk_level: str | None = Query(None, pattern="^(high|medium|low)$"),
    detection_tier: str | None = Query(None, pattern="^(statistical|gpt)$"),
    db: Session = Depends(get_db),
):
    """
    List fraud alerts with filters.
    risk_level and detection_tier filters help analysts
    focus on the most actionable alerts first.
    """
    filters = []
    params: dict = {"skip": skip, "limit": limit}

    if risk_level:
        filters.append("gpt_risk_level = :risk_level")
        params["risk_level"] = risk_level

    if detection_tier:
        filters.append("detection_tier = :detection_tier")
        params["detection_tier"] = detection_tier

    where = "WHERE " + " AND ".join(filters) if filters else ""

    rows = db.execute(
        text(f"""
            SELECT id, alert_id, transaction_id, account_id,
                   confidence_score, detection_tier, anomaly_reasons,
                   gpt_is_fraud, gpt_reasoning, gpt_risk_level, created_at
            FROM alerts
            {where}
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :skip
        """),
        params,
    ).fetchall()

    total = db.execute(
        text(f"SELECT COUNT(*) FROM alerts {where}"),
        {k: v for k, v in params.items() if k not in ("skip", "limit")},
    ).scalar()

    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "alerts": [dict(r._mapping) for r in rows],
    }


@router.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    """
    Aggregate statistics for Grafana dashboard panels.
    Single endpoint returns all stats in one query round-trip
    rather than making Grafana hit multiple endpoints.
    """
    stats = db.execute(
        text("""
        SELECT
            (SELECT COUNT(*) FROM transactions) AS total_transactions,
            (SELECT COUNT(*) FROM alerts) AS total_alerts,
            (SELECT COUNT(*) FROM alerts WHERE detection_tier = 'gpt') AS gpt_escalations,
            (SELECT COUNT(*) FROM alerts WHERE gpt_is_fraud = true) AS confirmed_fraud,
            (SELECT ROUND(AVG(confidence_score)::numeric, 3)
             FROM alerts) AS avg_confidence,
            (SELECT COUNT(*) FROM transactions
             WHERE created_at > NOW() - INTERVAL '60 seconds') AS txns_last_60s,
            (SELECT COUNT(*) FROM alerts
             WHERE created_at > NOW() - INTERVAL '300 seconds') AS alerts_last_5min
    """)
    ).fetchone()

    return dict(stats._mapping)
