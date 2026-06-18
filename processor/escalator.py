import logging
import os
from typing import Any

log = logging.getLogger(__name__)

GPT_ESCALATION_ENABLED = os.getenv("GPT_ESCALATION_ENABLED", "true").lower() == "true"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
LLM_MODEL = "gpt-4o-mini"
MAX_TOKENS = 500


def escalate_to_gpt(
    txn: dict[str, Any], detection_result: dict[str, Any]
) -> dict[str, Any]:
    """
    Send high-confidence anomalies to GPT-4o-mini for deeper analysis.

    Why GPT here instead of a rule?
    Statistical rules catch what we explicitly program. GPT can reason
    about combinations of signals that are hard to express as rules —
    e.g. 'a $3000 purchase at a luxury store is suspicious for this
    account but the Z-score is only 2.8, just below threshold.'

    Cost control: this function is only called when confidence >= 0.75,
    which means roughly the top 10% of flagged transactions.
    Each call costs ~$0.0003 at gpt-4o-mini pricing.
    """
    if not GPT_ESCALATION_ENABLED:
        log.info("GPT escalation disabled — skipping")
        return _disabled_result()

    if not OPENAI_API_KEY:
        log.warning("OPENAI_API_KEY not set — skipping GPT escalation")
        return _disabled_result()

    try:
        # Import here to avoid hard dependency if escalation is disabled
        from openai import OpenAI

        client = OpenAI(api_key=OPENAI_API_KEY)

        prompt = _build_prompt(txn, detection_result)

        response = client.chat.completions.create(
            model=LLM_MODEL,
            max_tokens=MAX_TOKENS,
            temperature=0.0,  # deterministic — fraud decisions should be consistent
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a fraud detection analyst. Analyze the transaction and "
                        "statistical anomaly signals provided. Respond ONLY with a JSON "
                        "object containing exactly these fields: "
                        "is_fraud (boolean), risk_level (string: high/medium/low), "
                        "reasoning (string, max 2 sentences explaining your decision)."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        )

        import json

        result = json.loads(response.choices[0].message.content)

        return {
            "gpt_is_fraud": bool(result.get("is_fraud", False)),
            "gpt_risk_level": str(result.get("risk_level", "low")),
            "gpt_reasoning": str(result.get("reasoning", "")),
            "gpt_escalated": True,
        }

    except Exception as e:
        log.error(f"GPT escalation failed: {e}")
        return _error_result(str(e))


def _build_prompt(txn: dict[str, Any], detection: dict[str, Any]) -> str:
    return f"""
Transaction Details:
- Account: {txn["account_id"]}
- Amount: ${float(txn["amount"]):.2f}
- Merchant: {txn["merchant"]} ({txn["merchant_category"]})
- Location: {txn["location"]}
- Time: {txn["timestamp"]}
- Type: {txn["transaction_type"]}

Statistical Analysis:
- Confidence Score: {detection["confidence"]:.2f} / 1.00
- Account Mean Amount: ${detection["account_mean"]:.2f}
- Z-Score: {detection["zscore"]:.2f}
- Triggered Rules: {", ".join(detection["reasons"]) if detection["reasons"] else "none"}
- History Window: {detection["window_size"]} transactions

Is this transaction fraudulent?
""".strip()


def _disabled_result() -> dict[str, Any]:
    return {
        "gpt_is_fraud": None,
        "gpt_risk_level": None,
        "gpt_reasoning": None,
        "gpt_escalated": False,
    }


def _error_result(error: str) -> dict[str, Any]:
    return {
        "gpt_is_fraud": None,
        "gpt_risk_level": None,
        "gpt_reasoning": f"Escalation failed: {error}",
        "gpt_escalated": False,
    }
