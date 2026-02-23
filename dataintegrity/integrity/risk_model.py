"""
integrity/risk_model.py
-----------------------
Severity-based risk weighting for data quality rule scores.

Formula
-------
When a rule **fails** (``metric_value < threshold``), the raw score is penalised
proportionally to the rule's severity weight::

    penalised_score = raw_score / severity_weight

This means:
  * A LOW   severity failure (weight=1.0) is unchanged.
  * A MEDIUM severity failure (weight=1.5) is divided by 1.5  → harsher penalty.
  * A HIGH   severity failure (weight=2.0) is divided by 2.0  → harshest penalty.

The result is still clipped to ``[0.0, 1.0]`` so the DataScore scale is preserved.

When a rule **passes**, the raw score is returned unchanged regardless of severity,
since the weight is only a risk amplifier for failures.

Public API
----------
* :data:`SEVERITY_WEIGHTS` — the severity → weight mapping.
* :func:`apply_risk_weight` — apply the weighting to a single rule score.
"""

from __future__ import annotations

from typing import Dict

# ---------------------------------------------------------------------------
# Severity weight table
# ---------------------------------------------------------------------------

SEVERITY_WEIGHTS: Dict[str, float] = {
    "LOW": 1.0,
    "MEDIUM": 1.5,
    "HIGH": 2.0,
}

#: The minimum score below which a rule is considered to have failed.
DEFAULT_PASS_THRESHOLD: float = 0.5


def apply_risk_weight(
    raw_score: float,
    severity: str,
    passed: bool,
) -> float:
    """
    Apply a severity-based risk penalty to a rule's raw score.

    **Passing rules** are returned unchanged — severity only amplifies failures.

    **Failing rules** are penalised as follows::

        penalised = raw_score / SEVERITY_WEIGHTS[severity]

    The result is clamped to ``[0.0, 1.0]``.

    Args:
        raw_score: Raw rule score in ``[0.0, 1.0]`` from the rule function.
        severity:  Rule severity — one of ``"LOW"``, ``"MEDIUM"``, ``"HIGH"``.
                   Unknown values fall back to weight ``1.0`` (no extra penalty).
        passed:    Whether the rule passed (``True``) or failed (``False``).

    Returns:
        Risk-adjusted score in ``[0.0, 1.0]``.

    Examples::

        apply_risk_weight(0.3, "HIGH",   passed=False)  # 0.3 / 2.0 = 0.15
        apply_risk_weight(0.3, "MEDIUM", passed=False)  # 0.3 / 1.5 = 0.20
        apply_risk_weight(0.3, "LOW",    passed=False)  # 0.3 / 1.0 = 0.30
        apply_risk_weight(0.9, "HIGH",   passed=True)   # 0.9        (unchanged)
    """
    if passed:
        return float(max(0.0, min(1.0, raw_score)))

    weight = SEVERITY_WEIGHTS.get(severity.upper() if severity else "LOW", 1.0)
    penalised = raw_score / weight
    return float(max(0.0, min(1.0, penalised)))
