"""
integrity/scorer.py
-------------------
Weighted composite DataScore computation.

DataScore ∈ [0, 100] — higher is better.

v0.2.1 change: When ``rule_severities`` is supplied, failing dimensions are
penalised proportionally via :func:`~dataintegrity.integrity.risk_model.apply_risk_weight`
before weighting.  Callers that omit ``rule_severities`` observe identical
behaviour to v0.2.0.
"""

from __future__ import annotations

from typing import Dict, Optional

from dataintegrity.core.config import IntegrityConfig, DEFAULT_CONFIG
from dataintegrity.integrity.risk_model import apply_risk_weight, DEFAULT_PASS_THRESHOLD


class DataScorer:
    """
    Aggregates per-dimension quality scores into a single composite DataScore.

    The weighting scheme is driven by the ``score_weights`` mapping in the
    supplied :class:`~dataintegrity.core.config.IntegrityConfig`.  Weights
    must sum to 1.0 (enforced by :meth:`IntegrityConfig.validate`).

    When ``rule_severities`` is provided to :meth:`compute`, failing rules
    (scores below ``threshold``) receive an additional severity-based penalty
    via :func:`~dataintegrity.integrity.risk_model.apply_risk_weight` before
    the weighted average is calculated.  This keeps the score within 0–100.

    Args:
        config: Configuration object (uses DEFAULT_CONFIG if omitted).

    Example::

        scorer = DataScorer()
        result = scorer.compute(
            {"completeness": 0.95, "uniqueness": 0.88,
             "validity": 1.00, "consistency": 0.90, "timeliness": 0.70},
        )
        # result["data_score"] → 91.4
    """

    def __init__(self, config: Optional[IntegrityConfig] = None) -> None:
        self.config = config or DEFAULT_CONFIG

    def compute(
        self,
        dimension_scores: Dict[str, float],
        rule_severities: Optional[Dict[str, str]] = None,
        pass_threshold: float = DEFAULT_PASS_THRESHOLD,
    ) -> Dict[str, object]:
        """
        Compute the weighted composite DataScore from individual dimension scores.

        Unknown dimension names in ``dimension_scores`` that are NOT in the
        weight config are silently ignored.  Missing dimensions receive a
        weight-proportional penalty of 0.

        When ``rule_severities`` is supplied, each dimension's score is first
        adjusted by :func:`~dataintegrity.integrity.risk_model.apply_risk_weight`
        before contributing to the weighted average:

        - **Passing** rules (score ≥ ``pass_threshold``) → score is unchanged.
        - **Failing** rules (score < ``pass_threshold``) → score is divided by
          the severity weight so high-severity failures hurt more.

        Score formula (per dimension *d* with weight *w*)::

            adjusted  = apply_risk_weight(raw, severity, passed)
            score     = Σ (adjusted_d × w_d) × 100

        Args:
            dimension_scores: Mapping of dimension name → score in [0.0, 1.0].
            rule_severities:  Optional mapping of dimension name → severity string
                              (``"LOW"``, ``"MEDIUM"``, ``"HIGH"``).
                              If ``None``, severity weighting is disabled.
            pass_threshold:   Score below which a rule is considered failed
                              (default: 0.5).

        Returns:
            Dict with keys:

            * ``"data_score"``    — float, composite score in [0.0, 100.0].
            * ``"breakdown"``     — per-dimension raw score, adjusted score,
                                    severity, weight, and weighted contribution.
            * ``"weights_used"``  — the weight mapping that was applied.
        """
        weights = self.config.score_weights
        weighted_sum = 0.0
        breakdown: Dict[str, Dict[str, object]] = {}

        for dimension, weight in weights.items():
            raw_score = dimension_scores.get(dimension, 0.0)

            # Apply risk-weighting when severity metadata is provided
            if rule_severities is not None:
                severity = rule_severities.get(dimension, "LOW")
                passed = raw_score >= pass_threshold
                adjusted_score = apply_risk_weight(raw_score, severity, passed)
            else:
                severity = "N/A"
                adjusted_score = raw_score

            contribution = adjusted_score * weight
            weighted_sum += contribution

            breakdown[dimension] = {
                "raw_score": round(raw_score, 4),
                "adjusted_score": round(adjusted_score, 4),
                "severity": severity,
                "weight": weight,
                "contribution": round(contribution, 4),
            }

        data_score = round(weighted_sum * 100, 2)

        return {
            "data_score": data_score,
            "breakdown": breakdown,
            "weights_used": dict(weights),
        }
