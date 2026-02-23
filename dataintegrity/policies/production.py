"""
policies/production.py
----------------------
Implementation of the Production Policy:
- DataScore >= 95
- Completeness >= 0.98
- Drifted columns = 0
"""

from typing import Any, Dict
from .base import BasePolicy


class ProductionPolicy(BasePolicy):
    name = "production"

    MIN_DATASCORE = 95
    MIN_COMPLETENESS = 0.98
    MAX_DRIFTED_COLUMNS = 0

    def evaluate(self, audit_result: Dict[str, Any]) -> Dict[str, Any]:
        violations = []

        score = audit_result.get("overall_score", 0)
        if score < self.MIN_DATASCORE:
            violations.append(f"DataScore ({score:.1f}) is below {self.MIN_DATASCORE}")

        dim_scores = audit_result.get("dimension_scores", {})
        comp_score = dim_scores.get("completeness", 0)
        if comp_score < self.MIN_COMPLETENESS:
            violations.append(
                f"Completeness ({comp_score:.1%}) is below {self.MIN_COMPLETENESS:.1%}"
            )

        drift_results = audit_result.get("drift_results", [])
        drifted_cols = [r["column"] for r in drift_results if r.get("drift_detected")]
        if len(drifted_cols) > self.MAX_DRIFTED_COLUMNS:
            violations.append("Data drift detected (zero tolerance for production)")

        return {
            "policy": self.name,
            "status": "PASS" if not violations else "FAIL",
            "violations": violations
        }
