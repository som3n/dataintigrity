"""
policies/research.py
--------------------
Implementation of the Research Policy:
- DataScore >= 90
- Completeness >= 0.95
- Max drifted columns = 2
- No CRITICAL ISO alignment characteristics
"""

from typing import Any, Dict
from .base import BasePolicy


class ResearchPolicy(BasePolicy):
    name = "research"

    MIN_DATASCORE = 90
    MIN_COMPLETENESS = 0.95
    MAX_DRIFTED_COLUMNS = 2

    def evaluate(self, audit_result: Dict[str, Any]) -> Dict[str, Any]:
        violations = []

        # 1. DataScore check
        score = audit_result.get("overall_score", 0)
        if score < self.MIN_DATASCORE:
            violations.append(f"DataScore ({score:.1f}) is below {self.MIN_DATASCORE}")

        # 2. Completeness check
        dim_scores = audit_result.get("dimension_scores", {})
        comp_score = dim_scores.get("completeness", 0)
        if comp_score < self.MIN_COMPLETENESS:
            violations.append(
                f"Completeness ({comp_score:.1%}) is below {self.MIN_COMPLETENESS:.1%}"
            )

        # 3. Drift check
        drift_results = audit_result.get("drift_results", [])
        drifted_cols = [r["column"] for r in drift_results if r.get("drift_detected")]
        if len(drifted_cols) > self.MAX_DRIFTED_COLUMNS:
            violations.append(
                f"Drifted columns ({len(drifted_cols)}) exceed limit ({self.MAX_DRIFTED_COLUMNS})"
            )

        # 4. ISO Alignment check (if profile was used)
        standards = audit_result.get("standards_alignment")
        if standards and standards.get("characteristics"):
            for char, info in standards["characteristics"].items():
                if info.get("status") == "CRITICAL":
                    violations.append(f"ISO Characteristic '{char}' is CRITICAL")

        return {
            "policy": self.name,
            "status": "PASS" if not violations else "FAIL",
            "violations": violations
        }
