"""
policies/file_policy.py
-----------------------
Policy implementation that loads thresholds from a YAML file.
"""

from typing import Any, Dict, List
import yaml
from pathlib import Path

from dataintegrity.policies.base import BasePolicy


class FilePolicy(BasePolicy):
    """
    Enforces quality thresholds defined in an external YAML file.
    
    Expected YAML structure:
        version: 1
        policy:
          completeness: 0.95
          uniqueness: 0.9
          ...
    """

    def __init__(self, policy_path: str) -> None:
        self.name = f"file:{Path(policy_path).name}"
        self.policy_path = Path(policy_path)
        self.policy_data = self._load_policy()

    def _load_policy(self) -> Dict[str, Any]:
        """Load and validate the YAML policy file."""
        if not self.policy_path.exists():
            raise FileNotFoundError(f"Policy file not found: {self.policy_path}")

        try:
            with open(self.policy_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except Exception as exc:
            raise ValueError(f"Failed to parse policy YAML: {exc}")

        if not isinstance(data, dict):
            raise ValueError("Policy file must be a YAML dictionary.")

        if "version" not in data:
            raise ValueError("Policy file missing top-level 'version' key.")
        
        if "policy" not in data or not isinstance(data["policy"], dict):
            raise ValueError("Policy file missing or invalid 'policy' section.")

        return data

    def evaluate(self, audit_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate audit results against thresholds in the policy file.
        Includes support for PII governance enforcement.
        """
        policy_config = self.policy_data.get("policy", {})
        dimension_scores = audit_result.get("dimension_scores", {})
        pii_report = audit_result.get("pii_summary", {})  # This is the new summary block
        
        # Backward compatibility for old pii_summary which might be a per-column dict
        # In v0.3.1+, pii_summary and pii_findings are structured.
        pii_summary_block = audit_result.get("pii_summary", {})
        if "total_matches" not in pii_summary_block:
             # Look inside the dict if it's the old structure
             pass

        violations = []
        pii_violation = False

        # 1. Dimension Score Checks
        for dim, min_score in policy_config.items():
            if dim == "pii": continue # Skip the PII block for now
            
            actual_score = dimension_scores.get(dim)
            if actual_score is not None and actual_score < min_score:
                violations.append(
                    f"Dimension '{dim}' failed: score {actual_score:.4f} < threshold {min_score}"
                )

        # 2. PII Governance Checks (Requirement 4)
        pii_policy = policy_config.get("pii")
        if pii_policy and isinstance(pii_policy, dict):
            # Check high risk (using summary block for efficiency)
            if pii_policy.get("block_high_risk") and pii_summary_block.get("high_risk_columns", 0) > 0:
                violations.append("PII Policy Violation: High-risk PII columns detected.")
                pii_violation = True
            
            # Check medium risk ratios (using flat findings list)
            max_medium_ratio = pii_policy.get("max_medium_risk_ratio")
            if max_medium_ratio is not None:
                # Get findings from root or summary
                findings_to_check = audit_result.get("pii_findings", [])
                if not findings_to_check and isinstance(audit_result.get("pii_summary"), dict):
                     # Fallback for some internal structures
                     pass
                
                for finding in findings_to_check:
                    if finding["highest_risk"] == "medium" and finding.get("match_ratio", 0) > max_medium_ratio:
                        col = finding.get("column", "unknown")
                        violations.append(
                            f"PII Policy Violation: Column '{col}' medium-risk ratio {finding['match_ratio']} "
                            f"> threshold {max_medium_ratio}"
                        )
                        pii_violation = True

            # Check low risk
            if pii_policy.get("allow_low_risk") is False and pii_summary_block.get("low_risk_columns", 0) > 0:
                violations.append("PII Policy Violation: Low-risk PII detected (disallowed).")
                pii_violation = True

        status = "PASS" if not violations else "FAIL"
        
        return {
            "policy": self.name,
            "type": "file",
            "version": self.policy_data.get("version"),
            "status": status,
            "passed": status == "PASS",
            "violations": violations,
            "pii_violation": pii_violation
        }
