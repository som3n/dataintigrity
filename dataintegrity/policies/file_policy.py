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
        """
        thresholds = self.policy_data["policy"]
        dimension_scores = audit_result.get("dimension_scores", {})
        
        violations = []
        for dim, min_score in thresholds.items():
            actual_score = dimension_scores.get(dim)
            if actual_score is None:
                # If a dimension in policy is not present in audit, we count it as a violation
                # or we could skip it. The prompt says "Apply thresholds to audit results".
                violations.append(f"Dimension '{dim}' defined in policy was not evaluated in audit.")
                continue
            
            if actual_score < min_score:
                violations.append(
                    f"Dimension '{dim}' failed: score {actual_score:.4f} < threshold {min_score}"
                )

        status = "PASS" if not violations else "FAIL"
        
        return {
            "policy": self.name,
            "type": "file",
            "version": self.policy_data.get("version"),
            "status": status,
            "passed": status == "PASS",
            "violations": violations,
        }
