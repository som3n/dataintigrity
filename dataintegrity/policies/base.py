"""
policies/base.py
----------------
Abstract base class and interface for the Data Integrity Policy Engine.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class BasePolicy(ABC):
    """
    Interface for enforceable governance policies.
    """
    name: str

    @abstractmethod
    def evaluate(self, audit_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate an audit result against the policy rules.

        Args:
            audit_result: The dictionary representation of a DatasetAuditResult.

        Returns:
            Dict containing:
                - policy: name of the policy
                - status: "PASS" or "FAIL"
                - violations: list of failure reason strings
        """
        pass
