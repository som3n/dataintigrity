"""integrity sub-package — rules, scorer, orchestration engine, and comparator."""

from dataintegrity.integrity.rules import RULE_REGISTRY
from dataintegrity.integrity.scorer import DataScorer
from dataintegrity.integrity.engine import IntegrityEngine
from dataintegrity.integrity.comparison import IntegrityComparator
from dataintegrity.integrity.plugins import IntegrityRule, register_rule, get_registered_rules
from dataintegrity.integrity.risk_model import SEVERITY_WEIGHTS
from dataintegrity.integrity.history import IntegrityHistoryTracker

__all__ = [
    "RULE_REGISTRY",
    "DataScorer",
    "IntegrityEngine",
    "IntegrityComparator",
    "IntegrityRule",
    "register_rule",
    "get_registered_rules",
    "SEVERITY_WEIGHTS",
    "IntegrityHistoryTracker",
]
