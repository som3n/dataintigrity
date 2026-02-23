"""
policies package — enforceable governance rules.
"""

from .research import ResearchPolicy
from .production import ProductionPolicy

POLICY_REGISTRY = {
    "research": ResearchPolicy(),
    "production": ProductionPolicy(),
}

__all__ = ["POLICY_REGISTRY", "ResearchPolicy", "ProductionPolicy"]
