"""core sub-package — Dataset, hashing, configuration, versioning, and store."""

from dataintegrity.core.dataset import Dataset
from dataintegrity.core.config import IntegrityConfig, DEFAULT_CONFIG
from dataintegrity.core.hashing import compute_dataframe_fingerprint
from dataintegrity.core.versioning import DatasetVersion
from dataintegrity.core.store import LocalVersionStore
from dataintegrity.core.config_hashing import compute_config_hash
from dataintegrity.core.execution import ExecutionManifest
from dataintegrity.core.result_schema import RuleResult, DatasetAuditResult

__all__ = [
    "Dataset",
    "IntegrityConfig",
    "DEFAULT_CONFIG",
    "compute_dataframe_fingerprint",
    "DatasetVersion",
    "LocalVersionStore",
    "compute_config_hash",
    "ExecutionManifest",
    "RuleResult",
    "DatasetAuditResult",
]
