"""
core/config_hashing.py
----------------------
Deterministic hashing of an :class:`~dataintegrity.core.config.IntegrityConfig`
so that any change to configuration is detectable from the run manifest.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from typing import Any, Dict

from dataintegrity.core.config import IntegrityConfig


def _config_to_serialisable(config: IntegrityConfig) -> Dict[str, Any]:
    """
    Convert an :class:`IntegrityConfig` dataclass to a plain, JSON-serialisable
    dictionary with deterministically sorted keys at every nesting level.

    Args:
        config: The configuration object to serialise.

    Returns:
        A sorted, JSON-serialisable dict representation of the config.
    """
    raw: Dict[str, Any] = dataclasses.asdict(config)

    def _sort_recursive(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: _sort_recursive(v) for k in sorted(obj) for v in [obj[k]]}
        if isinstance(obj, list):
            return [_sort_recursive(i) for i in obj]
        return obj

    return _sort_recursive(raw)


def compute_config_hash(config: IntegrityConfig) -> str:
    """
    Compute a deterministic SHA-256 hash of an :class:`IntegrityConfig` instance.

    The config is first serialised to a sorted JSON string (so key ordering
    never affects the digest), then hashed with SHA-256.

    Args:
        config: The :class:`IntegrityConfig` to hash.

    Returns:
        Lowercase hex digest string (64 characters).

    Example::

        from dataintegrity.core.config import IntegrityConfig
        from dataintegrity.core.config_hashing import compute_config_hash

        cfg = IntegrityConfig()
        h = compute_config_hash(cfg)
        # h == compute_config_hash(IntegrityConfig())  # always True
    """
    serialisable = _config_to_serialisable(config)
    canonical_json = json.dumps(serialisable, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
