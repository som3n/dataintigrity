"""
core/config.py
--------------
Centralized configuration management for the dataintegrity SDK.
"""

from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class IntegrityConfig:
    """
    Configuration object for the dataintegrity pipeline.

    Attributes:
        drift_p_threshold: p-value threshold below which drift is flagged.
        timeliness_max_age_days: max acceptable age (days) for timestamp columns.
        score_weights: Weighted contribution of each quality dimension.
        pii_patterns: Mapping of PII type to regex pattern string.
    """

    drift_p_threshold: float = 0.05
    timeliness_max_age_days: int = 30

    score_weights: Dict[str, float] = field(default_factory=lambda: {
        "completeness": 0.30,
        "uniqueness": 0.20,
        "validity": 0.20,
        "consistency": 0.20,
        "timeliness": 0.10,
    })

    pii_patterns: Dict[str, str] = field(default_factory=lambda: {
        "email": r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",
        "phone": r"\b(\+?\d[\d\s\-().]{7,}\d)\b",
        "ssn": r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b",
        "credit_card": r"\b(?:\d[ -]?){13,16}\b",
    })

    def validate(self) -> None:
        """Validate configuration values are within acceptable ranges."""
        total_weight = sum(self.score_weights.values())
        if not abs(total_weight - 1.0) < 1e-6:
            raise ValueError(
                f"score_weights must sum to 1.0, got {total_weight:.4f}"
            )
        if not 0.0 < self.drift_p_threshold < 1.0:
            raise ValueError("drift_p_threshold must be between 0 and 1.")
        if self.timeliness_max_age_days <= 0:
            raise ValueError("timeliness_max_age_days must be positive.")


# Singleton default config — callers may override by passing their own instance.
DEFAULT_CONFIG = IntegrityConfig()
