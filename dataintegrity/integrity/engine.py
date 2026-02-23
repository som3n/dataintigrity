"""
integrity/engine.py
-------------------
Orchestration engine that drives the full integrity audit pipeline.

Accepts a :class:`~dataintegrity.core.dataset.Dataset`, runs every registered
quality rule in :mod:`~dataintegrity.integrity.rules`, computes the composite
DataScore with severity-based risk weighting, and returns a fully-structured
:class:`~dataintegrity.core.result_schema.DatasetAuditResult`.

v0.2.1 changes
--------------
* Returns :class:`~dataintegrity.core.result_schema.DatasetAuditResult` instead
  of a raw ``dict``.  Call ``result.to_legacy_dict()`` for v0.2.0-compatible
  output.
* Generates an :class:`~dataintegrity.core.execution.ExecutionManifest` on every
  run capturing config hash, environment fingerprint, and rule/drift metadata.
* Applies severity-based risk weighting via
  :func:`~dataintegrity.integrity.risk_model.apply_risk_weight`.
"""

from __future__ import annotations

import warnings
from typing import Any, Dict, List, Optional

from dataintegrity.core.config import IntegrityConfig, DEFAULT_CONFIG
from dataintegrity.core.config_hashing import compute_config_hash
from dataintegrity.core.dataset import Dataset
from dataintegrity.core.execution import ExecutionManifest
from dataintegrity.core.result_schema import DatasetAuditResult, RuleResult
from dataintegrity.integrity.rules import (
    RULE_REGISTRY,
    RULE_SEVERITY,
    RULE_DESCRIPTIONS,
)
from dataintegrity.integrity.scorer import DataScorer


class IntegrityEngine:
    """
    Runs all registered data quality rules against a Dataset and aggregates
    the results into a :class:`~dataintegrity.core.result_schema.DatasetAuditResult`.

    Args:
        config:            An :class:`~dataintegrity.core.config.IntegrityConfig`
                           instance.  Defaults to the package-level
                           :data:`~dataintegrity.core.config.DEFAULT_CONFIG`.
        timestamp_columns: Column names to use for timeliness checks.
        column_groups:     Column groups for cross-column consistency checks.

    Example::

        from dataintegrity.core.dataset import Dataset
        from dataintegrity.integrity.engine import IntegrityEngine

        engine = IntegrityEngine()
        result = engine.run(dataset)
        # v0.2.1 structured result
        print(result.overall_score)
        print(result.manifest.run_id)

        # v0.2.0 compatible access
        legacy = result.to_legacy_dict()
        print(legacy["data_score"])
    """

    def __init__(
        self,
        config: Optional[IntegrityConfig] = None,
        timestamp_columns: Optional[List[str]] = None,
        column_groups: Optional[List[List[str]]] = None,
    ) -> None:
        self.config = config or DEFAULT_CONFIG
        self.timestamp_columns = timestamp_columns or []
        self.column_groups = column_groups
        self._scorer = DataScorer(config=self.config)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, dataset: Dataset, profile: Optional[str] = None) -> DatasetAuditResult:
        """
        Execute the full integrity pipeline against the dataset.

        Steps
        -----
        1. Handle profile-specific overrides (e.g. ISO-25012 weights).
        2. Compute a config hash for the manifest.
        3. Run every rule in :data:`~dataintegrity.integrity.rules.RULE_REGISTRY`.
        4. Build a :class:`~dataintegrity.core.result_schema.RuleResult` per rule.
        5. Compute the composite DataScore with severity risk weighting.
        6. If profile selected, evaluate standards alignment.
        7. Generate an :class:`~dataintegrity.core.execution.ExecutionManifest`.
        8. Return a :class:`~dataintegrity.core.result_schema.DatasetAuditResult`.

        Args:
            dataset: The dataset to audit.
            profile: Optional standards alignment profile (e.g. "iso-25012").

        Returns:
            A :class:`~dataintegrity.core.result_schema.DatasetAuditResult`.
        """
        config = self.config
        
        # 1. Profile-specific overrides
        alignment_data = None
        if profile == "iso-25012":
            from dataintegrity.standards.iso_25012 import (
                evaluate_iso_25012_alignment,
                ISO_25012_DEFAULT_WEIGHTS
            )
            # Override weights if user hasn't passed a custom config with weights
            # (Check if config is the DEFAULT_CONFIG to detect 'no custom config')
            if config is DEFAULT_CONFIG:
                config = IntegrityConfig(
                    score_weights=ISO_25012_DEFAULT_WEIGHTS,
                    drift_p_threshold=config.drift_p_threshold,
                    timeliness_max_age_days=config.timeliness_max_age_days
                )

        config_hash = compute_config_hash(config)
        dimension_scores: Dict[str, float] = {}

        # ----------------------------------------------------------------
        # 2. Run all rules
        # ----------------------------------------------------------------
        for rule_name, rule_fn in RULE_REGISTRY.items():
            try:
                score = rule_fn(
                    dataset,
                    config=config,
                    timestamp_columns=self.timestamp_columns,
                    column_groups=self.column_groups,
                )
                dimension_scores[rule_name] = float(score)
            except Exception as exc:  # pragma: no cover
                dimension_scores[rule_name] = 0.0
                warnings.warn(
                    f"Rule {rule_name!r} raised an exception and will score 0: {exc}",
                    RuntimeWarning,
                    stacklevel=2,
                )

        # ----------------------------------------------------------------
        # 3. Compute score with severity risk weighting
        # ----------------------------------------------------------------
        scorer = DataScorer(config=config)
        score_result = scorer.compute(
            dimension_scores,
            rule_severities=RULE_SEVERITY,
        )
        overall_score: float = score_result["data_score"]  # type: ignore[assignment]
        breakdown: Dict[str, Any] = score_result["breakdown"]  # type: ignore[assignment]

        # ----------------------------------------------------------------
        # 4. Standards Alignment evaluation
        # ----------------------------------------------------------------
        if profile == "iso-25012":
            from dataintegrity.standards.iso_25012 import evaluate_iso_25012_alignment
            alignment_data = {
                "profile": "ISO/IEC 25012",
                "characteristics": evaluate_iso_25012_alignment(dimension_scores)
            }

        # ----------------------------------------------------------------
        # 5. Build typed RuleResult list
        # ----------------------------------------------------------------
        rule_results: List[RuleResult] = []
        for rule_name in RULE_REGISTRY:
            raw = dimension_scores.get(rule_name, 0.0)
            severity = RULE_SEVERITY.get(rule_name, "LOW")
            description = RULE_DESCRIPTIONS.get(rule_name, rule_name)
            passed = raw >= 0.5  # standard pass threshold
            dim_breakdown = breakdown.get(rule_name, {})
            contribution = float(dim_breakdown.get("contribution", 0.0))  # type: ignore[arg-type]
            rule_results.append(
                RuleResult(
                    rule_id=rule_name,
                    description=description,
                    metric_value=raw,
                    threshold=0.5,
                    passed=passed,
                    severity=severity,
                    weighted_contribution=contribution,
                )
            )

        # ----------------------------------------------------------------
        # 6. Build ExecutionManifest
        # ----------------------------------------------------------------
        manifest = ExecutionManifest.create(
            dataset_fingerprint=dataset.fingerprint,
            config_hash=config_hash,
            rules_executed=list(RULE_REGISTRY.keys()),
            final_score=overall_score,
            drift_checks_executed=[],
        )

        # ----------------------------------------------------------------
        # 7. Assemble structured result
        # ----------------------------------------------------------------
        audit_result = DatasetAuditResult(
            manifest=manifest,
            rule_results=rule_results,
            drift_results=[],
            pii_summary={},
            overall_score=overall_score,
            breakdown=breakdown,
            dimension_scores=dimension_scores,
            shape=dataset.shape,
            source=dataset.source,
            standards_alignment=alignment_data,
        )

        # ----------------------------------------------------------------
        # 8. Persist summary into dataset.profile (backward compat)
        # ----------------------------------------------------------------
        dataset.profile.update(audit_result.to_legacy_dict())

        return audit_result
