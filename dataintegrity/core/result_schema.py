"""
core/result_schema.py
---------------------
Structured, typed result objects returned by the integrity engine.

Classes
-------
* :class:`RuleResult`        — outcome of a single quality rule evaluation.
* :class:`DatasetAuditResult` — full audit outcome including manifest, all rule
                               results, drift data, PII summary, and the composite
                               score.

Backward Compatibility
----------------------
:meth:`DatasetAuditResult.to_legacy_dict` returns the exact ``dict`` shape that
the v0.2.0 engine returned, so any existing code that does ``result["data_score"]``
can be migrated trivially::

    legacy = audit_result.to_legacy_dict()
    print(legacy["data_score"])
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from dataintegrity.core.execution import ExecutionManifest


# ---------------------------------------------------------------------------
# RuleResult
# ---------------------------------------------------------------------------

@dataclass
class RuleResult:
    """
    Captures the outcome of evaluating a single data quality rule.

    Attributes:
        rule_id:      Unique identifier for the rule (e.g. ``"completeness"``).
        description:  Human-readable description of what the rule checks.
        metric_value: The raw score computed by the rule in ``[0.0, 1.0]``.
        threshold:    The minimum acceptable score for the rule to pass.
        passed:       ``True`` if ``metric_value >= threshold``.
        severity:     Risk level of this rule — one of ``"LOW"``, ``"MEDIUM"``,
                      ``"HIGH"``.
        weighted_contribution: Weighted contribution to the final DataScore.
    """

    rule_id: str
    description: str
    metric_value: float
    threshold: float
    passed: bool
    severity: str
    weighted_contribution: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialise to a plain dictionary.

        Returns:
            Dict representation of this rule result.
        """
        return {
            "rule_id": self.rule_id,
            "description": self.description,
            "metric_value": round(self.metric_value, 4),
            "threshold": self.threshold,
            "passed": self.passed,
            "severity": self.severity,
            "weighted_contribution": round(self.weighted_contribution, 4),
        }


# ---------------------------------------------------------------------------
# DatasetAuditResult
# ---------------------------------------------------------------------------

@dataclass
class DatasetAuditResult:
    """
    Full, structured result of a single dataset integrity audit.

    This object is returned by :meth:`~dataintegrity.integrity.engine.IntegrityEngine.run`
    and supersedes the raw ``dict`` returned in v0.2.0.

    Attributes:
        manifest:      :class:`~dataintegrity.core.execution.ExecutionManifest`
                       for this run.
        rule_results:  Ordered list of :class:`RuleResult` for every rule evaluated.
        drift_results: List of drift check outcome dicts (empty if drift not run).
        pii_summary:   PII scan summary dict (empty if PII not run inline).
        overall_score: Composite DataScore in ``[0.0, 100.0]``.
        breakdown:     Per-dimension score breakdown dict (mirrors legacy format).
        dimension_scores: Raw per-dimension scores in ``[0.0, 1.0]``.
        shape:         ``(rows, columns)`` tuple of the audited dataset.
        source:        Optional source path/table identifier.
    """

    manifest: ExecutionManifest
    rule_results: List[RuleResult]
    drift_results: List[Dict[str, Any]]
    pii_summary: Dict[str, Any]
    overall_score: float
    breakdown: Dict[str, Dict[str, float]]
    dimension_scores: Dict[str, float]
    shape: Tuple[int, int]
    schema_version: str = "0.3"
    fingerprint: Dict[str, Any] = field(default_factory=dict)
    source: Optional[str] = None
    standards_alignment: Optional[Dict[str, Any]] = None
    policy_evaluations: List[Dict[str, Any]] = field(default_factory=list)
    policy_evaluation: Optional[Dict[str, Any]] = None

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """
        Return a fully-structured dictionary representation.
        Requirement 2 & 3: pii_findings (flat list) and pii_summary (block).
        """
        # Collect flat findings from all columns
        flat_findings = []
        for col, report in self.pii_summary.items():
            if col == "pii_summary": continue
            if isinstance(report, dict) and "pii_findings" in report:
                flat_findings.extend(report["pii_findings"])

        summary_block = self.pii_summary.get("pii_summary", {})

        return {
            "manifest": self.manifest.to_dict(),
            "rule_results": [r.to_dict() for r in self.rule_results],
            "drift_results": self.drift_results,
            "pii_findings": flat_findings,
            "pii_summary": summary_block,
            "overall_score": self.overall_score,
            "breakdown": self.breakdown,
            "dimension_scores": self.dimension_scores,
            "shape": list(self.shape),
            "schema_version": self.schema_version,
            "fingerprint": self.fingerprint,
            "source": self.source,
            "standards_alignment": self.standards_alignment,
            "policy_evaluations": self.policy_evaluations,
            "policy_evaluation": self.policy_evaluation,
        }

    def to_json(self, indent: int = 2) -> str:
        """
        Serialise the full audit result to a JSON string.

        Args:
            indent: JSON indentation level (default: 2).

        Returns:
            Formatted JSON string.
        """
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_legacy_dict(self) -> Dict[str, Any]:
        """
        Return a dictionary in the exact v0.2.0 engine output format.

        This method exists solely for backward compatibility with code written
        against the v0.2.0 API that expects a raw ``dict`` from ``engine.run()``.

        Returns:
            Dict with keys: ``data_score``, ``breakdown``, ``dimension_scores``,
            ``fingerprint``, ``shape``, ``source``, ``rules_run``.

        Example::

            result = engine.run(dataset)               # DatasetAuditResult
            legacy = result.to_legacy_dict()
            print(legacy["data_score"])                # works like v0.2.0
        """
        return {
            "data_score": self.overall_score,
            "breakdown": self.breakdown,
            "dimension_scores": self.dimension_scores,
            "fingerprint": self.manifest.dataset_fingerprint,
            "shape": self.shape,
            "source": self.source,
            "rules_run": self.manifest.rules_executed,
            "pii_summary": self.pii_summary,
        }
