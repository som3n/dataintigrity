"""
standards/iso_25012.py
----------------------
Lightweight alignment mapping for the ISO/IEC 25012 Data Quality Model.

This module provides functions to map the dataintegrity internal metrics
to ISO/IEC 25012 quality characteristics. This is for alignment reporting
only and does not constitute official certification.
"""

from __future__ import annotations

from typing import Dict


# Mapping internal dimensions to ISO 25012 characteristics
# We focus on characteristics that are programmatically evaluable.
# Note: "Accuracy" encompasses validity (semantic accuracy).
ISO_25012_MAPPING = {
    "Completeness": ["completeness"],
    "Consistency": ["consistency"],
    "Currentness": ["timeliness"],
    "Accuracy": ["validity"],
}

# ISO-recommended default weights for general purpose auditing
ISO_25012_DEFAULT_WEIGHTS = {
    "completeness": 0.30,
    "uniqueness": 0.15,
    "validity": 0.25,
    "consistency": 0.15,
    "timeliness": 0.15,
}


def evaluate_iso_25012_alignment(dimension_scores: Dict[str, float]) -> Dict[str, dict]:
    """
    Maps internal dimension scores to ISO 25012 characteristics and assigns status.

    Args:
        dimension_scores: Dict of internal dimension scores (e.g. {"completeness": 0.95, ...})

    Returns:
        Dict mapping ISO characteristic names to score and status objects.
    """
    alignment = {}

    for iso_char, internal_dims in ISO_25012_MAPPING.items():
        # Calculate mean if multiple internal dimensions map to one ISO char
        scores = [
            dimension_scores.get(dim, 0.0)
            for dim in internal_dims
            if dim in dimension_scores
        ]
        
        if not scores:
            score = 0.0
        else:
            score = sum(scores) / len(scores)

        alignment[iso_char] = {
            "score": float(score),
            "status": _get_status_for_score(score),
        }

    return alignment


def _get_status_for_score(score: float) -> str:
    """
    Assign a status label based on the 0-1 score according to ISO alignment logic.
    """
    if score >= 0.95:
        return "PASS"
    if score >= 0.85:
        return "MINOR_ISSUES"
    if score >= 0.70:
        return "MODERATE_ISSUES"
    return "CRITICAL"
