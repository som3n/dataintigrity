"""
ingestion/pii_registry.py
-------------------------
Modular registry for high-confidence global identity patterns.
"""

from dataclasses import dataclass
from typing import List, Optional

@dataclass
class PIIEntity:
    """
    Representation of a known PII identity type.
    """
    type: str
    category: str  # "identity", "financial", "contact"
    country: str   # "US", "IN", "UK", "global", etc.
    regex: str
    risk_level: str  # "high", "medium", "low"
    confidence: str  # "high", "medium"
    priority: int    # Lower = Higher priority (e.g., 1 is highest)

def luhn_check(number: str) -> bool:
    """
    Implementation of the Luhn algorithm for credit card validation.
    """
    digits = [int(d) for d in number if d.isdigit()]
    if not digits:
        return False
    
    checksum = 0
    reverse_digits = digits[::-1]
    for i, digit in enumerate(reverse_digits):
        if i % 2 == 1:
            digit *= 2
            if digit > 9:
                digit -= 9
        checksum += digit
    return checksum % 10 == 0

GLOBAL_PII_REGISTRY: List[PIIEntity] = [
    PIIEntity(
        type="ssn_us",
        category="identity",
        country="US",
        regex=r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b",
        risk_level="high",
        confidence="high",
        priority=2
    ),
    PIIEntity(
        type="aadhaar_india",
        category="identity",
        country="IN",
        regex=r"\b\d{4}\s?\d{4}\s?\d{4}\b",
        risk_level="high",
        confidence="high",
        priority=2
    ),
    PIIEntity(
        type="pan_india",
        category="identity",
        country="IN",
        regex=r"\b[A-Z]{5}\d{4}[A-Z]{1}\b",
        risk_level="high",
        confidence="high",
        priority=2
    ),
    PIIEntity(
        type="gstin_india",
        category="identity",
        country="IN",
        regex=r"\b\d{2}[A-Z]{5}\d{4}[A-Z]{1}[A-Z\d]{1}[Z]{1}[A-Z\d]{1}\b",
        risk_level="high",
        confidence="high",
        priority=2
    ),
    PIIEntity(
        type="ni_uk",
        category="identity",
        country="UK",
        regex=r"\b[A-CEGHJ-PR-TW-Z]{1}[A-CEGHJ-NPR-TW-Z]{1}\s?\d{2}\s?\d{2}\s?\d{2}\s?[A-D]{1}\b",
        risk_level="high",
        confidence="high",
        priority=2
    ),
    PIIEntity(
        type="credit_card",
        category="financial",
        country="global",
        regex=r"\b(?:\d[ -]?){13,16}\b",
        risk_level="high",
        confidence="high",  # Will be further validated with Luhn
        priority=1
    ),
    PIIEntity(
        type="iban",
        category="financial",
        country="global",
        regex=r"\b[A-Z]{2}\d{2}[A-Z\d]{4}\d{7}([A-Z\d]?){0,16}\b",
        risk_level="high",
        confidence="high",
        priority=3
    ),
    PIIEntity(
        type="passport",
        category="identity",
        country="global",
        regex=r"\b[A-Z0-9]{6,9}\b",  # Basic heuristic, usually needs context
        risk_level="high",
        confidence="medium",
        priority=4
    )
]
