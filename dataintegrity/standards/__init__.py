"""
standards sub-package — alignment with data quality models (e.g., ISO/IEC 25012).
"""

from dataintegrity.standards.iso_25012 import (
    evaluate_iso_25012_alignment,
    ISO_25012_DEFAULT_WEIGHTS,
)

__all__ = ["evaluate_iso_25012_alignment", "ISO_25012_DEFAULT_WEIGHTS"]
