"""
ingestion/pii.py
----------------
Regex-based PII (Personally Identifiable Information) detection engine.

Scans every string/object column in a Dataset for known PII patterns and
returns a structured per-column report.
"""

from __future__ import annotations

import math
import re
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from dataintegrity.core.config import IntegrityConfig, DEFAULT_CONFIG
from dataintegrity.core.dataset import Dataset
from dataintegrity.ingestion.pii_registry import GLOBAL_PII_REGISTRY, luhn_check


def calculate_shannon_entropy(text: str) -> float:
    """
    Calculate the Shannon entropy of a string.
    High entropy suggests random-looking strings (like some IDs).
    """
    if not text:
        return 0.0
    probabilities = [n_x / len(text) for n_x in pd.Series(list(text)).value_counts()]
    entropy = -sum(p * math.log(p, 2) for p in probabilities)
    return entropy


class PIIDetector:
    """
    Detects PII in DataFrame columns using configurable regex patterns.

    Args:
        config: An :class:`~dataintegrity.core.config.IntegrityConfig` instance.
                Uses :data:`~dataintegrity.core.config.DEFAULT_CONFIG` if omitted.

    The detector scans every column whose dtype is ``object`` (string-like)
    and optionally also numeric/datetime columns when their string representation
    might match a pattern (e.g. SSNs stored as integers).
    """

    def __init__(self, config: Optional[IntegrityConfig] = None) -> None:
        self.config = config or DEFAULT_CONFIG
        self._compiled: Dict[str, re.Pattern] = {
            name: re.compile(pattern)
            for name, pattern in self.config.pii_patterns.items()
        }
        self._global_compiled = [
            (entity, re.compile(entity.regex))
            for entity in GLOBAL_PII_REGISTRY
        ]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan(self, dataset: Dataset, sample_threshold: int = 100000) -> Dict[str, Any]:
        """
        Scan all columns in the dataset for PII patterns with governance-grade enforcement.

        This version includes:
        - Deterministic Sampling for large datasets.
        - Priority-Based Match Resolution (deterministic overlap handling).
        - False Positive Guardrails (sequential, repeated, entropy, ratio).
        - Risk Aggregation and Summary.

        Args:
            dataset: The dataset to inspect.
            sample_threshold: Threshold above which to perform deterministic sampling.

        Returns:
            A PII result dict containing 'pii_summary' (dataset-level) and
            per-column details.
        """
        df = dataset.df
        total_rows = len(df)
        
        # 0. Deterministic Sampling
        if total_rows > sample_threshold:
            # Deterministic first-N sample (per requirement 6)
            scan_df = df.iloc[:sample_threshold]
            is_sampled = True
        else:
            scan_df = df
            is_sampled = False

        column_reports: Dict[str, Any] = {}
        total_matches = 0
        risk_counts = {"high": 0, "medium": 0, "low": 0}

        for col in scan_df.columns:
            series = scan_df[col]

            # Skip obviously non-PII columns (dates, bools)
            if pd.api.types.is_bool_dtype(series): continue
            if pd.api.types.is_datetime64_any_dtype(series): continue
            
            # Heuristic: skip predominantly ISO-date columns
            sample_head = series.dropna().astype(str).head(20)
            if not sample_head.empty:
                iso_date_hits = sample_head.str.match(r"^\d{4}-\d{2}-\d{2}").sum()
                if iso_date_hits / len(sample_head) >= 0.8:
                    continue

            col_report = self._scan_column(col, series, total_rows if not is_sampled else sample_threshold)
            
            # Aggregate risk for summary block
            if col_report["pii_detected"]:
                column_reports[col] = col_report
                total_matches += col_report["count"]
                
                # Findings might have different risks; take the highest
                findings = col_report.get("pii_findings", [])
                if findings:
                    highest_risk = "low"
                    risk_map = {"high": 3, "medium": 2, "low": 1}
                    for f in findings:
                        if risk_map.get(f["highest_risk"], 0) > risk_map.get(highest_risk, 0):
                            highest_risk = f["highest_risk"]
                    risk_counts[highest_risk] += 1

        # Dataset-Level Summary Block (Requirement 3)
        pii_summary = {
            "high_risk_columns": risk_counts["high"],
            "medium_risk_columns": risk_counts["medium"],
            "low_risk_columns": risk_counts["low"],
            "total_columns_with_pii": len(column_reports),
            "total_matches": total_matches,
            "is_sampled": is_sampled,
            "sample_size": sample_threshold if is_sampled else total_rows
        }

        # Combine with column reports (backward compatibility)
        result = column_reports
        result["pii_summary"] = pii_summary
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _scan_column(self, col_name: str, series: pd.Series, scan_total: int) -> Dict[str, Any]:
        """Scan a single Series and return its PII report dict."""
        string_values = series.dropna().astype(str)
        if string_values.empty:
            return {
                "pii_detected": False,
                "count": 0,
                "patterns_hit": {},
                "pii_findings": []
            }

        # Row-level match tracking for Priority Resolution (Requirement 1)
        row_matches: Dict[int, List[Dict[str, Any]]] = {}
        
        # Robust Guardrails (Requirement 5+)
        is_numeric = pd.api.types.is_numeric_dtype(series)
        has_decimals = False
        if is_numeric:
            # Check for non-zero fractional parts
            # We sample to stay performant on large numeric columns
            sample_numeric = series.dropna().head(1000)
            if not sample_numeric.empty:
                has_decimals = (sample_numeric % 1 != 0).any()

        # 1. Configurable patterns (backward compatibility)
        for pattern_name, regex in self._compiled.items():
            # Robust Guardrail: skip phone/ssn/cc for columns with decimals
            if has_decimals and pattern_name in ["phone", "ssn", "credit_card"]:
                continue

            for idx, val in zip(string_values.index, string_values):
                if regex.search(val):
                    # FP Guardrails: sequential/repeating (Requirement 5)
                    if self._is_noisy(val):
                        continue
                        
                    # Robust Guardrail: purely numeric broad matches in numeric columns
                    if is_numeric and pattern_name == "phone" and val.isdigit():
                        # A purely numeric 'phone' in a numeric column is likely a quantity
                        continue

                    if idx not in row_matches: row_matches[idx] = []
                    row_matches[idx].append({
                        "type": pattern_name,
                        "priority": 99,  # Default low priority for legacy
                        "category": "legacy",
                        "risk_level": "medium" if pattern_name in ["email", "phone"] else "low",
                        "confidence": "high"
                    })

        # 2. Global Identity Registry
        for entity, regex in self._global_compiled:
            # Robust Guardrail: skip most identities for columns with decimals
            if has_decimals and entity.category in ["identity", "financial"]:
                continue

            for idx, val in zip(string_values.index, string_values):
                if regex.search(val):
                    # Specialized validations
                    if entity.type == "credit_card" and not luhn_check(val):
                        continue
                    
                    if self._is_noisy(val):
                        continue

                    # Robust Guardrail: purely numeric broad matches in numeric columns
                    if is_numeric and entity.type == "passport" and val.isdigit():
                        # A purely numeric 'passport' in a numeric column is likely an index or quantity
                        continue

                    if idx not in row_matches: row_matches[idx] = []
                    row_matches[idx].append({
                        "type": entity.type,
                        "priority": entity.priority,
                        "category": entity.category,
                        "risk_level": entity.risk_level,
                        "confidence": entity.confidence,
                        "val": val
                    })

        # 3. Priority Resolution: Keep only highest priority match per row
        resolved_matches: List[Dict[str, Any]] = []
        rows_with_any_hit: Set[int] = set()
        
        for idx, matches in row_matches.items():
            # Sort by priority (asc)
            best_match = sorted(matches, key=lambda x: x["priority"])[0]
            resolved_matches.append(best_match)
            rows_with_any_hit.add(idx)

        # 4. Column-Level Aggregation (Requirement 2)
        type_counts: Dict[str, int] = {}
        for m in resolved_matches:
            t = m["type"]
            type_counts[t] = type_counts.get(t, 0) + 1

        pii_findings = []
        for t, count in type_counts.items():
            # Get metadata from first occurrence or registry
            # We'll just find the first match of this type
            sample_match = next(m for m in resolved_matches if m["type"] == t)
            
            # Match ratio check (FP Guardrail)
            match_ratio = count / scan_total
            if match_ratio < 0.01 and sample_match["confidence"] != "high":
                continue # Ignore low-confidence low-ratio matches
            
            # Entropy check for confidence boost
            if "val" in sample_match:
                entropy = calculate_shannon_entropy(sample_match["val"])
                # Only check entropy for medium confidence matches
                if entropy < 2.0 and sample_match["confidence"] != "high":
                     continue # Ignore low entropy "random" matches

            pii_findings.append({
                "column": col_name,
                "dominant_type": t,
                "category": sample_match["category"],
                "highest_risk": sample_match["risk_level"],
                "confidence": sample_match["confidence"],
                "matches": count,
                "match_ratio": round(match_ratio, 4)
            })

        # 5. Generic Identifier Heuristics (Requirement 2 logic updated)
        if not pii_findings: # Only if no high-confidence registry matches
            keywords = ["id", "identifier", "national", "tax", "gov", "registration"]
            if any(kw in col_name.lower() for kw in keywords):
                unique_ratio = series.nunique() / scan_total
                sample_str = string_values.head(100)
                avg_len = sample_str.str.len().mean()
                if 8 <= avg_len <= 20 and unique_ratio > 0.8:
                    heuristic_count = len(string_values)
                    # For heuristics, we assume all non-null rows are affected
                    rows_with_any_hit |= set(string_values.index)
                    
                    pii_findings.append({
                        "column": col_name,
                        "dominant_type": "unknown_structured_identifier",
                        "category": "identity",
                        "highest_risk": "medium",
                        "confidence": "medium",
                        "matches": heuristic_count,
                        "match_ratio": round(heuristic_count / scan_total, 4)
                    })

        pii_count = len(rows_with_any_hit)
        
        # Backward compatibility fields
        legacy_patterns_hit = {}
        for t, count in type_counts.items():
            legacy_patterns_hit[t] = count

        return {
            "pii_detected": pii_count > 0 or len(pii_findings) > 0,
            "count": pii_count,
            "patterns_hit": legacy_patterns_hit,
            "pii_findings": pii_findings
        }

    def _is_noisy(self, val: str) -> bool:
        """Sequential or repeated digit guards (Requirement 5)."""
        clean = "".join(filter(str.isdigit, val))
        if not clean: return False
        
        # Repeating: 0000000
        if len(set(clean)) == 1 and len(clean) > 4:
            return True
            
        # Sequential: 123456789
        if clean in "0123456789" or clean in "9876543210":
            if len(clean) > 4:
                return True
        
        return False
