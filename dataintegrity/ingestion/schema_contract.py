"""
ingestion/schema_contract.py
------------------------------
Schema contract enforcement — validates that a Dataset conforms to
a user-supplied column/type contract before downstream processing.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from dataintegrity.core.dataset import Dataset


# Mapping from human-readable type labels → callable predicates that
# test whether a pandas Series satisfies the type requirement.
_TYPE_CHECKERS: Dict[str, Any] = {
    "int": pd.api.types.is_integer_dtype,
    "integer": pd.api.types.is_integer_dtype,
    "float": pd.api.types.is_float_dtype,
    "numeric": pd.api.types.is_numeric_dtype,
    "str": pd.api.types.is_string_dtype,
    "string": pd.api.types.is_string_dtype,
    "object": pd.api.types.is_object_dtype,
    "bool": pd.api.types.is_bool_dtype,
    "boolean": pd.api.types.is_bool_dtype,
    "datetime": pd.api.types.is_datetime64_any_dtype,
    "date": pd.api.types.is_datetime64_any_dtype,
    "timestamp": pd.api.types.is_datetime64_any_dtype,
    "category": pd.api.types.is_categorical_dtype,
}


class SchemaViolationError(ValueError):
    """Raised when a Dataset's schema does not satisfy the contract."""

    def __init__(self, violations: List[str]) -> None:
        self.violations = violations
        bullet_list = "\n  • ".join(violations)
        super().__init__(
            f"Schema contract violated ({len(violations)} issue(s)):\n  • {bullet_list}"
        )


class SchemaContract:
    """
    Validates a :class:`~dataintegrity.core.dataset.Dataset` against a
    user-provided schema contract dictionary.

    The contract is a dict mapping column name → expected type string:

    .. code-block:: python

        contract = {
            "customer_id": "int",
            "email":       "string",
            "signup_date": "datetime",
            "score":       "float",
        }

    Type strings are matched case-insensitively against a built-in lookup
    table (see ``_TYPE_CHECKERS`` above).

    Args:
        contract:       Column → expected type mapping.
        strict:         If True, raise :class:`SchemaViolationError` on first
                        failure; otherwise collect all violations and raise once.
        allow_missing:  If True, columns present in the contract but absent
                        from the dataset are reported as violations but do not
                        block validation when ``strict=False``.
    """

    def __init__(
        self,
        contract: Dict[str, str],
        strict: bool = False,
        allow_missing: bool = False,
    ) -> None:
        self.contract = {k: v.lower() for k, v in contract.items()}
        self.strict = strict
        self.allow_missing = allow_missing

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(self, dataset: Dataset) -> Dict[str, Any]:
        """
        Check the dataset against the schema contract.

        Args:
            dataset: The dataset to validate.

        Returns:
            A report dict with keys ``"passed"``, ``"violations"``.

        Raises:
            SchemaViolationError: If any violations are found.
            KeyError:             If a type string is not recognised.
        """
        violations: List[str] = []
        df = dataset.df

        for col_name, expected_type in self.contract.items():
            # --- Check existence ---
            if col_name not in df.columns:
                msg = f"Column {col_name!r} is required but missing from dataset."
                if self.strict:
                    raise SchemaViolationError([msg])
                violations.append(msg)
                continue

            # --- Resolve type checker ---
            checker = _TYPE_CHECKERS.get(expected_type)
            if checker is None:
                raise KeyError(
                    f"Unknown type {expected_type!r} in contract. "
                    f"Valid types: {sorted(_TYPE_CHECKERS.keys())}"
                )

            # --- Check type compatibility ---
            series = df[col_name]
            if not checker(series):
                actual_dtype = str(series.dtype)
                msg = (
                    f"Column {col_name!r}: expected dtype compatible with "
                    f"{expected_type!r} but got {actual_dtype!r}."
                )
                if self.strict:
                    raise SchemaViolationError([msg])
                violations.append(msg)

        report: Dict[str, Any] = {
            "passed": len(violations) == 0,
            "violations": violations,
        }

        if violations:
            raise SchemaViolationError(violations)

        return report
