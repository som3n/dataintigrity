"""
integrity/plugins.py
--------------------
Rule plugin system for the dataintegrity integrity engine.

This module defines the abstract base class all integrity rules must implement
and provides a lightweight, thread-safe registry for discovering rules at runtime.

Usage
-----
Built-in rules (in :mod:`~dataintegrity.integrity.rules`) are registered
automatically when this module is imported. Third-party rules can call
:func:`register_rule` to add themselves::

    from dataintegrity.integrity.plugins import register_rule, IntegrityRule

    @register_rule
    class MyCustomRule(IntegrityRule):
        id = "my_rule"
        description = "Checks something important"
        severity = "HIGH"

        def evaluate(self, dataset, config):
            ...

Public API
----------
* :class:`IntegrityRule`       — ABC every rule must subclass.
* :func:`register_rule`        — decorator / function to register a rule class.
* :func:`get_registered_rules` — returns a snapshot of the current registry.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Type

from dataintegrity.core.config import IntegrityConfig
from dataintegrity.core.dataset import Dataset


# ---------------------------------------------------------------------------
# Forward-reference guard — RuleResult is in result_schema; import deferred
# to avoid circular imports.
# ---------------------------------------------------------------------------
# We import it locally inside IntegrityRule.evaluate to keep the ABC usable
# from within rules.py without triggering circular import chains.


# ---------------------------------------------------------------------------
# Plugin registry
# ---------------------------------------------------------------------------

_PLUGIN_REGISTRY: Dict[str, Type["IntegrityRule"]] = {}


def register_rule(rule_class: Type["IntegrityRule"]) -> Type["IntegrityRule"]:
    """
    Register a concrete :class:`IntegrityRule` subclass in the global plugin registry.

    Can be used as a class decorator or called directly::

        @register_rule
        class MyRule(IntegrityRule):
            ...

        # or equivalently:
        register_rule(MyRule)

    Args:
        rule_class: A concrete subclass of :class:`IntegrityRule` that defines
                    ``id``, ``description``, ``severity``, and ``evaluate``.

    Returns:
        The same class (so the function is usable as a decorator).

    Raises:
        TypeError:  If ``rule_class`` is not a subclass of :class:`IntegrityRule`.
        ValueError: If a rule with the same ``id`` is already registered.
    """
    if not (isinstance(rule_class, type) and issubclass(rule_class, IntegrityRule)):
        raise TypeError(
            f"register_rule expects an IntegrityRule subclass, got {rule_class!r}"
        )
    rule_id: str = rule_class.id  # type: ignore[attr-defined]
    if not rule_id:
        raise ValueError("IntegrityRule subclass must define a non-empty 'id' attribute.")
    if rule_id in _PLUGIN_REGISTRY:
        raise ValueError(
            f"A rule with id={rule_id!r} is already registered. "
            "Use a unique id or deregister the existing rule first."
        )
    _PLUGIN_REGISTRY[rule_id] = rule_class
    return rule_class


def deregister_rule(rule_id: str) -> None:
    """
    Remove a rule from the registry by its ID.

    This is primarily useful in tests to avoid state pollution.

    Args:
        rule_id: The ``id`` of the rule to remove.

    Raises:
        KeyError: If no rule with ``rule_id`` is currently registered.
    """
    if rule_id not in _PLUGIN_REGISTRY:
        raise KeyError(f"No rule with id={rule_id!r} found in registry.")
    del _PLUGIN_REGISTRY[rule_id]


def get_registered_rules() -> Dict[str, Type["IntegrityRule"]]:
    """
    Return a snapshot of all currently registered rule classes.

    Returns:
        A shallow copy of the plugin registry mapping rule ID → rule class.
    """
    return dict(_PLUGIN_REGISTRY)


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------

class IntegrityRule(ABC):
    """
    Abstract base class for all dataintegrity quality rules.

    Subclasses **must** define the following class-level attributes:

    Attributes:
        id:          Unique string identifier for this rule (e.g. ``"completeness"``).
        description: Human-readable description of what the rule checks.
        severity:    Risk level — must be one of ``"LOW"``, ``"MEDIUM"``, ``"HIGH"``.

    And must implement:

    * :meth:`evaluate` — perform the check and return a :class:`RuleResult`.
    """

    #: Unique rule identifier — override in subclass.
    id: str = ""

    #: Human-readable description — override in subclass.
    description: str = ""

    #: Risk severity — override in subclass.  One of LOW / MEDIUM / HIGH.
    severity: str = "MEDIUM"

    @abstractmethod
    def evaluate(
        self,
        dataset: Dataset,
        config: IntegrityConfig,
    ) -> "RuleResult":  # noqa: F821
        """
        Evaluate this rule against the given dataset.

        Args:
            dataset: The :class:`~dataintegrity.core.dataset.Dataset` to inspect.
            config:  The :class:`~dataintegrity.core.config.IntegrityConfig`
                     controlling thresholds and weights.

        Returns:
            A :class:`~dataintegrity.core.result_schema.RuleResult` containing the
            evaluation outcome.
        """
        ...
