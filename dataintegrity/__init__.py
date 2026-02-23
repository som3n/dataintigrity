"""
dataintegrity — Data Infrastructure SDK v0.2
"""

__version__ = "0.2.2"
__author__ = "dataintegrity"

from dataintegrity.core.dataset import Dataset
from dataintegrity.core.config import IntegrityConfig, DEFAULT_CONFIG

__all__ = ["Dataset", "IntegrityConfig", "DEFAULT_CONFIG", "__version__"]
