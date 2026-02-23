import os
import json
import pytest
import pandas as pd
from dataintegrity.core.dataset import Dataset
from dataintegrity.core.config import IntegrityConfig
from dataintegrity.core.config_hashing import compute_config_hash
from dataintegrity.core.execution import ExecutionManifest
from dataintegrity.core.result_schema import DatasetAuditResult, RuleResult
from dataintegrity.integrity.engine import IntegrityEngine
from dataintegrity.integrity.risk_model import apply_risk_weight
from dataintegrity.integrity.history import IntegrityHistoryTracker

def test_config_hashing():
    config1 = IntegrityConfig()
    config2 = IntegrityConfig(drift_p_threshold=0.01)
    
    hash1 = compute_config_hash(config1)
    hash2 = compute_config_hash(config2)
    hash1_repo = compute_config_hash(IntegrityConfig())
    
    assert hash1 == hash1_repo
    assert hash1 != hash2
    assert len(hash1) == 64

def test_execution_manifest_creation():
    manifest = ExecutionManifest.create(
        dataset_fingerprint="abc",
        config_hash="123",
        rules_executed=["completeness"],
        final_score=95.0
    )
    
    assert manifest.dataset_fingerprint == "abc"
    assert manifest.config_hash == "123"
    assert manifest.rules_executed == ["completeness"]
    assert manifest.final_score == 95.0
    assert manifest.run_id is not None
    assert manifest.environment["os"] is not None

def test_rule_result_serialization():
    res = RuleResult(
        rule_id="test",
        description="desc",
        metric_value=0.9,
        threshold=0.5,
        passed=True,
        severity="HIGH"
    )
    d = res.to_dict()
    assert d["rule_id"] == "test"
    assert d["passed"] is True
    assert d["severity"] == "HIGH"

def test_risk_weighting():
    # HIGH severity failure: 0.4 / 2.0 = 0.2
    assert apply_risk_weight(0.4, "HIGH", False) == pytest.approx(0.2)
    # MEDIUM severity failure: 0.3 / 1.5 = 0.2
    assert apply_risk_weight(0.3, "MEDIUM", False) == pytest.approx(0.2)
    # LOW severity failure: 0.4 / 1.0 = 0.4
    assert apply_risk_weight(0.4, "LOW", False) == pytest.approx(0.4)
    # Passing rule: unchanged
    assert apply_risk_weight(0.4, "HIGH", True) == pytest.approx(0.4)

def test_engine_v021_return_type():
    df = pd.DataFrame({"a": [1, 2, None], "b": [1, 1, 1]})
    dataset = Dataset(df)
    engine = IntegrityEngine()
    result = engine.run(dataset)
    
    assert isinstance(result, DatasetAuditResult)
    assert result.overall_score > 0
    assert result.manifest.dataset_fingerprint == dataset.fingerprint
    
    # Backward compatibility check
    legacy = result.to_legacy_dict()
    assert "data_score" in legacy
    assert legacy["data_score"] == result.overall_score

def test_history_tracker(tmp_path):
    tracker = IntegrityHistoryTracker(storage_root=tmp_path)
    df = pd.DataFrame({"a": [1, 2, 3]})
    dataset = Dataset(df)
    engine = IntegrityEngine()
    result = engine.run(dataset)
    
    path = tracker.record(result)
    assert path.exists()
    
    history = tracker.load_history(dataset.fingerprint)
    assert len(history) == 1
    assert history[0]["overall_score"] == result.overall_score
    
    trend = tracker.get_score_trend(dataset.fingerprint)
    assert trend == [result.overall_score]
