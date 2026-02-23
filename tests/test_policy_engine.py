import pytest
import pandas as pd
from click.testing import CliRunner
from dataintegrity.cli import cli
from dataintegrity.policies.research import ResearchPolicy
from dataintegrity.policies.production import ProductionPolicy
from dataintegrity.core.dataset import Dataset
from dataintegrity.core.result_schema import DatasetAuditResult
from dataintegrity.core.execution import ExecutionManifest

@pytest.fixture
def mock_audit_result():
    manifest = ExecutionManifest.create(
        dataset_fingerprint="abc",
        config_hash="123",
        rules_executed=["completeness"],
        final_score=92.0,
        drift_checks_executed=[]
    )
    return {
        "overall_score": 92.0,
        "dimension_scores": {"completeness": 0.96},
        "drift_results": [],
        "standards_alignment": None
    }

def test_research_policy_pass(mock_audit_result):
    policy = ResearchPolicy()
    result = policy.evaluate(mock_audit_result)
    assert result["status"] == "PASS"
    assert len(result["violations"]) == 0

def test_research_policy_fail_score(mock_audit_result):
    mock_audit_result["overall_score"] = 85.0
    policy = ResearchPolicy()
    result = policy.evaluate(mock_audit_result)
    assert result["status"] == "FAIL"
    assert "DataScore" in result["violations"][0]

def test_research_policy_fail_completeness(mock_audit_result):
    mock_audit_result["dimension_scores"]["completeness"] = 0.90
    policy = ResearchPolicy()
    result = policy.evaluate(mock_audit_result)
    assert result["status"] == "FAIL"
    assert "Completeness" in result["violations"][0]

def test_research_policy_fail_drift(mock_audit_result):
    mock_audit_result["drift_results"] = [
        {"column": "a", "drift_detected": True},
        {"column": "b", "drift_detected": True},
        {"column": "c", "drift_detected": True},
    ]
    policy = ResearchPolicy()
    result = policy.evaluate(mock_audit_result)
    assert result["status"] == "FAIL"
    assert "Drifted columns" in result["violations"][0]

def test_production_policy_fail_stricter(mock_audit_result):
    # Research passes at 92, but Production fails (needs 95)
    policy = ProductionPolicy()
    result = policy.evaluate(mock_audit_result)
    assert result["status"] == "FAIL"
    assert "DataScore" in result["violations"][0]

def test_cli_policy_fail(tmp_path):
    csv_file = tmp_path / "fail.csv"
    # Create a CSV that will result in a low score
    # Low completeness: lots of NaNs
    df = pd.DataFrame({"a": [1, None, None, None], "b": [1, 2, 3, 4]})
    df.to_csv(csv_file, index=False)
    
    runner = CliRunner()
    # Research policy requires DataScore >= 90 and Completeness >= 0.95
    # This should fail.
    result = runner.invoke(cli, ["audit", str(csv_file), "--policy", "research"])
    assert result.exit_code == 1
    assert "POLICY EVALUATION (research)" in result.output
    assert "Status: FAIL" in result.output

def test_cli_policy_pass(tmp_path):
    csv_file = tmp_path / "pass.csv"
    # Healthy dataset
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
    df.to_csv(csv_file, index=False)
    
    runner = CliRunner()
    result = runner.invoke(cli, ["audit", str(csv_file), "--policy", "research"])
    # May still fail if default rules find issues, but for simple numeric data it should pass.
    # If it fails, it's likely due to other rules (validity, etc.) but let's check exit code.
    # For a clean dataset, Research should pass.
    assert result.exit_code == 0
    assert "Status: PASS" in result.output

def test_cli_json_output(tmp_path):
    csv_file = tmp_path / "test.csv"
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df.to_csv(csv_file, index=False)
    
    runner = CliRunner()
    result = runner.invoke(cli, ["audit", str(csv_file), "--policy", "research", "--output", "json"])
    assert result.exit_code == 0
    import json
    data = json.loads(result.output)
    assert "policy_evaluation" in data
    assert data["policy_evaluation"]["policy"] == "research"
