import json
import yaml
import pytest
from click.testing import CliRunner
from dataintegrity.cli import cli
from pathlib import Path

def test_v030_output_schema_and_fingerprint(tmp_path):
    # Create a small dummy CSV
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("a,b\n1,2\n3,4", encoding="utf-8")
    
    runner = CliRunner()
    result = runner.invoke(cli, ["audit", str(csv_file), "--output", "json"])
    
    assert result.exit_code == 0
    data = json.loads(result.output)
    
    # 1. Verify schema version
    assert data["schema_version"] == "0.3"
    
    # 2. Verify fingerprint structure
    assert "fingerprint" in data
    fp = data["fingerprint"]
    assert "structural" in fp
    assert "statistical" in fp
    assert "row_count" in fp
    assert fp["row_count"] == 2
    assert "combined" in fp

def test_v030_policy_file_pass(tmp_path):
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("a,b\n1,2\n3,4", encoding="utf-8")
    
    policy_file = tmp_path / "policy.yaml"
    policy_content = {
        "version": 1,
        "policy": {
            "completeness": 0.5,
            "uniqueness": 0.5
        }
    }
    policy_file.write_text(yaml.dump(policy_content))
    
    runner = CliRunner()
    result = runner.invoke(cli, ["audit", str(csv_file), "--policy-file", str(policy_file), "--output", "json"])
    
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["policy_evaluation"]["status"] == "PASS"
    assert data["policy_evaluation"]["type"] == "file"

def test_v030_policy_file_fail(tmp_path):
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("a,a\n1,2\n1,2", encoding="utf-8") # Duplicates
    
    policy_file = tmp_path / "policy.yaml"
    policy_content = {
        "version": 1,
        "policy": {
            "uniqueness": 1.0 # Will fail due to duplicates
        }
    }
    policy_file.write_text(yaml.dump(policy_content))
    
    runner = CliRunner()
    result = runner.invoke(cli, ["audit", str(csv_file), "--policy-file", str(policy_file), "--output", "json"])
    
    # Implementation should exit 1 on policy fail
    assert result.exit_code == 1
    data = json.loads(result.output)
    assert data["policy_evaluation"]["status"] == "FAIL"
    assert len(data["policy_evaluation"]["violations"]) > 0

def test_v030_policy_file_invalid_yaml(tmp_path):
    policy_file = tmp_path / "bad.yaml"
    policy_file.write_text("invalid: [yaml: content")
    
    runner = CliRunner()
    result = runner.invoke(cli, ["audit", "sample.csv", "--policy-file", str(policy_file)])
    
    assert result.exit_code == 1
    assert "Policy error" in result.output
