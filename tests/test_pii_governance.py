import pandas as pd
import pytest
import os
import yaml
from dataintegrity.core.dataset import Dataset
from dataintegrity.ingestion.pii import PIIDetector
from dataintegrity.policies.file_policy import FilePolicy

def test_priority_resolution():
    # Value that matches both credit card (priority 1) and Aadhaar (priority 2)
    # 4532 0151 1283 0366 is a valid CC and also matches Aadhaar regex (12 digits with spaces)
    df = pd.DataFrame({
        "mixed_col": ["4532 0151 1283 0366", "1212 3434 5656"] 
    })
    dataset = Dataset(df)
    detector = PIIDetector()
    report = detector.scan(dataset)
    
    col_findings = report["mixed_col"]["pii_findings"]
    # Credit Card should win for the first row
    assert any(f["dominant_type"] == "credit_card" for f in col_findings)
    assert any(f["dominant_type"] == "aadhaar_india" for f in col_findings)

def test_false_positive_guardrails():
    df = pd.DataFrame({
        "noisy_col": ["123456789", "000000000", "9876543210"], # Should be filtered
        "valid_ssn": ["453-20-1511", "987-65-4321", "000-11-2222"] # low count
    })
    dataset = Dataset(df)
    detector = PIIDetector()
    report = detector.scan(dataset)
    
    # noisy_col should have no findings
    assert "noisy_col" not in report
    
    # valid_ssn should be detected
    assert report["valid_ssn"]["pii_detected"] is True

def test_governance_policy_enforcement(tmp_path):
    # 1. High risk block
    # Use NON-SEQUENTIAL SSN to bypass guards
    # Wait, my logic says 123-45-6789 is noisy because 123456789 is sequential.
    df = pd.DataFrame({"ssn": ["453-20-1511"] * 10})
    dataset = Dataset(df)
    detector = PIIDetector()
    pii_results = detector.scan(dataset)
    
    audit_data = {
        "dimension_scores": {"completeness": 1.0},
        "pii_summary": pii_results["pii_summary"]
    }
    audit_data.update({k: v for k, v in pii_results.items() if k != "pii_summary"})

    policy_file = tmp_path / "high_risk_block.yaml"
    policy_file.write_text(yaml.dump({
        "version": 1,
        "policy": {
            "pii": {
                "block_high_risk": True
            }
        }
    }))
    
    pol = FilePolicy(str(policy_file))
    eval_res = pol.evaluate(audit_data)
    
    assert eval_res["passed"] is False
    assert eval_res["pii_violation"] is True
    assert "High-risk PII columns detected" in str(eval_res["violations"])

def test_medium_risk_ratio_threshold(tmp_path):
    # ID must be 8-20 chars for heuristic. 
    # Must avoid segments matching Passport regex \b[A-Z0-9]{6,9}\b.
    # "ID.X.Y.Z.001" segments are all < 6 chars. Total length = 12.
    df = pd.DataFrame({"national_id": [f"ID.X.Y.Z.{i:03}" for i in range(100)]})
    dataset = Dataset(df)
    detector = PIIDetector()
    pii_results = detector.scan(dataset)
    
    # Simulate DatasetAuditResult.to_dict() flat list
    flat_findings = []
    for k, v in pii_results.items():
        if k != "pii_summary" and isinstance(v, dict):
            flat_findings.extend(v.get("pii_findings", []))

    audit_data = {
        "dimension_scores": {"completeness": 1.0},
        "pii_summary": pii_results["pii_summary"],
        "pii_findings": flat_findings
    }

    policy_file = tmp_path / "ratio_threshold.yaml"
    policy_file.write_text(yaml.dump({
        "version": 1,
        "policy": {
            "pii": {
                "max_medium_risk_ratio": 0.5
            }
        }
    }))
    
    pol = FilePolicy(str(policy_file))
    eval_res = pol.evaluate(audit_data)
    
    # Ensure it hit the heuristic (medium risk)
    assert any(f["dominant_type"] == "unknown_structured_identifier" for f in flat_findings)
    
    assert eval_res["passed"] is False
    assert "PII Policy Violation: Column 'national_id' medium-risk ratio" in str(eval_res["violations"])

def test_deterministic_sampling():
    # Create 150,000 rows
    df = pd.DataFrame({
        "id": [f"ID-{i}" for i in range(150000)],
        "pii": ["test@example.com"] * 150000
    })
    dataset = Dataset(df)
    detector = PIIDetector()
    
    # Scan with low threshold
    report = detector.scan(dataset, sample_threshold=50000)
    
    assert report["pii_summary"]["is_sampled"] is True
    assert report["pii_summary"]["sample_size"] == 50000
    # Match ratio should be calculated from sample (1.0)
    assert report["pii"]["pii_findings"][0]["match_ratio"] == 1.0
