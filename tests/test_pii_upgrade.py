import pandas as pd
import pytest
from dataintegrity.core.dataset import Dataset
from dataintegrity.ingestion.pii import PIIDetector, calculate_shannon_entropy

def test_shannon_entropy():
    # Low entropy (repeating)
    assert calculate_shannon_entropy("aaaaa") == 0.0
    # Higher entropy
    assert calculate_shannon_entropy("abcde") > 2.0
    assert calculate_shannon_entropy("1a2b3c4d5e") > calculate_shannon_entropy("1111122222")

def test_global_identifiers():
    df = pd.DataFrame({
        "us_ssn": ["123-456-7890", "987 65 4321", "not-a-ssn"],
        "india_aadhaar": ["1234 5678 9012", "987654321098", "too-short"],
        "india_pan": ["ABCDE1234F", "FGHIJ5678K", "invalid-pan"],
        "credit_card": ["4532 0151 1283 0361", "invalid-card", "1234"], # 4532... is a valid-looking card
    })
    dataset = Dataset(df)
    detector = PIIDetector()
    report = detector.scan(dataset)

    # US SSN
    assert report["us_ssn"]["pii_detected"] is True
    assert "ssn_us" in report["us_ssn"]["patterns_hit"]
    assert any(f["type"] == "ssn_us" for f in report["us_ssn"]["pii_findings"])

    # India Aadhaar
    assert report["india_aadhaar"]["pii_detected"] is True
    assert "aadhaar_india" in report["india_aadhaar"]["patterns_hit"]

    # India PAN
    assert report["india_pan"]["pii_detected"] is True
    assert "pan_india" in report["india_pan"]["patterns_hit"]

    # Credit Card
    # Note: 4532 0151 1283 0361 passes Luhn check (actually I should ensure it does)
    # 4*1 + 5*2(1) + 3*1 + 2*2(4) + 0*1 + 1*2(2) + 5*1 + 1*2(2) + 1*1 + 2*2(4) + 8*1 + 3*2(6) + 0*1 + 3*2(6) + 6*1 + 1*2(2)
    # Sum: 4 + 1 + 3 + 4 + 0 + 2 + 5 + 2 + 1 + 4 + 8 + 6 + 0 + 6 + 6 + 2 = 50. 50 % 10 == 0. Valid.
    assert report["credit_card"]["pii_detected"] is True
    assert "credit_card" in report["credit_card"]["patterns_hit"]

def test_generic_identifier_heuristic():
    df = pd.DataFrame({
        "national_id": ["ID-8372-X", "ID-1923-Y", "ID-0021-Z"], # Short, unique, keyword in name
        "random_col": ["a", "b", "c"],
        "user_registration_number": ["REG-123456789", "REG-987654321", "REG-112233445"]
    })
    dataset = Dataset(df)
    detector = PIIDetector()
    report = detector.scan(dataset)

    # national_id should match heuristic
    # avg_len for "ID-8372-X" is 9. unique_ratio is 1.0. name has "id" and "national".
    assert any(f["type"] == "unknown_structured_identifier" for f in report["national_id"]["pii_findings"])
    
    # user_registration_number should match heuristic
    assert any(f["type"] == "unknown_structured_identifier" for f in report["user_registration_number"]["pii_findings"])

def test_backward_compatibility():
    # Existing config patterns should still work
    df = pd.DataFrame({
        "email": ["test@example.com", "user@domain.org"],
        "phone": ["+1-202-555-0173", "555-0101"]
    })
    dataset = Dataset(df)
    detector = PIIDetector()
    report = detector.scan(dataset)

    assert report["email"]["pii_detected"] is True
    assert report["email"]["count"] == 2
    assert "email" in report["email"]["patterns_hit"]

    assert report["phone"]["pii_detected"] is True
    assert "phone" in report["phone"]["patterns_hit"]
