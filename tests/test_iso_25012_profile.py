import pytest
import pandas as pd
from click.testing import CliRunner
from dataintegrity.cli import cli
from dataintegrity.integrity.engine import IntegrityEngine
from dataintegrity.core.dataset import Dataset
from dataintegrity.standards.iso_25012 import evaluate_iso_25012_alignment

def test_iso_25012_status_logic():
    """Verify the score-to-status mapping logic for ISO alignment."""
    # Pass >= 0.95
    res = evaluate_iso_25012_alignment({"completeness": 0.96})
    assert res["Completeness"]["status"] == "PASS"
    
    # Minor Issues >= 0.85
    res = evaluate_iso_25012_alignment({"completeness": 0.90})
    assert res["Completeness"]["status"] == "MINOR_ISSUES"
    
    # Moderate Issues >= 0.70
    res = evaluate_iso_25012_alignment({"completeness": 0.75})
    assert res["Completeness"]["status"] == "MODERATE_ISSUES"
    
    # Critical < 0.70
    res = evaluate_iso_25012_alignment({"completeness": 0.50})
    assert res["Completeness"]["status"] == "CRITICAL"

def test_engine_iso_profile_integration():
    """Verify that the engine correctly computes and attaches ISO alignment data."""
    df = pd.DataFrame({"a": [1, 2], "b": [None, 4]}) # some missing values
    dataset = Dataset(df)
    engine = IntegrityEngine()
    
    # Run with profile
    result = engine.run(dataset, profile="iso-25012")
    
    assert result.standards_alignment is not None
    assert result.standards_alignment["profile"] == "ISO/IEC 25012"
    assert "Completeness" in result.standards_alignment["characteristics"]
    assert "Accuracy" in result.standards_alignment["characteristics"]

def test_cli_iso_profile_rendering():
    """Smoke test for CLI rendering of the ISO alignment section."""
    runner = CliRunner()
    # Create a small dummy CSV
    with runner.isolated_filesystem():
        with open("test.csv", "w") as f:
            f.write("id,name\n1,user\n2,")
        
        result = runner.invoke(cli, ["audit", "test.csv", "--profile", "iso-25012"])
        
        assert result.exit_code == 0
        assert "ISO/IEC 25012 ALIGNMENT" in result.output
        assert "Completeness" in result.output
        assert "Accuracy" in result.output

def test_cli_iso_profile_json():
    """Verify ISO alignment data is present in JSON output."""
    import json
    runner = CliRunner()
    with runner.isolated_filesystem():
        with open("test.csv", "w") as f:
            f.write("id,name\n1,user\n2,")
        
        result = runner.invoke(cli, ["audit", "test.csv", "--profile", "iso-25012", "--output", "json"])
        
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "standards_alignment" in data
        assert data["standards_alignment"]["profile"] == "ISO/IEC 25012"
