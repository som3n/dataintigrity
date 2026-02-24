from click.testing import CliRunner
from dataintegrity.cli import cli
from dataintegrity import __version__

def test_sdk_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output.strip() == f"dataintegrity {__version__}"

def test_cli_help_text():
    runner = CliRunner()
    result = runner.invoke(cli, ["audit", "--help"])
    assert result.exit_code == 0
    assert "--profile" in result.output
    assert "--policy" in result.output
    assert "iso-25012" in result.output
    assert "research" in result.output
    assert "production" in result.output
