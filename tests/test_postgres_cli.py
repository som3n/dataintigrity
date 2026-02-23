from unittest.mock import MagicMock, patch
import pandas as pd
from click.testing import CliRunner
from dataintegrity.cli import cli

def test_audit_postgres_routing():
    """Verify that the CLI correctly routes to PostgresConnector when --dsn is provided."""
    runner = CliRunner()
    
    # Mocking the database interactions
    mock_df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    
    with patch("dataintegrity.connectors.postgres.PostgresConnector") as mock_conn_cls:
        mock_instance = mock_conn_cls.return_value
        mock_instance.fetch.return_value = mock_df
        
        # We also need to mock make_url since we use it in cli.py
        with patch("sqlalchemy.engine.make_url") as mock_make_url:
            mock_url = MagicMock()
            mock_url.host = "localhost"
            mock_url.port = 5432
            mock_url.database = "testdb"
            mock_url.username = "postgres"
            mock_url.password = "pass"
            mock_make_url.return_value = mock_url
            
            result = runner.invoke(cli, [
                "audit", 
                "--dsn", "postgresql://postgres:pass@localhost:5432/testdb", 
                "--table", "users"
            ])
            
            assert result.exit_code == 0
            assert "Connecting to Database" in result.output
            assert "Loaded 2 rows" in result.output
            mock_conn_cls.assert_called_once()
            # Verify the query was constructed correctly
            args, kwargs = mock_conn_cls.call_args
            assert kwargs["query"] == "SELECT * FROM users"

def test_audit_postgres_query_routing():
    """Verify that the CLI handles custom queries with --dsn."""
    runner = CliRunner()
    mock_df = pd.DataFrame({"a": [1]})
    
    with patch("dataintegrity.connectors.postgres.PostgresConnector") as mock_conn_cls:
        mock_instance = mock_conn_cls.return_value
        mock_instance.fetch.return_value = mock_df
        
        with patch("sqlalchemy.engine.make_url") as mock_make_url:
            mock_make_url.return_value = MagicMock()
            
            result = runner.invoke(cli, [
                "audit", 
                "--dsn", "postgresql://postgres:pass@localhost:5432/testdb", 
                "--query", "SELECT * FROM custom_table"
            ])
            
            assert result.exit_code == 0
            args, kwargs = mock_conn_cls.call_args
            assert kwargs["query"] == "SELECT * FROM custom_table"

def test_audit_exclusive_args():
    """Verify that FILEPATH and --dsn are mutually exclusive."""
    runner = CliRunner()
    
    # Both provided
    result = runner.invoke(cli, ["audit", "sample.csv", "--dsn", "postgresql://..."])
    assert result.exit_code != 0
    assert "Cannot provide both FILEPATH and --dsn" in result.output
    
    # Neither provided
    result = runner.invoke(cli, ["audit"])
    assert result.exit_code != 0
    assert "Either FILEPATH or --dsn must be provided" in result.output
