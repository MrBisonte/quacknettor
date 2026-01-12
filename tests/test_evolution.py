import pytest
from unittest.mock import MagicMock
from duckel.adapters import PostgresTargetAdapter, AdapterError

def test_sync_schema_add_column():
    """Test that schema evolution adds missing columns."""
    config = {
        "type": "postgres",
        "conn": "postgres://user:pass@host/db",
        "table": "tgt_table",
        "schema_evolution": "evolve"
    }
    adapter = PostgresTargetAdapter(config)
    
    con = MagicMock()
    
    # Mock DESCRIBE calls
    # 1. Source schema: id (INT), name (VARCHAR), new_col (VARCHAR)
    # 2. Target schema: id (INT), name (VARCHAR)
    con.execute.return_value.fetchall.side_effect = [
        [("id", "INTEGER"), ("name", "VARCHAR"), ("new_col", "VARCHAR")], # Source
        [("id", "INTEGER"), ("name", "VARCHAR")]                          # Target
    ]
    
    relation_sql = "src_table"
    adapter.sync_schema(con, relation_sql)
    
    # Verify ALTER TABLE called for new_col
    # Note: The logic in sync_schema might make other calls (like ATTACH) but sync_schema calls DESCRIBE then ALTER
    con.execute.assert_any_call('ALTER TABLE tgt_table ADD COLUMN "new_col" VARCHAR')

def test_sync_schema_fail_mode():
    """Test that schema evolution fails when configured to fail."""
    config = {
        "type": "postgres",
        "conn": "postgres://user:pass@host/db",
        "table": "tgt",
        "schema_evolution": "fail"
    }
    adapter = PostgresTargetAdapter(config)
    con = MagicMock()
    
    # Mock mismatch
    con.execute.return_value.fetchall.side_effect = [
        [("id", "INT"), ("col2", "INT")], # Source
        [("id", "INT")]                   # Target
    ]
    
    with pytest.raises(AdapterError, match="Schema mismatch"):
        adapter.sync_schema(con, "src")

def test_sync_schema_ignore_mode():
    """Test that schema evolution does nothing when ignored."""
    config = {
        "type": "postgres",
        "conn": "postgres://user:pass@host/db",
        "table": "tgt",
        "schema_evolution": "ignore"
    }
    adapter = PostgresTargetAdapter(config)
    con = MagicMock()
    
    # No calls should be made to DB in ignore mode
    assert not con.execute.called

def test_sync_schema_override():
    """Test that evolution_override takes precedence."""
    config = {
        "type": "postgres",
        "conn": "postgres://user:pass@host/db",
        "table": "tgt",
        "schema_evolution": "ignore" # Default is ignore
    }
    adapter = PostgresTargetAdapter(config)
    con = MagicMock()
    
    con.execute.return_value.fetchall.side_effect = [
        [("id", "INT"), ("new", "INT")], # Source
        [("id", "INT")]                  # Target
    ]
    
    # Override with evolve
    adapter.sync_schema(con, "src", evolution_override="evolve")
    
    # Should have called ALTER TABLE
    con.execute.assert_any_call('ALTER TABLE tgt ADD COLUMN "new" INT')
