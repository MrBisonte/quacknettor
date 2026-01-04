import pytest
import os
from duckel.engine import resolve_env_tokens

def test_resolve_env_simple():
    """Test simple single variable resolution."""
    os.environ["TEST_VAR"] = "helloworld"
    assert resolve_env_tokens("__ENV:TEST_VAR") == "helloworld"

def test_resolve_env_embedded():
    """Test variable resolution embedded in a string."""
    os.environ["HOST"] = "localhost"
    assert resolve_env_tokens("host=__ENV:HOST port=5432") == "host=localhost port=5432"

def test_resolve_env_multiple():
    """Test multiple variables in one string."""
    os.environ["User"] = "admin" # Case sensitivity check if OS allows (Linux/Mac do usually)
    os.environ["Pass"] = "secret"
    input_str = "user=__ENV:User password=__ENV:Pass"
    expected = "user=admin password=secret"
    assert resolve_env_tokens(input_str) == "user=admin password=secret"

def test_resolve_missing():
    """Test resolution when env var is missing (should be empty string)."""
    if "MISSING_VAR" in os.environ:
        del os.environ["MISSING_VAR"]
    assert resolve_env_tokens("val=__ENV:MISSING_VAR") == "val="
