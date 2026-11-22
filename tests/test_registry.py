"""
The names a configuration may use are the names that have an implementation.

Sources and targets are open sets. Each has one registry in `duckel/adapters.py`, and the
configuration models validate `type` against its keys. One list, not two.
"""

from typing import get_args

import pytest
from pydantic import ValidationError

from duckel.adapters import SOURCES, TARGETS, create_source_adapter, create_target_adapter
from duckel.models import SourceConfig, TargetConfig

# Every field an adapter may ask for, so that each registered name can be built.
ANY_CONFIG = {"path": "./x", "conn": "host=localhost", "object": "t", "table": "t"}


def validated(model) -> set:
    """The names a configuration model accepts for `type`."""
    return set(get_args(model.model_fields["type"].annotation))


def test_the_validated_sources_are_the_registry():
    assert validated(SourceConfig) == set(SOURCES)


def test_the_validated_targets_are_the_registry():
    assert validated(TargetConfig) == set(TARGETS)


@pytest.mark.parametrize("name", sorted(SOURCES))
def test_each_registered_source_builds_its_adapter(name):
    assert isinstance(create_source_adapter({"type": name, **ANY_CONFIG}), SOURCES[name])


@pytest.mark.parametrize("name", sorted(TARGETS))
def test_each_registered_target_builds_its_adapter(name):
    assert isinstance(create_target_adapter({"type": name, **ANY_CONFIG}), TARGETS[name])


def test_a_source_with_no_adapter_is_refused_when_the_configuration_loads():
    """`duckdb` validated as a source type and had no adapter, so the run failed instead."""
    with pytest.raises(ValidationError, match="duckdb"):
        SourceConfig(type="duckdb", path="./x.duckdb")
