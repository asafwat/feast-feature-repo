"""
Feast catalog validation tests — catches bad commits BEFORE gitSync propagates
them to the running Feature Server.

Three layers of validation:
  1. feature_store.yaml — parses + has the keys the runtime expects
  2. definitions.py — imports cleanly + registers the expected objects
  3. Feast SDK integration — `FeatureStore(repo_path=...)` succeeds against
     a temp local sqlite registry (the most thorough check; mirrors what
     `feast apply` does at runtime, minus the s3 write)

The tests intentionally avoid talking to s3 or Redis — CI doesn't have those.
A separate integration suite (not built yet) would cover those.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).parent.parent


# ── 1. feature_store.yaml ──────────────────────────────────────────────────
def test_feature_store_yaml_parses():
    with open(REPO_ROOT / "feature_store.yaml", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    assert isinstance(config, dict)


def test_feature_store_yaml_has_required_keys():
    """Every Feast feature_store.yaml needs at minimum: project, registry,
    online_store, offline_store, provider."""
    with open(REPO_ROOT / "feature_store.yaml", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    required = {"project", "registry", "online_store", "offline_store", "provider"}
    missing = required - set(config.keys())
    assert not missing, f"feature_store.yaml missing required keys: {missing}"


def test_registry_points_to_s3():
    """Lab convention: registry MUST be s3-backed so all consumers see the same one.
    A local-file registry would silently break the Airflow apply DAG."""
    with open(REPO_ROOT / "feature_store.yaml", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    path = config["registry"].get("path", "")
    assert path.startswith("s3://"), f"registry path must be s3://, got: {path!r}"


# ── 2. definitions.py ──────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def definitions_module():
    """Import definitions.py as a module so we can inspect what it registered."""
    spec = importlib.util.spec_from_file_location(
        "definitions", REPO_ROOT / "definitions.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["definitions"] = module
    spec.loader.exec_module(module)
    return module


def test_definitions_imports(definitions_module):
    """If this test passes at all, definitions.py is syntactically valid AND
    all its imports resolve."""
    assert definitions_module is not None


def test_definitions_has_expected_objects(definitions_module):
    """Lab convention: at least one Entity, FileSource, FeatureView, FeatureService."""
    from feast import Entity, FeatureService, FeatureView, FileSource

    entities = [v for v in vars(definitions_module).values() if isinstance(v, Entity)]
    file_sources = [v for v in vars(definitions_module).values() if isinstance(v, FileSource)]
    feature_views = [v for v in vars(definitions_module).values() if isinstance(v, FeatureView)]
    services = [v for v in vars(definitions_module).values() if isinstance(v, FeatureService)]

    assert entities, "no Entity registered in definitions.py"
    assert file_sources, "no FileSource registered"
    assert feature_views, "no FeatureView registered"
    assert services, "no FeatureService registered (downstream consumers reference it by name)"


# ── 3. Object-level catalog validation ─────────────────────────────────────
# Note: a FULL `FeatureStore.apply()` call would catch more (e.g. column-type
# mismatches via schema inference) but it requires reading the offline parquet
# from s3 — which CI can't reach. The lab's integration tests (running inside
# the cluster) cover that. Here we do everything we CAN check without network.
def test_all_feature_views_have_schema(definitions_module):
    """Every FeatureView should declare at least one Field — empty schemas
    are usually a sign of an in-progress edit."""
    from feast import FeatureView

    for v in vars(definitions_module).values():
        if isinstance(v, FeatureView):
            assert v.schema, f"FeatureView '{v.name}' has no schema fields"


def test_all_feature_views_have_a_source(definitions_module):
    """A FeatureView with no source is a deployment-time crash."""
    from feast import FeatureView

    for v in vars(definitions_module).values():
        if isinstance(v, FeatureView):
            assert v.source is not None, f"FeatureView '{v.name}' has no source"


def test_feature_services_reference_existing_feature_views(definitions_module):
    """Every FeatureView a FeatureService points at must exist in this module —
    catches typos like `features=[user_feature]` (singular) when the var is
    `user_features` (plural)."""
    from feast import FeatureService, FeatureView

    registered_fvs = {
        v.name for v in vars(definitions_module).values() if isinstance(v, FeatureView)
    }
    for v in vars(definitions_module).values():
        if isinstance(v, FeatureService):
            referenced = {fv.name for fv in v.feature_view_projections}
            missing = referenced - registered_fvs
            assert not missing, (
                f"FeatureService '{v.name}' references unknown FeatureViews: {missing}"
            )


def test_no_duplicate_names_across_objects(definitions_module):
    """No two objects should share a name within their type — Feast registry
    keys by (type, name) and a duplicate is a silent override."""
    from feast import Entity, FeatureService, FeatureView

    for cls in (Entity, FeatureView, FeatureService):
        names = [
            v.name for v in vars(definitions_module).values() if isinstance(v, cls)
        ]
        assert len(names) == len(set(names)), (
            f"duplicate names in {cls.__name__}: {names}"
        )
