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


# ── 3. Full FeatureStore() construction with a temp local registry ─────────
def test_feature_store_loads_with_local_registry(tmp_path, definitions_module):
    """Build a temp feature_store.yaml pointing at a sqlite local registry,
    then run `FeatureStore.apply([...])` — same code path Feast's CLI takes.
    Catches type errors, name collisions, missing source columns, etc."""
    from feast import FeatureStore

    # Sync the catalog objects from definitions.py into a temp feature_store.yaml.
    repo_dir = tmp_path / "feast_repo"
    repo_dir.mkdir()
    (repo_dir / "feature_store.yaml").write_text(
        "project: ci_test\n"
        "provider: local\n"
        "registry: \n"
        f"  registry_type: file\n"
        f"  path: {(repo_dir / 'registry.db').as_posix()}\n"
        "offline_store:\n"
        "  type: file\n"
        "online_store:\n"
        "  type: sqlite\n"
        f"  path: {(repo_dir / 'online.db').as_posix()}\n"
        "entity_key_serialization_version: 3\n"
    )

    fs = FeatureStore(repo_path=str(repo_dir))
    # Collect all the Feast objects registered in definitions.py.
    from feast import Entity, FeatureService, FeatureView

    objects = []
    for v in vars(definitions_module).values():
        if isinstance(v, Entity | FeatureView | FeatureService):
            objects.append(v)

    # `apply` is the same call `feast apply` makes. If anything's wrong with
    # the catalog (bad types, name clashes, undefined entities), it raises here.
    # `skip_source_validation=True` because the FileSource points at s3 — we
    # can't actually fetch from s3 in CI, but the schema-level checks still run.
    fs.apply(objects)
