# feast-feature-repo

Source of truth for the **Feast feature catalog** used by the MLOps lab. This
directory is mirrored to a dedicated Git repo on GitHub (`asafwat/feast-feature-repo`)
so that feature evolution is decoupled from the Feast Feature Server's image
build / deployment lifecycle.

## Contents

| File | Purpose |
|---|---|
| `feature_store.yaml` | Topology — registry / offline store / online store backends |
| `definitions.py` | Catalog — Entities, FileSources, FeatureViews (the things `feast apply` registers) |

The dedicated repo intentionally contains **only these two files** (plus this
README + a `.gitignore`). The Feast `data/` directory referenced by FileSource
is NOT in Git — that lives in MinIO at `s3://feast-offline/`, populated by
`generate_data.py` in the parent exercise directory.

## How changes flow to the running platform

```
edit definitions.py
        ↓ git push
GitHub webhook (or manual trigger)
        ↓
Airflow `feast_apply` DAG
        ↓ KubernetesPodOperator with gitSync initContainer
        ↓ runs `feast apply` against this repo
        ↓ writes new registry.db to s3://feast-offline/registry.db
all downstream consumers re-read registry on next request:
   - Feast Feature Server  (continuous gitSync sidecar)
   - Airflow materialize task
   - Airflow training task
   - KServe transformer pod (online feature lookup)
   - JupyterHub notebooks (data-scientist experimentation)
```

## Initial setup (first time only)

```bash
gh repo create asafwat/feast-feature-repo --public --description "Feast feature catalog for MLOps lab"
git -C feast-feature-repo init
git -C feast-feature-repo add .
git -C feast-feature-repo commit -m "initial feature catalog"
git -C feast-feature-repo branch -M main
git -C feast-feature-repo remote add origin https://github.com/asafwat/feast-feature-repo.git
git -C feast-feature-repo push -u origin main
```

After that, the Feast Helm chart's `gitSync.repo` value points at this repo,
and `helm upgrade feast charts/feast` is sufficient to wire it up.
