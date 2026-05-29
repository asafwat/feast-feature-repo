"""
Feast feature definitions for the fraud-detection lab.

One Entity (`user_id`) + one FeatureView with 20 user-level features that map
1:1 to the synthetic columns produced by generate_data.py. The feature names
are realistic fraud-modeling features so the exercise reads like a real-world
setup; the underlying values are still synthetic floats from
sklearn.datasets.make_classification, just relabeled.

Source: a parquet file at s3://feast-offline/user_features.parquet (uploaded
to MinIO by generate_data.py and refreshed by the Airflow materialize DAG).
"""
from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.types import Float32

user = Entity(
    name="user",
    join_keys=["user_id"],
    description="A platform user — the entity all fraud features attach to",
)

user_features_source = FileSource(
    name="user_features_source",
    # Remote parquet on MinIO. Feast 0.50 (via fix #5076 shipped in 0.49)
    # correctly preserves s3:// URIs through path resolution. Credentials come
    # from AWS_* env vars (ESO-managed); the endpoint override below is
    # required because pyarrow's S3FileSystem (used internally by Feast for
    # FileSource s3:// reads) does NOT honor AWS_ENDPOINT_URL — it needs the
    # override passed in explicitly. Feast 0.50 exposes this via the
    # `s3_endpoint_override` FileSource param.
    path="s3://feast-offline/user_features.parquet",
    s3_endpoint_override="http://minio.mlops.svc.cluster.local:9000",
    timestamp_field="event_timestamp",
)

user_features = FeatureView(
    name="user_features",
    entities=[user],
    ttl=timedelta(days=365),
    schema=[
        # 30-day aggregates
        Field(name="user_age_days",                  dtype=Float32),
        Field(name="user_total_txn_count_30d",       dtype=Float32),
        Field(name="user_avg_txn_amount_30d",        dtype=Float32),
        Field(name="user_max_txn_amount_30d",        dtype=Float32),
        Field(name="user_std_txn_amount_30d",        dtype=Float32),
        Field(name="user_unique_merchants_30d",      dtype=Float32),
        Field(name="user_declined_txn_count_30d",    dtype=Float32),
        Field(name="user_high_risk_merchant_pct_30d", dtype=Float32),
        Field(name="user_intl_txn_count_30d",        dtype=Float32),
        Field(name="user_night_txn_count_30d",       dtype=Float32),
        # 7-day aggregates
        Field(name="user_avg_txn_amount_7d",         dtype=Float32),
        Field(name="user_max_txn_amount_7d",         dtype=Float32),
        Field(name="user_unique_merchants_7d",       dtype=Float32),
        Field(name="user_declined_txn_count_7d",     dtype=Float32),
        Field(name="user_night_txn_count_7d",        dtype=Float32),
        # 24h aggregates
        Field(name="user_avg_txn_amount_24h",        dtype=Float32),
        Field(name="user_max_txn_amount_24h",        dtype=Float32),
        Field(name="user_unique_merchants_24h",      dtype=Float32),
        Field(name="user_declined_txn_count_24h",    dtype=Float32),
        Field(name="user_high_value_txn_count_24h",  dtype=Float32),
    ],
    online=True,
    source=user_features_source,
    tags={"team": "fraud-detection", "owner": "ml-platform"},
)

# FeatureService — names a versioned bundle of features that downstream
# consumers (training jobs, KServe transformer, online lookup clients)
# request by name rather than by enumerating individual feature columns.
# This gives a stable contract between the feature catalog and consumers:
# adding a column to `user_features` is independent of which consumers want
# it; promoting a new model version means defining `fraud_detection_v2`
# without disturbing v1's signature.
#
# The KServe transformer in this exercise calls Feast's HTTP API with
#   {"feature_service": "fraud_detection_v1", "entities": {"user_id": [...]}}
# and gets back exactly the features below — same set the training task
# joins via get_historical_features(feature_service=fraud_detection_v1).
fraud_detection_v1 = FeatureService(
    name="fraud_detection_v1",
    features=[user_features],
    description="Fraud-detection model v1 — all user-level aggregates",
    tags={"team": "fraud-detection", "model_name": "fraud-detector-feast"},
)
