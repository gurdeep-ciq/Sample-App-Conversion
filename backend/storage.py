"""
storage.py — file storage abstraction (S3 in the cloud, local dir as fallback).

Uploaded files no longer live in process memory or a tempfile; they are written to
object storage keyed by content hash and re-read on demand, so the API is stateless.

  put_file(key, data)  -> store bytes
  get_file(key)        -> bytes (raises KeyError if missing)
  exists(key)          -> bool
  url(key)             -> a locator string (s3://… or file://…)

Backend is chosen by config: S3_BUCKET set -> boto3 S3; otherwise a local directory.
The interface is identical, so flipping to the cloud is config-only.
"""
from __future__ import annotations

import os

import config

_s3 = None


def _client():
    global _s3
    if _s3 is None:
        import boto3
        _s3 = boto3.client("s3", region_name=config.AWS_REGION)
    return _s3


def _local_path(key: str) -> str:
    p = os.path.join(config.LOCAL_FILESTORE, key)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    return p


def put_file(key: str, data: bytes) -> str:
    if config.storage_backend() == "s3":
        _client().put_object(Bucket=config.S3_BUCKET, Key=key, Body=data)
    else:
        with open(_local_path(key), "wb") as f:
            f.write(data)
    return url(key)


def get_file(key: str) -> bytes:
    if config.storage_backend() == "s3":
        try:
            return _client().get_object(Bucket=config.S3_BUCKET, Key=key)["Body"].read()
        except _client().exceptions.NoSuchKey as e:  # pragma: no cover
            raise KeyError(key) from e
    p = _local_path(key)
    if not os.path.exists(p):
        raise KeyError(key)
    with open(p, "rb") as f:
        return f.read()


def exists(key: str) -> bool:
    try:
        if config.storage_backend() == "s3":
            _client().head_object(Bucket=config.S3_BUCKET, Key=key)
            return True
        return os.path.exists(_local_path(key))
    except Exception:
        return False


def url(key: str) -> str:
    if config.storage_backend() == "s3":
        return f"s3://{config.S3_BUCKET}/{key}"
    return "file://" + _local_path(key)
