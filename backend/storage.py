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


def delete_upload(file_id: str) -> int:
    """Delete every object for one upload (raw + meta). Returns the count removed.
    NOTE: on S3 this needs s3:DeleteObject permission on the bucket."""
    prefix = config.S3_PREFIX.rstrip("/") + "/" + file_id + "/"
    if config.storage_backend() == "s3":
        c = _client()
        objs = c.list_objects_v2(Bucket=config.S3_BUCKET, Prefix=prefix).get("Contents", [])
        if not objs:
            return 0
        resp = c.delete_objects(Bucket=config.S3_BUCKET,
                                Delete={"Objects": [{"Key": o["Key"]} for o in objs], "Quiet": False})
        errors = resp.get("Errors", [])
        if errors:  # S3 returns 200 even when each key is denied — surface it
            e0 = errors[0]
            raise PermissionError(f"{e0.get('Code')}: {e0.get('Message')} "
                                  "(the S3 IAM key needs s3:DeleteObject)")
        return len(resp.get("Deleted", []))
    import shutil
    d = os.path.join(config.LOCAL_FILESTORE, config.S3_PREFIX, file_id)
    if os.path.isdir(d):
        n = len(os.listdir(d))
        shutil.rmtree(d, ignore_errors=True)
        return n
    return 0


def list_uploads() -> list[dict]:
    """Every previously-uploaded file (so the UI can reuse one without re-uploading).
    Returns [{fileId, fileName, ext, size, uploadedAt}] newest first."""
    import datetime
    import json as _json
    prefix = config.S3_PREFIX.rstrip("/") + "/"
    out: dict[str, dict] = {}

    def slot(fid):
        return out.setdefault(fid, {"fileId": fid, "fileName": None, "ext": None,
                                    "size": 0, "uploadedAt": None})

    if config.storage_backend() == "s3":
        c = _client()
        token = None
        while True:
            kw = {"Bucket": config.S3_BUCKET, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            r = c.list_objects_v2(**kw)
            for o in r.get("Contents", []):
                rest = o["Key"][len(prefix):]
                if "/" not in rest:
                    continue
                fid, name = rest.split("/", 1)
                e = slot(fid)
                if name == "meta.json":
                    try:
                        m = _json.loads(get_file(o["Key"]))
                        e["fileName"], e["ext"] = m.get("filename"), m.get("ext")
                    except Exception:  # noqa: BLE001
                        pass
                elif name.startswith("raw"):
                    e["size"] = o["Size"]
                    e["uploadedAt"] = o["LastModified"].isoformat()
            if r.get("IsTruncated"):
                token = r.get("NextContinuationToken")
            else:
                break
    else:
        base = os.path.join(config.LOCAL_FILESTORE, config.S3_PREFIX)
        if os.path.isdir(base):
            for fid in os.listdir(base):
                d = os.path.join(base, fid)
                if not os.path.isdir(d):
                    continue
                e = slot(fid)
                for fn in os.listdir(d):
                    p = os.path.join(d, fn)
                    if fn == "meta.json":
                        try:
                            m = _json.load(open(p, encoding="utf-8"))
                            e["fileName"], e["ext"] = m.get("filename"), m.get("ext")
                        except Exception:  # noqa: BLE001
                            pass
                    elif fn.startswith("raw"):
                        e["size"] = os.path.getsize(p)
                        e["uploadedAt"] = datetime.datetime.fromtimestamp(os.path.getmtime(p)).isoformat()

    res = [v for v in out.values() if v.get("fileName")]
    res.sort(key=lambda x: x.get("uploadedAt") or "", reverse=True)
    return res
