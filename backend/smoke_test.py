"""
smoke_test.py — end-to-end check against a running backend.

Usage:
    python backend/smoke_test.py file1.xlsx [file2.csv ...]

For each file: POST /profile, then POST /ingest using the proposed union groups,
and print a one-line summary per table. Stdlib only (urllib).
"""
import json
import os
import sys
import urllib.request
import uuid

BASE = os.environ.get("PIPELINE_STUDIO_URL", "http://127.0.0.1:8000")


def post_multipart(url, filepath):
    boundary = "----ps" + uuid.uuid4().hex
    with open(filepath, "rb") as f:
        data = f.read()
    name = os.path.basename(filepath)
    body = (
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + data + f"\r\n--{boundary}--\r\n".encode()
    req = urllib.request.Request(url, data=body,
                                 headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    return json.load(urllib.request.urlopen(req, timeout=120))


def post_json(url, obj):
    req = urllib.request.Request(url, data=json.dumps(obj).encode(),
                                 headers={"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=300))


def main(paths):
    if not paths:
        print(__doc__)
        sys.exit(1)
    for path in paths:
        print("=" * 66)
        print(os.path.basename(path))
        prof = post_multipart(BASE + "/profile", path)
        data = [s for s in prof["sheets"] if s["kind"] == "data"]
        meta = [s for s in prof["sheets"] if s["kind"] == "metadata"]
        print(f"  sheets: {len(prof['sheets'])} (data={len(data)}, metadata-skipped={len(meta)})")
        for g in prof["unionGroups"]:
            print(f"     group {g['table']:14s} union={g['union']} members={g['members']}")
        plan = {"fileId": prof["fileId"],
                "tables": [{"table": g["table"], "members": g["members"], "overrides": {}}
                           for g in prof["unionGroups"]]}
        out = post_json(BASE + "/ingest", plan)
        for t in out["tables"]:
            sc = t["schemaChange"]
            print(f"  -> {t['table']:14s} dagster={t['dagsterRun']['success']} "
                  f"rows={t['rowCount']} cols={len(t['schema']['columns'])} "
                  f"known={len(sc.get('known', []))} new={len(sc.get('new', []))}")


if __name__ == "__main__":
    main(sys.argv[1:])
