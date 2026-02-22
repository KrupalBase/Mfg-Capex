"""
Storage backend that abstracts local filesystem vs Google Cloud Storage.

When the GCS_BUCKET environment variable is set, all reads/writes go to that
GCS bucket.  Otherwise, everything falls back to the local ``data/`` directory
so local development is unchanged.
"""
from __future__ import annotations

import io
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

_GCS_BUCKET: str = os.environ.get("GCS_BUCKET", "")
_LOCAL_DATA_DIR: Path = Path(__file__).resolve().parent / "data"

# Lazy-initialised GCS client (only imported when needed)
_gcs_client = None
_gcs_bucket_obj = None


def _get_bucket():
    global _gcs_client, _gcs_bucket_obj
    if _gcs_bucket_obj is None:
        from google.cloud import storage as gcs
        _gcs_client = gcs.Client()
        _gcs_bucket_obj = _gcs_client.bucket(_GCS_BUCKET)
    return _gcs_bucket_obj


def is_remote() -> bool:
    return bool(_GCS_BUCKET)


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def read_csv(name: str) -> pd.DataFrame:
    """Read a CSV by filename (e.g. ``capex_clean.csv``)."""
    if is_remote():
        blob = _get_bucket().blob(name)
        if not blob.exists():
            return pd.DataFrame()
        content = blob.download_as_text(encoding="utf-8-sig")
        return pd.read_csv(io.StringIO(content), encoding="utf-8-sig").fillna("")
    else:
        path = _LOCAL_DATA_DIR / name
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path, encoding="utf-8-sig").fillna("")


def write_csv(name: str, df: pd.DataFrame) -> str:
    """Write a DataFrame as CSV. Returns the path or GCS URI written to."""
    if is_remote():
        blob = _get_bucket().blob(name)
        blob.upload_from_string(
            df.to_csv(index=False, encoding="utf-8-sig"),
            content_type="text/csv",
        )
        return f"gs://{_GCS_BUCKET}/{name}"
    else:
        _LOCAL_DATA_DIR.mkdir(exist_ok=True)
        path = _LOCAL_DATA_DIR / name
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return str(path)


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def read_json(name: str) -> Any:
    """Read a JSON file by name. Returns parsed object or empty dict/list."""
    if is_remote():
        blob = _get_bucket().blob(name)
        if not blob.exists():
            return {}
        content = blob.download_as_text(encoding="utf-8")
        return json.loads(content)
    else:
        path = _LOCAL_DATA_DIR / name
        if not path.exists():
            return {}
        with open(path, encoding="utf-8") as f:
            return json.load(f)


def write_json(name: str, data: Any) -> str:
    """Write a JSON-serialisable object. Returns path or GCS URI."""
    if is_remote():
        blob = _get_bucket().blob(name)
        blob.upload_from_string(
            json.dumps(data, indent=2),
            content_type="application/json",
        )
        return f"gs://{_GCS_BUCKET}/{name}"
    else:
        _LOCAL_DATA_DIR.mkdir(exist_ok=True)
        path = _LOCAL_DATA_DIR / name
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return str(path)


def file_exists(name: str) -> bool:
    if is_remote():
        return _get_bucket().blob(name).exists()
    else:
        return (_LOCAL_DATA_DIR / name).exists()


def local_data_dir() -> Path:
    """Return the local data directory (for pipeline steps that need local temp files)."""
    _LOCAL_DATA_DIR.mkdir(exist_ok=True)
    return _LOCAL_DATA_DIR
