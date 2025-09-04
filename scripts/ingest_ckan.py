#!/usr/bin/env python3
"""
Download public resources from NSW Data Portal (CKAN) datasets.

Usage:
  python3 scripts/ingest_ckan.py <dataset_url_or_slug> [<dataset_url_or_slug> ...]

Notes:
- Saves files under data/raw/nsw/<dataset-slug>/
- Writes a manifest.json summarising resources and download status
- Skips resources requiring auth or inaccessible
"""
from __future__ import annotations
import json
import os
import pathlib
import re
import sys
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List

CKAN_API_BASE = "https://data.nsw.gov.au/data/api/3/action"
USER_AGENT = "LibraryLens-Ingest/1.0 (+https://github.com/cwrigh13/LibraryLens)"


def request_json(url: str, params: Dict[str, str] | None = None) -> Dict[str, Any]:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read()
        return json.loads(data.decode("utf-8"))


def download_file(url: str, dest: pathlib.Path) -> str:
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=180) as resp, open(dest, "wb") as f:
        chunk = resp.read()
        f.write(chunk)
    return str(dest)


def slug_from_url(dataset_url_or_slug: str) -> str:
    # Accept either a CKAN dataset slug or a full URL like https://data.nsw.gov.au/data/dataset/<slug>
    parsed = urllib.parse.urlparse(dataset_url_or_slug)
    if not parsed.scheme:
        return dataset_url_or_slug.strip()
    if "data.nsw.gov.au" not in parsed.netloc:
        raise ValueError("Not a NSW Data Portal URL: " + dataset_url_or_slug)
    parts = [p for p in parsed.path.split("/") if p]
    # Expect .../data/dataset/<slug>
    if "dataset" in parts:
        i = parts.index("dataset")
        if i + 1 < len(parts):
            return parts[i + 1]
    # Fallback to last segment
    return parts[-1]


def ckan_package_show(slug: str) -> Dict[str, Any]:
    url = f"{CKAN_API_BASE}/package_show"
    data = request_json(url, {"id": slug})
    if not data.get("success"):
        raise RuntimeError(f"CKAN package_show failed for {slug}: {data}")
    return data["result"]


def safe_filename(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9._-]+", "-", name)
    name = name.strip("-._")
    return name or "file"


def ingest_dataset(root: pathlib.Path, dataset_url_or_slug: str) -> Dict[str, Any]:
    slug = slug_from_url(dataset_url_or_slug)
    pkg = ckan_package_show(slug)
    out_dir = root / "data" / "raw" / "nsw" / slug
    manifest: Dict[str, Any] = {
        "dataset": slug,
        "title": pkg.get("title"),
        "notes": pkg.get("notes"),
        "resources": [],
    }
    for res in pkg.get("resources", []):
        res_url = res.get("url")
        name = res.get("name") or res.get("id") or "resource"
        fmt = (res.get("format") or "").lower()
        rid = res.get("id")
        status: Dict[str, Any] = {
            "id": rid,
            "name": name,
            "format": fmt,
            "url": res_url,
            "saved_as": None,
            "ok": False,
            "error": None,
        }
        if not res_url:
            status["error"] = "missing url"
            manifest["resources"].append(status)
            continue
        # Build filename
        base = safe_filename(name)
        ext = ""
        # Try to infer extension
        parsed = urllib.parse.urlparse(res_url)
        path_ext = pathlib.Path(parsed.path).suffix
        if path_ext:
            ext = path_ext
        elif fmt:
            ext = "." + fmt if not fmt.startswith(".") else fmt
        filename = base + (ext or "")
        dest = out_dir / filename
        try:
            saved = download_file(res_url, dest)
            status["saved_as"] = saved
            status["ok"] = True
        except Exception as e:
            status["error"] = str(e)
        manifest["resources"].append(status)
        time.sleep(0.3)
    # Write manifest
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    ok = 0
    for arg in argv[1:]:
        try:
            m = ingest_dataset(repo_root, arg)
            print(f"Ingested {arg}: {len(m.get('resources', []))} resources")
            ok += 1
        except Exception as e:
            print(f"ERROR ingesting {arg}: {e}", file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))