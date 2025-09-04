#!/usr/bin/env python3
from __future__ import annotations
import csv
import json
import pathlib
import sys
import urllib.parse
from typing import Dict, Any, List

from ingest_utils import fetch_html, extract_file_links, slugify, download_with_limit

SIZE_LIMIT = 50 * 1024 * 1024  # 50MB


def domain_from_url(url: str) -> str:
    return urllib.parse.urlparse(url).netloc.replace(':', '_')


def main() -> int:
    repo = pathlib.Path(__file__).resolve().parents[1]
    catalog = repo / 'data' / 'sydney_library_datasets_catalog.csv'
    out_root = repo / 'data' / 'raw'
    out_root.mkdir(parents=True, exist_ok=True)
    if not catalog.exists():
        print(f"Missing catalog: {catalog}")
        return 1

    with catalog.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total = 0
    downloaded = 0
    skipped = 0

    for row in rows:
        name = row.get('Dataset Name') or 'dataset'
        url = row.get('Direct Link') or ''
        if not url:
            continue
        dom = domain_from_url(url)
        slug = slugify(name)[:120]
        out_dir = out_root / dom / slug
        out_dir.mkdir(parents=True, exist_ok=True)
        manifest: Dict[str, Any] = {
            'dataset_name': name,
            'source_url': url,
            'domain': dom,
            'saved_files': [],
            'notes': [],
        }

        # Try straightforward: if the direct link is already a file, download it
        if url and any(url.lower().endswith(ext) for ext in ['.csv', '.tsv', '.xlsx', '.xls', '.zip', '.json', '.geojson']):
            filename = slug + pathlib.Path(urllib.parse.urlparse(url).path).suffix
            ok, msg, size = download_with_limit(url, out_dir / filename, SIZE_LIMIT)
            manifest['notes'].append(f"direct:{msg}")
            if ok:
                manifest['saved_files'].append(filename)
                downloaded += 1
                total += 1
                (out_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
                continue
            else:
                skipped += 1
                total += 1

        # Else: fetch the page and look for direct file links
        try:
            html_text = fetch_html(url)
            links = extract_file_links(url, html_text)
        except Exception as e:
            manifest['notes'].append(f"error: fetch page failed: {e}")
            (out_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
            total += 1
            continue

        if not links:
            manifest['notes'].append('no direct file links found on page')
        else:
            # Download up to first 3 files per dataset to avoid bloat
            for i, link in enumerate(links[:3]):
                suffix = pathlib.Path(urllib.parse.urlparse(link).path).suffix or ''
                filename = f"{slug}-{i+1}{suffix}"
                ok, msg, size = download_with_limit(link, out_dir / filename, SIZE_LIMIT)
                manifest['notes'].append(f"{link}: {msg}")
                if ok:
                    manifest['saved_files'].append(filename)
                    downloaded += 1
            total += 1

        (out_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')

    print(f"Processed {total} catalog entries. Files downloaded: {downloaded}. Skipped: {skipped}.")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())