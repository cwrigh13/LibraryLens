#!/usr/bin/env python3
from __future__ import annotations
import html
import io
import os
import pathlib
import re
import sys
import time
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from typing import Iterable, List, Tuple, Dict, Any

USER_AGENT = "LibraryLens-Ingest/1.0 (+https://github.com/cwrigh13/LibraryLens)"
DEFAULT_SIZE_LIMIT_BYTES = 50 * 1024 * 1024  # 50MB

FILE_EXTENSIONS = {
    ".csv", ".tsv", ".xls", ".xlsx", ".zip", ".json", ".geojson", ".shp",
    ".gdb", ".xml", ".kml", ".kmz", ".pdf"
}


def slugify(text: str) -> str:
    text = (text or '').strip().lower()
    text = re.sub(r"[^a-z0-9]+", '-', text)
    return text.strip('-') or 'dataset'


def is_direct_file_url(url: str) -> bool:
    path = urllib.parse.urlparse(url).path.lower()
    ext = pathlib.Path(path).suffix
    return ext in FILE_EXTENSIONS


def fetch_html(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        content_type = resp.headers.get('Content-Type', '')
        data = resp.read()
        # attempt decode
        encoding = 'utf-8'
        m = re.search(r'charset=([\w-]+)', content_type)
        if m:
            encoding = m.group(1)
        try:
            return data.decode(encoding, errors='ignore')
        except Exception:
            return data.decode('utf-8', errors='ignore')


class LinkExtractor(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.links: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str]]):
        attrs_dict = dict(attrs)
        href = attrs_dict.get('href')
        if tag.lower() == 'a' and href:
            url = urllib.parse.urljoin(self.base_url, html.unescape(href))
            self.links.append(url)
        # arcgis hub often has data-download links in button data attributes; basic a/href extraction covers most


def extract_file_links(page_url: str, html_text: str) -> List[str]:
    parser = LinkExtractor(page_url)
    parser.feed(html_text)
    links = [u for u in parser.links if is_direct_file_url(u)]
    # Deduplicate, preserve order
    seen = set()
    dedup: List[str] = []
    for u in links:
        if u not in seen:
            seen.add(u)
            dedup.append(u)
    return dedup


def head_request(url: str) -> Dict[str, Any]:
    # emulate HEAD (not always supported) by GET without body read
    req = urllib.request.Request(url, method='GET', headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        headers = dict(resp.headers.items())
        # Do not read body to avoid download
        return headers


def download_with_limit(url: str, dest: pathlib.Path, size_limit: int = DEFAULT_SIZE_LIMIT_BYTES) -> Tuple[bool, str, int]:
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        headers = head_request(url)
    except Exception as e:
        headers = {}
    content_length = headers.get('Content-Length')
    if content_length and content_length.isdigit():
        size = int(content_length)
        if size > size_limit:
            return False, f"skipped: size {size} > limit {size_limit}", size
    # stream download
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    total = 0
    try:
        with urllib.request.urlopen(req, timeout=180) as resp, open(dest, 'wb') as f:
            while True:
                chunk = resp.read(1024 * 64)
                if not chunk:
                    break
                f.write(chunk)
                total += len(chunk)
                if total > size_limit:
                    try:
                        f.close()
                        dest.unlink(missing_ok=True)
                    except Exception:
                        pass
                    return False, f"skipped: streamed size > limit {size_limit}", total
    except Exception as e:
        return False, f"error: {e}", total
    return True, "ok", total