#!/usr/bin/env python3
from __future__ import annotations
import csv
import json
import pathlib
import re
from typing import List, Dict

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
INVENTORY_CSV = REPO_ROOT / 'data' / 'inventory.csv'
README = REPO_ROOT / 'README.md'


def collect_inventory() -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for mpath in REPO_ROOT.rglob('data/raw/**/manifest.json'):
        try:
            m = json.loads(mpath.read_text(encoding='utf-8'))
        except Exception:
            continue
        base = mpath.parent
        for fname in m.get('saved_files', []) or []:
            fpath = base / fname
            rows.append({
                'dataset_name': m.get('dataset_name', ''),
                'source_url': m.get('source_url', ''),
                'manifest_path': str(mpath.relative_to(REPO_ROOT)),
                'file_path': str(fpath.relative_to(REPO_ROOT)),
            })
    rows.sort(key=lambda r: (r['dataset_name'], r['file_path']))
    return rows


def write_inventory_csv(rows: List[Dict[str, str]]) -> None:
    INVENTORY_CSV.parent.mkdir(parents=True, exist_ok=True)
    with INVENTORY_CSV.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['dataset_name','source_url','manifest_path','file_path'])
        w.writeheader()
        w.writerows(rows)


def refresh_readme_links(rows: List[Dict[str, str]]) -> None:
    # Prepare bullet list
    bullets = [f"- {r['dataset_name']} — [{r['file_path']}]({r['file_path']})" for r in rows]
    section_intro = [
        "## Data Inventory",
        "",
        "Below is an index of downloaded files discovered via the ingestion scripts. The table is generated from `data/inventory.csv`.",
        "",
        "- Inventory CSV: `data/inventory.csv`",
        "",
        "### Files",
        "",
    ]
    new_section = "\n".join(section_intro + bullets) + "\n"

    if not README.exists():
        # Create a minimal README with the section
        content = [
            "# LibraryLens",
            "",
            "A resource hub for Sydney public library strategic planning.",
            "",
            new_section,
        ]
        README.write_text("\n".join(content), encoding='utf-8')
        return

    lines = README.read_text(encoding='utf-8').splitlines()

    # Find existing section bounds
    start = None
    for i, line in enumerate(lines):
        if re.match(r'^##\s+Data Inventory\s*$', line.strip()):
            start = i
            break

    if start is None:
        # Append section at end
        content = "\n".join(lines + ["", new_section])
        README.write_text(content, encoding='utf-8')
        return

    # Find next ## heading after start to delimit the section
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if re.match(r'^##\s+[^#].*', lines[j].strip()):
            end = j
            break

    # Rebuild file with new section
    before = lines[:start]
    after = lines[end:]
    content = "\n".join(before + [new_section] + after)
    README.write_text(content, encoding='utf-8')


def main() -> int:
    rows = collect_inventory()
    write_inventory_csv(rows)
    refresh_readme_links(rows)
    print(f"Updated {INVENTORY_CSV} with {len(rows)} rows and refreshed README")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())