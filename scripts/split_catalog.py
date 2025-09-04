#!/usr/bin/env python3
import csv
import pathlib
import re
import sys


def main() -> int:
    root = pathlib.Path(__file__).resolve().parents[1]
    catalog_path = root / 'data' / 'sydney_library_datasets_catalog.csv'
    out_dir = root / 'data' / 'catalog_datasets'
    out_dir.mkdir(parents=True, exist_ok=True)

    if not catalog_path.exists():
        print(f"Catalog not found: {catalog_path}", file=sys.stderr)
        return 1

    # Peek headers to ensure we retain their exact order
    with catalog_path.open(newline='', encoding='utf-8') as f:
        rdr = csv.reader(f)
        rows = list(rdr)
    if not rows:
        print("Catalog is empty", file=sys.stderr)
        return 1

    # Re-parse using DictReader to map rows to headers exactly
    with catalog_path.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = [h for h in (reader.fieldnames or []) if h is not None]

        def slugify(text: str) -> str:
            text = (text or '').strip().lower()
            text = re.sub(r'[^a-z0-9]+', '-', text)
            return text.strip('-') or 'dataset'

        seen: set[str] = set()
        count = 0
        for row in reader:
            # Filter only known fieldnames; drop None keys and extras
            clean_row = {k: (row.get(k, '') or '').strip() for k in fieldnames}
            name = clean_row.get('Dataset Name') or 'dataset'
            slug = slugify(name)[:120]
            base = slug
            i = 2
            while slug in seen:
                slug = f"{base}-{i}"
                i += 1
            seen.add(slug)

            out_path = out_dir / f"{slug}.csv"
            with out_path.open('w', newline='', encoding='utf-8') as out:
                writer = csv.DictWriter(out, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(clean_row)
            count += 1

    print(f"Wrote {count} dataset CSVs to {out_dir}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())