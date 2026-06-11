"""
scripts/sample.py
─────────────────
Extrait 1 000 000 lignes d'un CSV eCommerce par reservoir sampling
et les écrit en JSON Lines (.jsonl) pour le streaming Kafka.

Usage :
    python scripts/sample.py                         # défauts
    python scripts/sample.py --rows 500000
    python scripts/sample.py --source nov --rows 2000000
    python scripts/sample.py --source both
"""

import argparse
import csv
import json
import random
import sys
from pathlib import Path

from tqdm import tqdm

BASE_DIR = Path(__file__).resolve().parent.parent
ARCHIVE_DIR = BASE_DIR / "archive"
DATA_DIR = BASE_DIR / "data"

CSV_FILES = {
    "oct": ARCHIVE_DIR / "2019-Oct.csv",
    "nov": ARCHIVE_DIR / "2019-Nov.csv",
}


# ── Reservoir sampling (algorithme R) ──────────────────────────────────────

def reservoir_sample(path: Path, k: int, seed: int = 42) -> list[dict]:
    rng = random.Random(seed)
    reservoir: list[dict] = []
    total = sum(1 for _ in open(path, "rb")) - 1  # compte sans charger

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(tqdm(reader, total=total, desc=path.name, unit="row")):
            if i < k:
                reservoir.append(row)
            else:
                j = rng.randint(0, i)
                if j < k:
                    reservoir[j] = row

    return reservoir


# ── Normalisation ───────────────────────────────────────────────────────────

def normalize(row: dict) -> dict:
    def _int(v):  return int(v)   if v and v.strip() else None
    def _float(v): return float(v) if v and v.strip() else None
    def _str(v):  return v.strip() or None

    return {
        "event_time":    _str(row.get("event_time")),
        "event_type":    _str(row.get("event_type")),
        "product_id":    _int(row.get("product_id")),
        "category_id":   _int(row.get("category_id")),
        "category_code": _str(row.get("category_code")),
        "brand":         _str(row.get("brand")),
        "price":         _float(row.get("price")),
        "user_id":       _int(row.get("user_id")),
        "user_session":  _str(row.get("user_session")),
    }


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows",   type=int,  default=1_000_000)
    parser.add_argument("--source", choices=["oct", "nov", "both"], default="oct")
    parser.add_argument("--output", type=Path, default=DATA_DIR / "sample.jsonl")
    parser.add_argument("--seed",   type=int,  default=42)
    args = parser.parse_args()

    paths = list(CSV_FILES.values()) if args.source == "both" else [CSV_FILES[args.source]]
    missing = [p for p in paths if not p.exists()]
    if missing:
        print(f"❌  Fichier(s) introuvable(s) : {missing}", file=sys.stderr)
        sys.exit(1)

    records: list[dict] = []

    if len(paths) == 1:
        records = reservoir_sample(paths[0], args.rows, args.seed)
    else:
        # Répartition proportionnelle selon le nb de lignes de chaque fichier
        sizes = {p: sum(1 for _ in open(p, "rb")) - 1 for p in paths}
        total = sum(sizes.values())
        remaining = args.rows
        for idx, (p, n) in enumerate(sizes.items()):
            quota = args.rows if idx == len(paths) - 1 else round(args.rows * n / total)
            quota = min(quota, n, remaining)
            records += reservoir_sample(p, quota, args.seed + idx)
            remaining -= quota

    random.Random(args.seed).shuffle(records)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n✍️  Écriture → {args.output}")
    with open(args.output, "w", encoding="utf-8") as f:
        for row in tqdm(records, desc="jsonl", unit="row"):
            f.write(json.dumps(normalize(row), ensure_ascii=False) + "\n")

    size_mb = args.output.stat().st_size / 1024 / 1024
    print(f"✅  {len(records):,} lignes → {size_mb:.1f} MB")
    print("\nAperçu :")
    with open(args.output) as f:
        for _ in range(3):
            print(" ", f.readline().rstrip()[:120])


if __name__ == "__main__":
    main()
