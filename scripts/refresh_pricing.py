#!/usr/bin/env python3
"""Refresh IX pricing data from the peering.exposed Google Sheet.

Usage:
    python3 scripts/refresh_pricing.py

Writes updated data to src/peeringdb_mcp/ix_pricing.json.
Source: https://docs.google.com/spreadsheets/d/18ztPX_ysWYqEhJlf2SKQQsTNRbkwoxPSfaC6ScEZAG8
Maintained by Job Snijders, Will Hargrave, Kay Rechthien,
Massimiliano Stucchi, Paul Hoogsteder et al.
"""

from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path

try:
    import httpx
except ImportError:
    sys.exit("httpx is required: pip install httpx")

SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "18ztPX_ysWYqEhJlf2SKQQsTNRbkwoxPSfaC6ScEZAG8"
    "/export?format=csv&gid=0"
)

OUT_PATH = Path(__file__).parent.parent / "src" / "peeringdb_mcp" / "ix_pricing.json"


def _parse_price(val: str) -> float | None:
    v = val.strip()
    if v in ("", "-"):
        return None
    if "no public pricing" in v.lower():
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _parse_bool(val: str) -> bool | None:
    v = val.strip().lower()
    if v == "yes":
        return True
    if v in ("no", "insecure"):
        return False
    return None


def _parse_cent(val: str) -> float | None:
    v = val.strip()
    if v in ("", "-"):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def fetch_csv() -> str:
    print(f"Fetching {SHEET_URL} ...")
    with httpx.Client(follow_redirects=True, timeout=30) as client:
        resp = client.get(SHEET_URL)
    resp.raise_for_status()
    print(f"  Downloaded {len(resp.content):,} bytes")
    return resp.text


def parse(csv_text: str) -> list[dict]:
    rows = list(csv.reader(io.StringIO(csv_text)))

    # Rows 0-3 are header/metadata; data starts at row 4.
    data_rows = []
    for row in rows[4:]:
        ixp = row[1].strip() if len(row) > 1 else ""
        if not ixp:
            continue
        col5 = row[5].strip() if len(row) > 5 else ""
        # Skip footer note rows
        if (
            col5.startswith("1)")
            or col5 == "Notes:"
            or col5.startswith("Secure")
            or col5.startswith("A route")
        ):
            continue
        data_rows.append(row)

    entries = []
    for row in data_rows:
        while len(row) < 17:
            row.append("")
        no_public = (
            "no public pricing" in row[5].lower()
            or "no public pricing" in row[6].lower()
            or "no public pricing" in row[7].lower()
        )
        entries.append(
            {
                "ixp": row[1].strip(),
                "location": row[2].strip(),
                "secure_route_servers": _parse_bool(row[3]),
                "bcp214": _parse_bool(row[4]),
                "no_public_pricing": no_public,
                "price_400g_eur_month": _parse_price(row[5]),
                "price_100g_eur_month": _parse_price(row[6]),
                "price_10g_eur_month": _parse_price(row[7]),
                "cost_per_mbps_400g_85pct": _parse_cent(row[10]),
                "cost_per_mbps_400g_40pct": _parse_cent(row[11]),
                "cost_per_mbps_100g_85pct": _parse_cent(row[12]),
                "cost_per_mbps_100g_40pct": _parse_cent(row[13]),
                "cost_per_mbps_10g_85pct": _parse_cent(row[14]),
                "cost_per_mbps_10g_40pct": _parse_cent(row[15]),
                "notes": row[16].strip(),
            }
        )
    return entries


def main() -> None:
    csv_text = fetch_csv()
    entries = parse(csv_text)
    print(f"  Parsed {len(entries)} IXP entries")

    OUT_PATH.write_text(json.dumps(entries, indent=2) + "\n", encoding="utf-8")
    print(f"  Written to {OUT_PATH}")


if __name__ == "__main__":
    main()
