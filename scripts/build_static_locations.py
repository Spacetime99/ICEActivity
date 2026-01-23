"""
Helper script to regenerate the frontend static datasets from the CSV sources.
Usage:
    source .venv/bin/activate
    python3 scripts/build_static_locations.py
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
FIELD_OFFICES_CSV = REPO / "assets" / "ice_facilities.csv"
DETENTION_CSV = REPO / "ice_detention_facilities.csv"
FIELD_TS = REPO / "frontend" / "src" / "fieldOffices.ts"
DETENTION_TS = REPO / "frontend" / "src" / "detentionFacilities.ts"


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def sanitize(value: str | None) -> str:
    return (value or "").strip()


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def write_ts(path: Path, header: str, data_name: str, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(rows, indent=2, ensure_ascii=False)
    content = f"{header}\n\nexport const {data_name} = {serialized} as const;\n"
    path.write_text(content, encoding="utf-8")


def build_field_offices() -> int:
    output: list[dict[str, Any]] = []
    for row in load_csv(FIELD_OFFICES_CSV):
        lat = parse_float(row.get("latitude"))
        lon = parse_float(row.get("longitude"))
        if lat is None or lon is None:
            continue
        output.append(
            {
                "name": sanitize(row.get("name")),
                "city": sanitize(row.get("city")),
                "state": sanitize(row.get("state")),
                "latitude": lat,
                "longitude": lon,
            }
        )
    output.sort(key=lambda r: (r["state"], r["city"], r["name"]))
    header = """export type FieldOffice = {
  name: string;
  city: string;
  state: string;
  latitude: number;
  longitude: number;
};"""
    write_ts(FIELD_TS, header, "FIELD_OFFICES", output)
    return len(output)


def build_detention_facilities() -> int:
    output: list[dict[str, Any]] = []
    for row in load_csv(DETENTION_CSV):
        output.append(
            {
                "name": sanitize(row.get("name")),
                "fieldOffice": sanitize(row.get("field_office")),
                "address": sanitize(row.get("address_line1")),
                "city": sanitize(row.get("city")),
                "state": sanitize(row.get("state")),
                "postalCode": sanitize(row.get("postal_code")),
                "country": sanitize(row.get("country")),
                "phone": sanitize(row.get("phone")),
                "addressFull": sanitize(row.get("address_full")),
                "detailUrl": sanitize(row.get("detail_url")),
                "latitude": parse_float(row.get("latitude")),
                "longitude": parse_float(row.get("longitude")),
                "tracTypeDetailed": sanitize(row.get("trac_type_detailed")),
                "tracGuaranteedMinimum": sanitize(row.get("trac_guaranteed_minimum")),
                "tracAverageDailyPopulation": sanitize(row.get("trac_average_daily_population")),
                "tracAsOf": sanitize(row.get("trac_as_of")),
            }
        )
    output.sort(key=lambda r: (r["state"], r["city"], r["name"]))
    header = """export type DetentionFacility = {
  name: string;
  fieldOffice: string;
  address: string;
  city: string;
  state: string;
  postalCode: string;
  country: string;
  phone: string;
  addressFull: string;
  detailUrl: string;
  latitude: number | null;
  longitude: number | null;
  tracTypeDetailed: string;
  tracGuaranteedMinimum: string;
  tracAverageDailyPopulation: string;
  tracAsOf: string;
};"""
    write_ts(DETENTION_TS, header, "DETENTION_FACILITIES", output)
    return len(output)


def main() -> None:
    field_count = build_field_offices()
    detention_count = build_detention_facilities()
    print(f"Wrote {field_count} field offices to {FIELD_TS}")
    print(f"Wrote {detention_count} detention facilities to {DETENTION_TS}")


if __name__ == "__main__":
    main()
