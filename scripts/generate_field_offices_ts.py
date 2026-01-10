import csv
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "assets" / "ice_facilities.csv"
OUTPUT_PATH = ROOT / "frontend" / "src" / "fieldOffices.ts"

rows = []
with CSV_PATH.open(newline="", encoding="utf-8") as handle:
    reader = csv.DictReader(handle)
    for raw in reader:
        name = raw.get("name", "").strip()
        if not name:
            continue
        try:
            lat = float(raw.get("latitude", "") or 0.0)
            lon = float(raw.get("longitude", "") or 0.0)
        except ValueError:
            continue
        address = raw.get("address", "").strip()
        city = raw.get("city", "").strip()
        state = raw.get("state", "").strip()
        parts = [part for part in (address, city, state) if part]
        address_full = ", ".join(parts)
        rows.append(
            {
                "name": name,
                "address": address or None,
                "city": city,
                "state": state,
                "latitude": round(lat, 7),
                "longitude": round(lon, 7),
                "addressFull": address_full or None,
            }
        )

rows.sort(key=lambda item: item["name"])

header = """export type FieldOffice = {
  name: string;
  address?: string | null;
  addressFull?: string | null;
  city: string;
  state: string;
  latitude: number;
  longitude: number;
};

export const FIELD_OFFICES: FieldOffice[] = [
"""

lines = [header]
for row in rows:
    lines.append("  {")
    lines.append(f"    name: {json.dumps(row['name'])},")
    lines.append(f"    address: {json.dumps(row['address'])},")
    lines.append(f"    addressFull: {json.dumps(row['addressFull'])},")
    lines.append(f"    city: {json.dumps(row['city'])},")
    lines.append(f"    state: {json.dumps(row['state'])},")
    lines.append(f"    latitude: {row['latitude']},")
    lines.append(f"    longitude: {row['longitude']},")
    lines.append("  },")

lines.append("];\n")
OUTPUT_PATH.write_text("\n".join(lines), encoding="utf-8")
