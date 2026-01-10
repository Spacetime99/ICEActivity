import csv
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]
DETENTION_CSV = ROOT / "ice_detention_facilities.csv"
OUTPUT = ROOT / "frontend" / "src" / "staticLocations.ts"

FACILITY_CONFIG = {
    "Adelanto ICE Processing Center (CA)": {"source": "Adelanto ICE Processing Center"},
    "South Texas Family Residential Center (Dilley, TX)": {
        "manual": {
            "latitude": 28.6578745,
            "longitude": -99.2003862,
            "addressFull": "300 El Rancho Way Dilley, TX 78017 United States",
        }
    },
    "Tacoma ICE Processing Center (WA)": {"source": "Northwest ICE Processing Center (NWIPC)"},
    "Elizabeth Contract Detention Facility (NJ)": {"source": "Elizabeth Contract Detention Facility"},
    "Irwin County Detention Center (GA)": {
        "manual": {
            "latitude": 31.5925,
            "longitude": -83.2557,
            "addressFull": "132 Cotton Drive Ocilla, GA 31774 United States",
        }
    },
}

child_camps = [
    {
        "name": "Fort Bliss Emergency Intake Site (TX)",
        "latitude": 31.7926,
        "longitude": -106.424,
        "addressFull": "Fort Bliss, TX",
        "note": "Temporary intake site",
    },
    {
        "name": "Homestead Temporary Shelter (FL)",
        "latitude": 25.497,
        "longitude": -80.486,
        "addressFull": "Homestead, FL",
        "note": "Unaccompanied children shelter",
    },
    {
        "name": "Carrizo Springs Influx Care Facility (TX)",
        "latitude": 28.522,
        "longitude": -99.873,
        "addressFull": "Carrizo Springs, TX",
        "note": "Unaccompanied children facility",
    },
    {
        "name": "Donna Soft-Sided Facility (TX)",
        "latitude": 26.1707,
        "longitude": -98.0494,
        "addressFull": "Donna, TX",
        "note": "Unaccompanied children facility",
    },
]

rows = {}
with DETENTION_CSV.open(newline="", encoding="utf-8") as handle:
    reader = csv.DictReader(handle)
    for raw in reader:
        name = raw.get("name", "").strip()
        if not name:
            continue
        rows[name] = raw

locations = []
for display_name, cfg in FACILITY_CONFIG.items():
    entry = {"name": display_name, "type": "facility"}
    source_name = cfg.get("source")
    record = rows.get(source_name) if source_name else None
    if record:
        try:
            entry["latitude"] = float(record.get("latitude", "") or 0.0)
            entry["longitude"] = float(record.get("longitude", "") or 0.0)
        except ValueError:
            entry["latitude"] = entry["longitude"] = 0.0
        entry["address"] = record.get("address_line1") or None
        entry["addressFull"] = record.get("address_full") or None
    elif "manual" in cfg:
        manual = cfg["manual"]
        entry["latitude"] = manual.get("latitude", 0.0)
        entry["longitude"] = manual.get("longitude", 0.0)
        entry["address"] = manual.get("address")
        entry["addressFull"] = manual.get("addressFull")
    else:
        continue
    locations.append(entry)

for item in child_camps:
    locations.append(
        {
            "name": item["name"],
            "latitude": item["latitude"],
            "longitude": item["longitude"],
            "type": "child_camp",
            "address": None,
            "addressFull": item.get("addressFull"),
            "note": item.get("note"),
        }
    )

lines = [
    "export type StaticLocation = {",
    "  name: string;",
    "  latitude: number;",
    "  longitude: number;",
    "  type: \"facility\" | \"child_camp\";",
    "  address?: string | null;",
    "  addressFull?: string | null;",
    "  note?: string;",
    "};",
    "",
    "export const STATIC_LOCATIONS: StaticLocation[] = [",
]

for entry in locations:
    lines.append("  {")
    lines.append(f"    name: {json.dumps(entry['name'])},")
    lines.append(f"    latitude: {entry['latitude']},")
    lines.append(f"    longitude: {entry['longitude']},")
    lines.append(f"    type: {json.dumps(entry['type'])},")
    lines.append(f"    address: {json.dumps(entry.get('address'))},")
    lines.append(f"    addressFull: {json.dumps(entry.get('addressFull'))},")
    lines.append(f"    note: {json.dumps(entry.get('note'))}")
    lines.append("  },")

lines.append("];\n")
OUTPUT.write_text("\n".join(lines), encoding="utf-8")
