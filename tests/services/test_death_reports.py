from __future__ import annotations

from src.services import death_reports


SAMPLE_TEXT = """
U.S. Immigration and Customs Enforcement (ICE)
Detainee Death Report: AYALA Uribe, Ismael
General Demographic/Background Information
Date of Birth: November 27, 1985
Date of Death: September 22, 2025
Age: 39
Sex: Male
Country of Citizenship: Mexico
On August 22, 2025, ERO Los Angeles transferred Mr. AYALA Uribe to the Adelanto ICE
Processing Center (APC) in Adelanto, CA, pending removal proceedings.
"""


def test_parse_report_text_basic_fields() -> None:
    record = death_reports.parse_report_text(
        SAMPLE_TEXT,
        "https://www.ice.gov/doclib/foia/reports/ddrIsmaelUribeAyala.pdf",
    )
    assert record is not None
    assert record["person_name"] == "Ismael Ayala Uribe"
    assert record["name_raw"] == "AYALA Uribe, Ismael"
    assert record["date_of_birth"] == "1985-11-27"
    assert record["date_of_death"] == "2025-09-22"
    assert record["age"] == 39
    assert record["gender"] == "Male"
    assert record["country_of_citizenship"] == "Mexico"
    assert record["facility_or_location"] == "Adelanto ICE Processing Center (APC)"
    assert record["death_context"] == "detention"
    assert record["custody_status"] == "ICE detention"
    assert record["agency"] == "ICE"
    assert record["report_urls"] == [
        "https://www.ice.gov/doclib/foia/reports/ddrIsmaelUribeAyala.pdf"
    ]


def test_merge_records_keeps_existing_and_adds_urls() -> None:
    existing = {
        "id-1": {
            "id": "id-1",
            "person_name": "Ismael Ayala Uribe",
            "name_raw": "AYALA Uribe, Ismael",
            "date_of_birth": "1985-11-27",
            "date_of_death": "2025-09-22",
            "age": 39,
            "gender": "Male",
            "country_of_citizenship": "Mexico",
            "facility_or_location": None,
            "death_context": "detention",
            "custody_status": "ICE detention",
            "agency": "ICE",
            "report_urls": ["https://www.ice.gov/doclib/foia/reports/a.pdf"],
            "source_type": death_reports.SOURCE_TYPE,
            "extracted_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:00:00+00:00",
        }
    }
    incoming = [
        {
            "id": "id-1",
            "person_name": "Ismael Ayala Uribe",
            "name_raw": "AYALA Uribe, Ismael",
            "date_of_birth": "1985-11-27",
            "date_of_death": "2025-09-22",
            "age": 39,
            "gender": "Male",
            "country_of_citizenship": "Mexico",
            "facility_or_location": "Adelanto ICE Processing Center (APC)",
            "death_context": "detention",
            "custody_status": "ICE detention",
            "agency": "ICE",
            "report_urls": ["https://www.ice.gov/doclib/foia/reports/b.pdf"],
            "source_type": death_reports.SOURCE_TYPE,
            "extracted_at": "2026-01-02T00:00:00+00:00",
            "updated_at": "2026-01-02T00:00:00+00:00",
        }
    ]
    ordered, added, updated = death_reports._merge_records(existing, incoming)
    assert added == 0
    assert updated == 1
    assert ordered[0]["facility_or_location"] == "Adelanto ICE Processing Center (APC)"
    assert sorted(ordered[0]["report_urls"]) == [
        "https://www.ice.gov/doclib/foia/reports/a.pdf",
        "https://www.ice.gov/doclib/foia/reports/b.pdf",
    ]
