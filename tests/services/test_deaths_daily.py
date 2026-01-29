from __future__ import annotations

import uuid

from src.services import deaths_daily


def test_normalize_record_generates_stable_id() -> None:
    access_date = "2026-01-24"
    record = deaths_daily.normalize_record(
        {
            "person_name": "Jane Doe",
            "date_of_death": "2025-01-02",
            "facility_or_location": "Austin, Texas",
            "death_context": "street",
            "sources": [
                {
                    "url": "https://www.reuters.com/world/us/example-story",
                    "publisher": "Example",
                    "publish_date": "2025-01-03",
                },
            ],
        },
        access_date,
    )
    expected = str(
        uuid.uuid5(
            deaths_daily.DEATH_RECORD_NAMESPACE,
            "jane doe|2025-01-02|Austin, Texas".lower(),
        ),
    )
    assert record["id"] == expected


def test_merge_records_respects_placeholders_and_sources() -> None:
    access_date = "2026-01-24"
    base = deaths_daily.normalize_record(
        {
            "id": "record-1",
            "person_name": "Jane Doe",
            "date_of_death": "2025-01-02",
            "death_context": "detention",
            "agency": "ICE",
            "summary_1_sentence": "Initial summary.",
            "sources": [
                {
                    "url": "https://apnews.com/article/ice-custody",
                    "publisher": "Example",
                    "publish_date": "2025-01-03",
                },
            ],
        },
        access_date,
    )
    incoming = deaths_daily.normalize_record(
        {
            "id": "record-1",
            "person_name": "Jane Doe",
            "date_of_death": "2025-01-02",
            "death_context": "street",
            "agency": "unknown",
            "summary_1_sentence": "Updated summary.",
            "sources": [
                {
                    "url": "https://www.pbs.org/newshour/example",
                    "publisher": "Example",
                    "publish_date": "2025-01-04",
                },
            ],
        },
        access_date,
    )

    merged, diffs, summary = deaths_daily.merge_records(
        {"record-1": base},
        [incoming],
    )

    record = merged["record-1"]
    assert record["death_context"] == "detention"
    assert record["agency"] == "ICE"
    assert record["summary_1_sentence"] == "Updated summary."
    assert len(record["sources"]) == 2
    assert summary["updated"] == 1
    assert len(diffs) == 1
    assert diffs[0]["change_type"] == "updated"


def test_merge_records_normalizes_name_and_location() -> None:
    access_date = "2026-01-24"
    base = deaths_daily.normalize_record(
        {
            "id": "base",
            "person_name": "Renee Good",
            "date_of_death": "2026-01-11",
            "facility_or_location": "Minneapolis, Minnesota",
            "death_context": "street",
            "sources": [{"url": "https://www.reuters.com/world/us/example-story"}],
        },
        access_date,
    )
    incoming = deaths_daily.normalize_record(
        {
            "id": "incoming",
            "person_name": "Renee Nicole Good",
            "date_of_death": "2026-01-11",
            "facility_or_location": "Minneapolis, Minnesota",
            "death_context": "street",
            "summary_1_sentence": "Updated summary.",
            "sources": [{"url": "https://apnews.com/article/ice-custody"}],
        },
        access_date,
    )

    merged, diffs, summary = deaths_daily.merge_records(
        {"base": base},
        [incoming],
    )

    assert len(merged) == 1
    assert "incoming" not in merged
    assert merged["base"]["summary_1_sentence"] == "Updated summary."
    assert len(merged["base"]["sources"]) == 2
    assert summary["updated"] == 1
    assert len(diffs) == 1


def test_triplets_to_records_skips_generic_and_missing_sources() -> None:
    access_date = "2026-01-24"
    triplets = [
        {
            "title": "Story about protest",
            "who": "Protestors",
            "what": "was shot",
            "where": "Minneapolis, Minnesota",
            "published_at": "2026-01-11T12:00:00Z",
            "url": "https://www.reuters.com/world/us/example-story",
            "source": "Reuters",
        },
        {
            "title": "Another story",
            "who": "Renee Good",
            "what": "was shot",
            "where": "Minneapolis, Minnesota",
            "published_at": "2026-01-11T12:00:00Z",
            "url": "",
            "source": "Reuters",
        },
    ]
    records = deaths_daily.triplets_to_records(triplets, access_date)
    assert records == []


def test_build_index_counts_and_date_range() -> None:
    access_date = "2026-01-24"
    records = [
        deaths_daily.normalize_record(
            {
                "id": "a",
                "person_name": "A",
                "date_of_death": "2025-01-02",
                "death_context": "detention",
                "homicide_status": "suspected",
                "sources": [{"url": "https://www.reuters.com/world/us/example"}],
            },
            access_date,
        ),
        deaths_daily.normalize_record(
            {
                "id": "b",
                "person_name": "B",
                "date_of_death": "2026-03",
                "death_context": "street",
                "homicide_status": "unknown",
                "sources": [{"url": "https://www.nytimes.com/example"}],
            },
            access_date,
        ),
        deaths_daily.normalize_record(
            {
                "id": "c",
                "person_name": "C",
                "date_of_death": "2026",
                "death_context": "street",
                "homicide_status": "ruled_homicide",
                "sources": [{"url": "https://www.pbs.org/newshour/example"}],
            },
            access_date,
        ),
    ]

    index = deaths_daily.build_index(records)
    assert index["counts"]["year"]["2025"] == 1
    assert index["counts"]["year"]["2026"] == 2
    assert index["counts"]["context"]["detention"] == 1
    assert index["counts"]["context"]["street"] == 2
    assert index["counts"]["homicide_status"]["suspected"] == 1
    assert index["counts"]["homicide_status"]["ruled_homicide"] == 1
    assert index["date_range"]["min"] == "2025-01-02"
    assert index["date_range"]["max"] == "2026-03-01"


def test_triangulation_caps_confidence_without_ap_and_nbc() -> None:
    access_date = "2026-01-24"
    record = deaths_daily.normalize_record(
        {
            "person_name": "Jane Doe",
            "date_of_death": "2026-01-10",
            "death_context": "street",
            "confidence_score": 80,
            "sources": [
                {"url": "https://www.reuters.com/world/us/example"},
                {"url": "https://www.nbcnews.com/news/us-news/example"},
            ],
        },
        access_date,
    )

    assert record["manual_review"] is True
    assert record["confidence_score"] == 45


def test_triangulation_boosts_with_ap_and_nbc() -> None:
    access_date = "2026-01-24"
    record = deaths_daily.normalize_record(
        {
            "person_name": "Jane Doe",
            "date_of_death": "2026-01-10",
            "death_context": "street",
            "confidence_score": 80,
            "sources": [
                {"url": "https://apnews.com/article/example"},
                {"url": "https://www.nbcnews.com/news/us-news/example"},
            ],
        },
        access_date,
    )

    assert record["manual_review"] is False
    assert record["confidence_score"] == 90


def test_normalize_record_derives_facility_fields() -> None:
    access_date = "2026-01-24"
    record = deaths_daily.normalize_record(
        {
            "person_name": "Sample Name",
            "date_of_death": "2025-05-12",
            "facility_or_location": "St. Louis County Jail",
            "death_context": "detention",
            "sources": [{"url": "https://www.ice.gov/doclib/foia/reports/example.pdf"}],
        },
        access_date,
    )
    assert record["facility_name"] == "St. Louis County Jail"
    assert record["location_category"] == "facility"
