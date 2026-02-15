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
                {"url": "https://www.reuters.com/world/us/example", "source_type": "news"},
                {"url": "https://www.nbcnews.com/news/us-news/example", "source_type": "news"},
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
                {"url": "https://apnews.com/article/example", "source_type": "news"},
                {"url": "https://www.nbcnews.com/news/us-news/example", "source_type": "news"},
            ],
        },
        access_date,
    )

    assert record["manual_review"] is False
    assert record["confidence_score"] == 90


def test_ice_report_entry_to_record_preserves_location_fields() -> None:
    record = deaths_daily.ice_report_entry_to_record(
        {
            "person_name": "Jane Doe",
            "date_of_death": "2025-10-20",
            "initial_custody_location": "Eloy Detention Center, Eloy, AZ",
            "death_location": "Banner University Medical Center Phoenix, Phoenix, AZ",
            "facility_or_location": "Banner University Medical Center Phoenix, Phoenix, AZ",
            "report_urls": ["https://www.ice.gov/doclib/foia/reports/ddrJaneDoe.pdf"],
        },
        access_date="2026-02-15",
        min_year=2025,
    )
    assert record is not None
    assert record["initial_custody_location"] == "Eloy Detention Center, Eloy, AZ"
    assert record["death_location"] == "Banner University Medical Center Phoenix, Phoenix, AZ"
    assert record["facility_or_location"] == "Banner University Medical Center Phoenix, Phoenix, AZ"


def test_collapse_duplicate_records_merges_name_variants_and_sources() -> None:
    access_date = "2026-02-15"
    left = deaths_daily.normalize_record(
        {
            "id": "left",
            "person_name": "Gabriel Garcia Aviles",
            "date_of_death": "2025-10-23",
            "death_context": "detention",
            "facility_or_location": "Victor Valley Global Medical Center",
            "sources": [
                {
                    "url": "https://www.ice.gov/doclib/foia/reports/dderGabrielGarciaAviles.pdf",
                    "source_type": "official_report",
                },
            ],
            "confidence_score": 90,
        },
        access_date,
    )
    right = deaths_daily.normalize_record(
        {
            "id": "right",
            "person_name": "Gabriel Garcia-Aviles",
            "date_of_death": "2025-10-23",
            "death_context": "detention",
            "facility_or_location": None,
            "sources": [
                {
                    "url": "https://www.ice.gov/news/releases/illegal-alien-dies-victorville-medical-center-california-after-complications-alcohol",
                    "source_type": "official_release",
                },
            ],
            "confidence_score": 70,
        },
        access_date,
    )

    collapsed = deaths_daily.collapse_duplicate_records({"left": left, "right": right})
    assert len(collapsed) == 1
    merged = list(collapsed.values())[0]
    assert merged["facility_or_location"] == "Victor Valley Global Medical Center"
    assert len(merged["sources"]) == 2


def test_collapse_duplicate_records_merges_detention_same_day_even_with_location_mismatch() -> None:
    access_date = "2026-02-15"
    left = deaths_daily.normalize_record(
        {
            "id": "left",
            "person_name": "Francisco Gaspar Andres",
            "date_of_death": "2025-12-03",
            "death_context": "detention",
            "facility_or_location": "Krome South Processing Center, Miami, FL",
            "sources": [
                {
                    "url": "https://www.ice.gov/doclib/foia/reports/ddrFranciscoGasparAndres.pdf",
                    "source_type": "official_report",
                },
            ],
            "confidence_score": 90,
        },
        access_date,
    )
    right = deaths_daily.normalize_record(
        {
            "id": "right",
            "person_name": "Francisco Gaspar-Andres",
            "date_of_death": "2025-12-03",
            "death_context": "detention",
            "facility_or_location": None,
            "sources": [
                {
                    "url": "https://www.ice.gov/news/releases/example-francisco",
                    "source_type": "official_release",
                },
            ],
            "confidence_score": 60,
        },
        access_date,
    )

    collapsed = deaths_daily.collapse_duplicate_records({"left": left, "right": right})
    assert len(collapsed) == 1
    merged = list(collapsed.values())[0]
    assert merged["facility_or_location"] == "Krome South Processing Center, Miami, FL"
    assert len(merged["sources"]) == 2


def test_should_drop_record_rejects_non_person_official_release_names() -> None:
    access_date = "2026-02-15"
    record = deaths_daily.normalize_record(
        {
            "person_name": "Inspector General",
            "date_of_death": "2025-10-23",
            "death_context": "detention",
            "sources": [
                {
                    "url": "https://www.ice.gov/news/releases/example",
                    "source_type": "official_release",
                },
            ],
        },
        access_date,
    )
    assert deaths_daily._should_drop_record(record) is True


def test_collapse_duplicate_records_merges_street_story_followups() -> None:
    access_date = "2026-02-15"
    first = deaths_daily.normalize_record(
        {
            "id": "street-1",
            "person_name": "Renee Nicole Good",
            "date_of_death": "2026-01-16",
            "death_context": "street",
            "facility_or_location": "Minneapolis, Minnesota",
            "sources": [
                {
                    "url": "https://www.nbcnews.com/news/us-news/minneapolis-police-fire-department-reports-reveal-chaotic-moments-ice-rcna254362",
                    "source_type": "news",
                },
            ],
            "confidence_score": 45,
        },
        access_date,
    )
    second = deaths_daily.normalize_record(
        {
            "id": "street-2",
            "person_name": "Renee Nicole Good",
            "date_of_death": "2026-01-19",
            "death_context": "street",
            "facility_or_location": "Minneapolis, Minnesota",
            "sources": [
                {
                    "url": "https://www.nbcnews.com/news/us-news/bruce-springsteen-dedicates-song-renee-good-decries-crackdown-immigran-rcna254731",
                    "source_type": "news",
                },
            ],
            "confidence_score": 45,
        },
        access_date,
    )

    collapsed = deaths_daily.collapse_duplicate_records({"street-1": first, "street-2": second})
    assert len(collapsed) == 1
    merged = list(collapsed.values())[0]
    assert merged["person_name"] == "Renee Nicole Good"
    assert merged["date_of_death"] == "2026-01-16"
    assert merged["facility_or_location"] == "Minneapolis, Minnesota"
    assert len(merged["sources"]) == 2


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


def test_triplets_to_records_rejects_role_based_non_person_name() -> None:
    access_date = "2026-01-24"
    triplets = [
        {
            "title": "Officials say person was killed after encounter",
            "who": "family representative",
            "what": "was killed by an ICE officer",
            "where": "Minneapolis, Minnesota",
            "published_at": "2026-01-29T12:00:00Z",
            "url": "https://www.nbcnews.com/news/us-news/example",
            "source": "NBC News",
        },
    ]
    records = deaths_daily.triplets_to_records(triplets, access_date)
    assert records == []


def test_merge_records_dedupes_same_person_by_shared_source_url() -> None:
    access_date = "2026-01-24"
    base = deaths_daily.normalize_record(
        {
            "id": "base",
            "person_name": "Luis Gustavo Nunez Caceres",
            "date_of_death": "2026-01-05",
            "death_context": "detention",
            "primary_report_url": "https://www.ice.gov/news/releases/sample-release",
            "sources": [
                {
                    "url": "https://www.ice.gov/news/releases/sample-release",
                    "source_type": "official_release",
                },
            ],
        },
        access_date,
    )
    incoming = deaths_daily.normalize_record(
        {
            "id": "incoming",
            "person_name": "Luis Gustavo Nunez-Caceres",
            "date_of_death": "2025-12-23",
            "death_context": "detention",
            "primary_report_url": "https://www.ice.gov/news/releases/sample-release",
            "summary_1_sentence": "Updated summary.",
            "sources": [
                {
                    "url": "https://www.ice.gov/news/releases/sample-release",
                    "source_type": "official_release",
                },
            ],
        },
        access_date,
    )

    merged, _, _ = deaths_daily.merge_records({"base": base}, [incoming])
    assert len(merged) == 1
    assert "incoming" not in merged
    assert merged["base"]["summary_1_sentence"] == "Updated summary."


def test_merge_records_dedupes_nearby_dates_for_same_person_and_location() -> None:
    access_date = "2026-01-24"
    base = deaths_daily.normalize_record(
        {
            "id": "base",
            "person_name": "Renee Nicole Good",
            "date_of_death": "2026-01-16",
            "facility_or_location": "Minneapolis, Minnesota",
            "death_context": "street",
            "sources": [{"url": "https://www.nbcnews.com/news/us-news/a"}],
        },
        access_date,
    )
    incoming = deaths_daily.normalize_record(
        {
            "id": "incoming",
            "person_name": "Renee Good",
            "date_of_death": "2026-01-19",
            "facility_or_location": "Minneapolis, Minnesota",
            "death_context": "street",
            "sources": [{"url": "https://www.nbcnews.com/news/us-news/b"}],
        },
        access_date,
    )

    merged, _, _ = deaths_daily.merge_records({"base": base}, [incoming])
    assert len(merged) == 1
    assert "incoming" not in merged
    assert len(merged["base"]["sources"]) == 2


def test_normalize_record_sanitizes_narrative_city_field() -> None:
    access_date = "2026-01-24"
    record = deaths_daily.normalize_record(
        {
            "person_name": "Luis Gustavo Nunez Caceres",
            "date_of_death": "2026-01-05",
            "death_context": "detention",
            "city": (
                "United States Detainee Death Notifications. Illegal Alien In Ice Custody "
                "Passes Away At Houston-Area Hospital After Being Admitted For Chronic "
                "Heart-Related Health Issues Conroe"
            ),
            "state": "Texas",
            "sources": [{"url": "https://www.ice.gov/news/releases/example"}],
        },
        access_date,
    )
    assert record["city"] is None
    assert record["state"] == "Texas"


def test_should_drop_record_for_news_street_non_person_name() -> None:
    access_date = "2026-01-24"
    record = deaths_daily.normalize_record(
        {
            "person_name": "family representative",
            "date_of_death": "2026-01-29",
            "death_context": "street",
            "sources": [{"url": "https://www.nbcnews.com/news/us-news/example", "source_type": "news"}],
        },
        access_date,
    )
    assert deaths_daily._should_drop_record(record) is True
