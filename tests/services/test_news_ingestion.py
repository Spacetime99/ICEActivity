from pathlib import Path

from src.services.news_ingestion import NewsIngestor, NewsReport


def make_report(text: str) -> NewsReport:
    return NewsReport(
        source="test",
        source_id=text,
        title=text,
        url="https://example.com",
        summary=None,
        published_at=None,
        locations=[],
        city_mentions=[],
        facility_mentions=[],
        raw={},
    )


def make_ingestor(tmp_path: Path, keywords: list[str]) -> NewsIngestor:
    return NewsIngestor(
        sources=[],
        output_dir=tmp_path,
        search_terms=[],
        relevance_keywords=keywords,
    )


def test_uppercase_ice_keyword_requires_exact_case(tmp_path: Path) -> None:
    ingestor = make_ingestor(tmp_path, ["ICE"])

    assert ingestor._is_relevant(make_report("ICE announces new policy"))
    assert not ingestor._is_relevant(make_report("Ice announces new policy"))
    assert not ingestor._is_relevant(make_report("ice announces new policy"))
    assert not ingestor._is_relevant(make_report("Local police announce new policy"))


def test_lowercase_keywords_remain_case_insensitive(tmp_path: Path) -> None:
    ingestor = make_ingestor(tmp_path, ["immigration"])

    assert ingestor._is_relevant(make_report("Immigration court backlog grows"))
    assert ingestor._is_relevant(make_report("IMMIGRATION COURT BACKLOG GROWS"))


def test_protest_context_keywords_surface_immigration_protests(tmp_path: Path) -> None:
    ingestor = make_ingestor(tmp_path, ["immigration"])

    assert ingestor._is_relevant(
        make_report("Protesters march against ICE raids in Chicago"),
    )
    assert ingestor._is_relevant(
        make_report("Residents march over housing policy in city hall"),
    )
