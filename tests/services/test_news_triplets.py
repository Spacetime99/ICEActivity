from src.services.news_triplets import _detect_event_types


def test_detect_event_types_matches_multiple_labels() -> None:
    text = "Organizers planned a protest march and held a vigil outside ICE."
    types = set(_detect_event_types(text))

    assert "protest" in types
    assert "march" in types
    assert "vigil" in types


def test_detect_event_types_flags_unrest() -> None:
    text = "Police reported civil unrest and riots during the night."
    types = set(_detect_event_types(text))

    assert "civil_unrest" in types
    assert "riot" in types


def test_detect_event_types_ignores_month_march() -> None:
    text = "The trial is set for March 2024 in Portland."
    types = set(_detect_event_types(text))

    assert "march" not in types


def test_detect_event_types_ignores_mid_sentence_march_month() -> None:
    text = "The hearing in March 2024 was continued."
    types = set(_detect_event_types(text))

    assert "march" not in types


def test_detect_event_types_detects_march_event() -> None:
    text = "Organizers announced a march for justice downtown."
    types = set(_detect_event_types(text))

    assert "march" in types
