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
