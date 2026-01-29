from __future__ import annotations

from src.services import newsroom_deaths


SAMPLE_TEXT = (
    "JANUARY 18, 2026EL PASO, TX\n"
    "DETAINEE DEATH NOTIFICATIONS\n"
    "ICE reports death of illegal alien in custody in El Paso\n"
    "EL PASO, Texas \u2014 Victor Manuel Diaz, a 36-year-old illegal alien from Nicaragua, "
    "died Jan. 16, 2026 at a hospital."
)


def test_parse_death_fields_handles_dateline_and_date() -> None:
    fields = newsroom_deaths.parse_death_fields(SAMPLE_TEXT)
    assert fields["release_date"] == "2026-01-18"
    assert fields["city"] == "El Paso"
    assert fields["state"] == "TX"
    assert fields["person_name"] == "Victor Manuel Diaz"
    assert fields["date_of_death"] == "2026-01-16"
    assert fields["age"] == 36
    assert fields["nationality"] == "Nicaragua"


def test_parse_death_fields_infers_year_from_release_date() -> None:
    text = (
        "JANUARY 18, 2026EL PASO, TX\n"
        "EL PASO, Texas \u2014 Victor Manuel Diaz, a 36-year-old illegal alien from Nicaragua, "
        "died in ICE custody Jan. 14 at Camp East Montana in El Paso, Texas."
    )
    fields = newsroom_deaths.parse_death_fields(text)
    assert fields["person_name"] == "Victor Manuel Diaz"
    assert fields["date_of_death"] == "2026-01-14"


def test_parse_death_fields_handles_month_day_without_year() -> None:
    text = (
        "JANUARY 9, 2026 INDIO, CA\n"
        "DETAINEE DEATH NOTIFICATIONS\n"
        "Illegal alien in ICE custody passes away at California hospital\n"
        "INDIO, California - Luis Beltran Yanez-Cruz, a 68-year-old illegal alien from Honduras "
        "in ICE custody, passed away Jan 6 at the John F. Kennedy Memorial Hospital."
    )
    fields = newsroom_deaths.parse_death_fields(text)
    assert fields["person_name"] == "Luis Beltran Yanez-Cruz"
    assert fields["date_of_death"] == "2026-01-06"


def test_parse_death_fields_marks_under_investigation() -> None:
    text = (
        "JANUARY 7, 2026 CONROE, TX\n"
        "DETAINEE DEATH NOTIFICATIONS\n"
        "CONROE, Texas - Luis Gustavo Nunez Caceres, a 42-year-old illegal alien from Honduras "
        "in ICE custody, passed away Jan 5 at HCA Houston Healthcare in Conroe.\n"
        "The official cause of his death remains under investigation."
    )
    fields = newsroom_deaths.parse_death_fields(text)
    assert fields["person_name"] == "Luis Gustavo Nunez Caceres"
    assert fields["date_of_death"] == "2026-01-05"
    assert fields["under_investigation"] is True
