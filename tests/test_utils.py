"""Modulo para test de tasks."""
from centraal_dataframework.utils import parse_connection_string
from centraal_dataframework.resources import DUMMY_STRING


def test_parse_connection_string():
    """Test para parse connection."""
    key_values = parse_connection_string(DUMMY_STRING)
    assert key_values["AccountName"] == "na"
    assert key_values["AccountKey"] == "key!=k"
