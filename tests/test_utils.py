"""Tests de utils."""

import json

import pytest

from centraal_dataframework.utils import build_queue_message, parse_connection_string, parse_queue_message


def test_parse_connection_string():
    """Parse connection string con '=' en el valor."""
    conn = "DefaultEndpointsProtocol=https;AccountName=na;AccountKey=key!=k;EndpointSuffix=core.windows.net"
    key_values = parse_connection_string(conn)
    assert key_values["AccountName"] == "na"
    assert key_values["AccountKey"] == "key!=k"


def test_parse_queue_message_plain():
    """Texto plano = nombre de tarea."""
    task, params, run_id = parse_queue_message('mi_tarea')
    assert task == 'mi_tarea'
    assert params == {}
    assert run_id


def test_parse_queue_message_json():
    """JSON con task/params/run_id."""
    raw = json.dumps({'task': 'alpha', 'params': {'d': '2024-01-01'}, 'run_id': 'abc'})
    task, params, run_id = parse_queue_message(raw)
    assert task == 'alpha'
    assert params['d'] == '2024-01-01'
    assert run_id == 'abc'


def test_parse_queue_message_empty():
    """Vacío falla."""
    with pytest.raises(ValueError):
        parse_queue_message('  ')


def test_build_queue_message():
    """Serializa JSON."""
    raw = build_queue_message('t1', {'a': 1}, run_id='r1')
    data = json.loads(raw)
    assert data == {'task': 't1', 'params': {'a': 1}, 'run_id': 'r1'}
