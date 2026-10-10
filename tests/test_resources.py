"""Tests de resources / DQResult."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from centraal_dataframework.excepciones import StorageNoConfigurado
from centraal_dataframework.resources import DQResult, get_datalake
from centraal_dataframework.settings import Settings


def test_dq_result_defaults():
    """DQResult defaults."""
    r = DQResult(success=True)
    assert r.failed == []
    assert r.url == ''


def test_get_datalake_requires_config():
    """Sin connection string ni account name falla."""
    s = Settings(azure_storage_connection_string=None, storage_account_name=None)
    with pytest.raises(StorageNoConfigurado):
        get_datalake(s)


def test_get_datalake_from_connection_string(monkeypatch):
    """Usa AccountKey del connection string."""
    conn = "DefaultEndpointsProtocol=https;AccountName=acct;" "AccountKey=key123;EndpointSuffix=core.windows.net"
    s = Settings(azure_storage_connection_string=conn)
    fake = object()
    mock_cls = MagicMock()
    mock_cls.from_account_key.return_value = fake
    monkeypatch.setattr('centraal_dataframework.resources._import_datalake', lambda: mock_cls)
    assert get_datalake(s) is fake
    mock_cls.from_account_key.assert_called_once_with('acct', 'key123')
