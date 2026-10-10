"""Coverage extra notifiers."""

from unittest.mock import MagicMock, patch

from centraal_dataframework.excepciones import ErrorTareaCalidadDatos
from centraal_dataframework.notifiers import (
    CompositeNotifier,
    WebhookNotifier,
    _extract_failed_from_result,
    build_default_notifier,
)
from centraal_dataframework.settings import Settings


def test_webhook_and_composite():
    """Webhook + composite."""
    with patch('centraal_dataframework.notifiers.post_req') as post:
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        post.return_value = resp
        wh = WebhookNotifier('https://hooks.example/x', name='teams')
        wh.notify_error([], RuntimeError('e'), 't')
        wh.notify_dq([], ErrorTareaCalidadDatos('m', failed=['f']))
        wh.notify_success([], 't', 'rid')
        comp = CompositeNotifier([wh])
        comp.notify_success(['a@b.com'], 't', 'r')
        assert post.call_count >= 3


def test_build_default_notifier_with_webhooks():
    """Incluye teams/slack si hay URL."""
    s = Settings(teams_webhook_url='https://t', slack_webhook_url='https://s', logicapp_email_url=None)
    n = build_default_notifier(s)
    assert len(n.notifiers) == 3


def test_extract_failed_from_gx_style():
    """Extrae fallos de run_results."""
    item = MagicMock()
    item.success = False
    item.expectation_config = MagicMock(type='ExpectColumnValuesToNotBeNull')
    vr = MagicMock(results=[item])
    result = MagicMock(run_results={'k': MagicMock(validation_result=vr)})
    # quitar list_validation_results
    del result.list_validation_results
    failed = _extract_failed_from_result(result)
    assert any('ExpectColumn' in f or 'Null' in f for f in failed) or failed
