"""Tests de notifiers."""

from unittest.mock import MagicMock, patch

from centraal_dataframework.excepciones import ErrorTareaCalidadDatos
from centraal_dataframework.notifiers import LogicAppNotifier, send_email_dq, send_email_error


class _FakeResult:
    def list_validation_results(self, group_by=None):
        item_ok = {'success': True, 'exception_info': {}, 'result': {'a': 1}}
        item_fail = {
            'success': False,
            'exception_info': {},
            'result': {'b': 2},
            'expectation_config': {'expectation_type': 'expect_column_values_to_not_be_null'},
        }

        class VR:
            results = [item_ok, item_fail]

        return [VR()]

    def list_expectation_suite_names(self):
        return ['suite']

    def list_data_asset_names(self):
        return ['asset']


def test_notify_error_escapes_html_and_posts():
    """Escape de HTML y POST correcto."""
    notifier = LogicAppNotifier('https://example.com/hook', timeout=5)
    with patch('centraal_dataframework.notifiers.post_req') as post:
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        post.return_value = resp
        notifier.notify_error(['a@b.com'], RuntimeError('x <y>'), 'tarea1')
        payload = post.call_args.kwargs['json']
        assert '&lt;y&gt;' in payload['body_msj']
        assert 'tarea1' in payload['subject']


def test_notify_dq_uses_failed_list():
    """Usa lista failed filtrada."""
    notifier = LogicAppNotifier('https://example.com/hook')
    exc = ErrorTareaCalidadDatos('fail', url_docs='http://docs', failed=['exp_a'], check_point_result=_FakeResult())
    with patch('centraal_dataframework.notifiers.post_req') as post:
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        post.return_value = resp
        notifier.notify_dq(['a@b.com'], exc)
        body = post.call_args.kwargs['json']['body_msj']
        assert 'exp_a' in body


def test_send_failure_does_not_raise():
    """Fallo de red no tapa el error original."""
    notifier = LogicAppNotifier('https://example.com/hook')
    with patch('centraal_dataframework.notifiers.post_req', side_effect=RuntimeError('net')):
        notifier.notify_error(['a@b.com'], RuntimeError('orig'), 't')


def test_no_url_skips():
    """Sin URL no llama HTTP."""
    notifier = LogicAppNotifier(None)
    with patch('centraal_dataframework.notifiers.post_req') as post:
        notifier.notify_error(['a@b.com'], RuntimeError('x'), 't')
        post.assert_not_called()


def test_compat_wrappers():
    """Wrappers legacy."""
    with patch('centraal_dataframework.notifiers.post_req') as post:
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        post.return_value = resp
        send_email_error('https://x', ['a@b.com'], RuntimeError('e'), 't')
        send_email_dq(
            'https://x',
            ['a@b.com'],
            ErrorTareaCalidadDatos('m', failed=['f'], check_point_result=_FakeResult()),
        )
        assert post.call_count == 2
