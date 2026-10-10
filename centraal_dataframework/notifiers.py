"""Notificadores de alertas (Logic App, Teams, Slack)."""

from __future__ import annotations

import html
import logging
import traceback
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Sequence

from requests import post as post_req

from centraal_dataframework.excepciones import ErrorTareaCalidadDatos

logger = logging.getLogger(__name__)


class Notifier(ABC):
    """Interfaz de notificaciones."""

    @abstractmethod
    def notify_error(self, emails: Sequence[str], exception: Exception, tarea: str) -> None:
        """Notifica error de tarea."""

    @abstractmethod
    def notify_dq(self, emails: Sequence[str], excepcion: ErrorTareaCalidadDatos) -> None:
        """Notifica fallo de calidad de datos."""

    def notify_success(self, emails: Sequence[str], tarea: str, run_id: str = '') -> None:
        """Notifica éxito (opcional)."""
        return None


class LogicAppNotifier(Notifier):
    """Envía notificaciones vía Logic App HTTP."""

    def __init__(self, url: Optional[str], timeout: int = 30):
        """Inicializa con URL de Logic App."""
        self.url = url
        self.timeout = timeout

    def notify_error(self, emails: Sequence[str], exception: Exception, tarea: str) -> None:
        """Construye email de error y lo envía."""
        detalles = html.escape(''.join(traceback.TracebackException.from_exception(exception).format()))
        subject = f'¡ALERTA! Proceso de datos llamado: {tarea} FALLO'
        body = f"La tarea {html.escape(tarea)} <strong>FALLO</strong>.<br><i>Detalles tecnicos</i>:<br>{detalles}"
        self._send(list(emails), subject, body)

    def notify_dq(self, emails: Sequence[str], excepcion: ErrorTareaCalidadDatos) -> None:
        """Construye email de DQ y lo envía."""
        detalles = html.escape(''.join(traceback.TracebackException.from_exception(excepcion).format()))
        subject = f'¡ALERTA! {excepcion.message}'
        failed_items = excepcion.failed or _extract_failed_from_result(excepcion.check_point_result)
        error_msj = "<br>".join(html.escape(str(item)) for item in failed_items) or "Sin detalle de fallos."
        url = html.escape(excepcion.url_docs or '')
        body = (
            f"<strong>Fallaron expectativas de calidad de datos.</strong><br><br>"
            "<b>Detalle del resultado</b>"
            "<ul>"
            f"""<li><a href="{url}" target="_blank">Detalles de la expectativa</a></li>"""
            f"<li><i>Expectativas fallidas</i>: {error_msj}</li>"
            f"<li><i>Detalles tecnicos:</i>{detalles}</li>"
            "</ul>"
        )
        self._send(list(emails), subject, body)

    def notify_success(self, emails: Sequence[str], tarea: str, run_id: str = '') -> None:
        """Notifica éxito."""
        subject = f'OK: {tarea} completada'
        body = f"La tarea {html.escape(tarea)} terminó correctamente. run_id={html.escape(run_id)}"
        self._send(list(emails), subject, body)

    def _send(self, emails: List[str], subject: str, body: str) -> None:
        if not emails:
            logger.warning("No existen emails configurados.")
            return
        if not self.url:
            logger.warning("LOGICAPP_EMAIL_URL no configurada; se omite envío a %s", emails)
            return
        try:
            logger.info("enviando notificacion a %s", emails)
            payload = {'emails': ", ".join(emails), 'subject': subject, 'body_msj': body}
            headers = {'Content-type': 'application/json'}
            response = post_req(url=self.url, json=payload, timeout=self.timeout, headers=headers)
            response.raise_for_status()
        except Exception:  # noqa: BLE001
            logger.exception("Fallo al enviar notificación; no se tapa el error original de la tarea.")


class WebhookNotifier(Notifier):
    """Notificador genérico por webhook (Teams / Slack)."""

    def __init__(self, url: Optional[str], name: str = 'webhook', timeout: int = 30):
        """Inicializa webhook."""
        self.url = url
        self.name = name
        self.timeout = timeout

    def notify_error(self, emails: Sequence[str], exception: Exception, tarea: str) -> None:
        """Envía mensaje de error al webhook."""
        text = f"ALERTA: tarea {tarea} falló: {exception}"
        self._post(text)

    def notify_dq(self, emails: Sequence[str], excepcion: ErrorTareaCalidadDatos) -> None:
        """Envía mensaje de DQ al webhook."""
        text = f"ALERTA DQ: {excepcion.message} url={excepcion.url_docs}"
        self._post(text)

    def notify_success(self, emails: Sequence[str], tarea: str, run_id: str = '') -> None:
        """Envía mensaje de éxito."""
        self._post(f"OK: {tarea} run_id={run_id}")

    def _post(self, text: str) -> None:
        if not self.url:
            return
        try:
            # Teams y Slack aceptan {text} o {text, content}; usamos ambos.
            post_req(self.url, json={'text': text, 'content': text}, timeout=self.timeout).raise_for_status()
        except Exception:  # noqa: BLE001
            logger.exception("Fallo al enviar webhook %s", self.name)


class CompositeNotifier(Notifier):
    """Encadena varios notificadores."""

    def __init__(self, notifiers: Sequence[Notifier]):
        """Inicializa con lista de notificadores."""
        self.notifiers = list(notifiers)

    def notify_error(self, emails: Sequence[str], exception: Exception, tarea: str) -> None:
        """Propaga error a todos."""
        for notifier in self.notifiers:
            notifier.notify_error(emails, exception, tarea)

    def notify_dq(self, emails: Sequence[str], excepcion: ErrorTareaCalidadDatos) -> None:
        """Propaga DQ a todos."""
        for notifier in self.notifiers:
            notifier.notify_dq(emails, excepcion)

    def notify_success(self, emails: Sequence[str], tarea: str, run_id: str = '') -> None:
        """Propaga éxito a todos."""
        for notifier in self.notifiers:
            notifier.notify_success(emails, tarea, run_id)


def build_default_notifier(settings: Any) -> Notifier:
    """Construye el notificador compuesto desde Settings."""
    items: List[Notifier] = [LogicAppNotifier(getattr(settings, 'logicapp_email_url', None))]
    teams = getattr(settings, 'teams_webhook_url', None)
    slack = getattr(settings, 'slack_webhook_url', None)
    if teams:
        items.append(WebhookNotifier(teams, name='teams'))
    if slack:
        items.append(WebhookNotifier(slack, name='slack'))
    return CompositeNotifier(items)


def _extract_failed_from_result(result: Any) -> List[str]:
    """Extrae expectativas fallidas de un resultado GX (best-effort)."""
    failed: List[str] = []
    if result is None:
        return failed
    try:
        if hasattr(result, 'list_validation_results'):
            validations = result.list_validation_results(group_by=None)
        else:
            validations = []
        for res in validations:
            results = getattr(res, 'results', None) or []
            for item in results:
                success = item.get('success') if isinstance(item, dict) else getattr(item, 'success', True)
                if success is False:
                    exp_type = (
                        item.get('expectation_config', {}).get('expectation_type')
                        if isinstance(item, dict)
                        else getattr(getattr(item, 'expectation_config', None), 'type', 'expectation')
                    )
                    failed.append(str(exp_type))
    except Exception:  # noqa: BLE001
        logger.debug("No se pudieron extraer fallos del resultado GX", exc_info=True)
    # GX 1.x CheckpointResult
    try:
        run_results = getattr(result, 'run_results', None) or {}
        for value in run_results.values():
            vr = getattr(value, 'validation_result', value)
            for item in getattr(vr, 'results', []) or []:
                if getattr(item, 'success', True) is False:
                    cfg = getattr(item, 'expectation_config', None)
                    failed.append(str(getattr(cfg, 'type', None) or getattr(cfg, 'expectation_type', 'expectation')))
    except Exception:  # noqa: BLE001
        pass
    return failed


# Compatibilidad con imports antiguos
def send_email_dq(url_logic_app: str, emails: List[str], excepcion: ErrorTareaCalidadDatos) -> None:
    """Compat: envía email DQ."""
    LogicAppNotifier(url_logic_app).notify_dq(emails, excepcion)


def send_email_error(url_logic_app: str, emails: List[str], exception: Exception, tarea: str) -> None:
    """Compat: envía email de error."""
    LogicAppNotifier(url_logic_app).notify_error(emails, exception, tarea)
