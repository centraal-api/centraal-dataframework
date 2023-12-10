"""Funciones para enviar correos."""
import logging
import traceback
from typing import List

from requests import post as post_req

from centraal_dataframework.excepciones import ErrorTareaCalidadDatos


def send_email_dq(url_logic_app: str, emails: List[str], excepcion: ErrorTareaCalidadDatos):
    """Construye email y lo envia usando la logic app."""
    detalles_tecnicos = ''.join(traceback.TracebackException.from_exception(excepcion).format())
    result = excepcion.check_point_result
    subject = f'¡ALERTA! {excepcion.message}'
    suites = result.list_expectation_suite_names()

    body = (
        f"Las expectativas {','.join(suites)} **Fallaron**.<br>*"
        f"Detalle del resultado:<br>asset de datos: {','.join(result.list_data_asset_names())}<br>"
        f"resultados: {','.join(result.list_validation_results())}<br>"
        f"Para mayor detalles revisar la ruta: {result.validation_result_url}<br>"
        f"*Detalles tecnicos*:<br>{detalles_tecnicos}"
    )

    _send(url_logic_app, emails, subject, body)


def send_email_error(url_logic_app: str, emails: List[str], exception: Exception, tarea: str) -> None:
    """Construye email y lo envia usando la logic app."""
    detalles_tecnicos = ''.join(traceback.TracebackException.from_exception(exception).format())
    subject = f'¡ALERTA! Proceso de datos llamado: {tarea} FALLO'
    # <br> es salto de linea
    body = f"La tarea {tarea} **FALLO**.<br>**Detalles tecnicos**:<br>{detalles_tecnicos}"
    _send(url_logic_app, emails, subject, body)


def _send(url_logic_app: str, emails: List[str], subject: str, body: str):
    if len(emails) < 1:
        logging.warning("No existen emails configurados.")
    else:
        logging.warning("enviando notificacion a %s", emails)
        payload = {'emails': ", ".join(emails), 'subject': subject, 'body_msj': body}
        headers = {'Content-type': 'application/json'}
        post_req(url=url_logic_app, json=payload, timeout=3 * 60, headers=headers)
