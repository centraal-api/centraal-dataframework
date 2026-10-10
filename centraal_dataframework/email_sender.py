"""Compatibilidad: reexporta notifiers antiguos."""

from centraal_dataframework.notifiers import send_email_dq, send_email_error

__all__ = ['send_email_dq', 'send_email_error']
