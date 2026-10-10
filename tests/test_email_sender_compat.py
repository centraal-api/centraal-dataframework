"""Compat email_sender."""

from centraal_dataframework import email_sender


def test_email_sender_exports():
    """Reexporta send_email_*."""
    assert callable(email_sender.send_email_dq)
    assert callable(email_sender.send_email_error)
