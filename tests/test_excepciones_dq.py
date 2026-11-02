"""Excepciones de partición y de motor de calidad."""

from __future__ import annotations


def test_particion_invalida_message():
    """ParticionInvalida guarda tabla y motivo y arma el mensaje."""
    from centraal_dataframework import ParticionInvalida

    exc = ParticionInvalida('bronze.vista', 'la columna fecha no sale de filepath()')
    assert exc.tabla == 'bronze.vista' and exc.motivo.startswith('la columna')
    assert str(exc) == "Partición inválida en 'bronze.vista': la columna fecha no sale de filepath()"


def test_error_motor_calidad_message():
    """ErrorMotorCalidad guarda nombre, modo y mensajes de GX."""
    from centraal_dataframework import ErrorMotorCalidad

    exc = ErrorMotorCalidad('ventas', 'sql', ['a', 'b'])
    assert exc.mensajes == ['a', 'b'] and exc.modo == 'sql' and exc.name == 'ventas'
    assert str(exc) == "GX no pudo evaluar 'ventas' en modo 'sql': a | b"
