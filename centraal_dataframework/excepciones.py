"""Modulo de excepciones."""

from typing import Any, Optional


class ErrorEnTarea(Exception):
    """Excepción para levantar cuando se presenta un error no controlado."""

    def __init__(self, task_name: str):
        """Inicializa con el nombre de la tarea."""
        self.task_name = task_name
        self.message = f"Se encontro un error en {task_name}."
        super().__init__(self.message)


class TareaDuplicada(Exception):
    """Excepción para levantar cuando la tarea ya existe."""

    def __init__(self, task_name: str):
        """Inicializa con el nombre de la tarea."""
        self.task_name = task_name
        self.message = f" la funcion {task_name} ya existe!."
        super().__init__(self.message)


class TareaNoDefinida(Exception):
    """Excepción para levantar cuando la tarea no esta definida."""

    def __init__(self, task_name: str):
        """Inicializa con el nombre de la tarea."""
        self.task_name = task_name
        self.message = f" la funcion {task_name} NO existe!."
        super().__init__(self.message)


class ErrorTareaCalidadDatos(Exception):
    """Excepción para levantar cuando falla una validación de calidad de datos."""

    def __init__(self, message: str, url_docs: str = '', check_point_result: Any = None, failed: Optional[list] = None):
        """Inicializa con el detalle de DQ."""
        self.message = message
        self.url_docs = url_docs
        self.check_point_result = check_point_result
        self.failed = failed or []
        super().__init__(self.message)


class ConfiguracionInvalida(Exception):
    """Excepción cuando la configuración del framework es inválida."""


class StorageNoConfigurado(Exception):
    """Excepción cuando falta conexión al storage."""


class SynapseNoConfigurado(Exception):
    """Excepción cuando falta configuración o dependencias de Synapse."""
