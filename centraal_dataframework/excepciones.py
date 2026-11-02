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

    def __init__(
        self,
        message: str,
        url_docs: str = '',
        check_point_result: Any = None,
        failed: Optional[list] = None,
        result: Any = None,
    ):
        """Inicializa con el detalle de DQ; result es el DQResult de la tarea."""
        self.message = message
        self.result = result
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


class RecursoDesconocido(Exception):
    """Excepción cuando una tarea declara una plataforma que no está registrada."""

    def __init__(self, name: str, available: Optional[list] = None):
        """Inicializa con la plataforma pedida y las registradas."""
        self.name = name
        self.message = f"La plataforma {name!r} no está registrada. Registradas: {available or []}."
        super().__init__(self.message)


class RecursoNoDeclarado(Exception):
    """Excepción cuando una tarea usa un recurso que no declaró en ``uses``."""

    def __init__(self, task_name: str, use: str, declared: tuple = ()):
        """Inicializa con la tarea, el recurso usado y los declarados."""
        self.task_name = task_name
        self.use = use
        self.message = (
            f"La tarea {task_name!r} usa {use!r} sin declararlo. "
            f"Agréguelo a @task(uses=[...]); declarados: {list(declared)}."
        )
        super().__init__(self.message)


class RecursoNoConfigurado(Exception):
    """Excepción cuando un recurso declarado no tiene su configuración completa."""

    def __init__(self, task_name: str, problems: list):
        """Inicializa con la tarea y los problemas encontrados."""
        self.task_name = task_name
        self.problems = problems
        self.message = f"La tarea {task_name!r} tiene recursos sin configurar: " + '; '.join(problems)
        super().__init__(self.message)


class ParticionInvalida(Exception):
    """Excepción cuando la partición declarada no se puede verificar o aplicar sobre el objeto de Synapse."""

    def __init__(self, tabla: str, motivo: str):
        """Inicializa con el objeto (esquema.tabla) y el motivo."""
        self.tabla = tabla
        self.motivo = motivo
        self.message = f"Partición inválida en {tabla!r}: {motivo}"
        super().__init__(self.message)


class ErrorMotorCalidad(Exception):
    """Excepción cuando GX no pudo evaluar una expectativa (error técnico, no un fallo de calidad)."""

    def __init__(self, name: str, modo: str, mensajes: list):
        """Inicializa con el nombre de la validación, el modo y los mensajes de GX."""
        self.name = name
        self.modo = modo
        self.mensajes = list(mensajes)
        self.message = f"GX no pudo evaluar {name!r} en modo {modo!r}: " + ' | '.join(self.mensajes)
        super().__init__(self.message)
