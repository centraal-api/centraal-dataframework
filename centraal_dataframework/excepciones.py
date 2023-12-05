"""Modulo de excepciones."""
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult


class ErrorEnTarea(Exception):
    """Excepción para levantar cuando se presenta un error no controlado.

    Attributes:
        nombre de la tarea que fallo.
    """

    def __init__(self, task_name: str):
        """Incializa con el nombre de la tarea."""
        self.message = f"Se encontro un error en {task_name}."
        super().__init__(self.message)


class TareaDuplicada(Exception):
    """Excepción para levantar cuando la tarea ya existe.

    Attributes:
        nombre de la tarea duplicada.
    """

    def __init__(self, task_name: str):
        """Incializa con el nombre de la tarea."""
        self.message = f" la funcion {task_name} ya existe!."
        super().__init__(self.message)


class TareaNoDefinida(Exception):
    """Excepción para levantar cuando la tarea no esta definida.

    Attributes:
        nombre de tarea no definida
    """

    def __init__(self, task_name: str):
        """Incializa con el nombre de la tarea."""
        self.message = f" la funcion {task_name} NO existe!."
        super().__init__(self.message)


class ErrorTareaCalidadDatos(Exception):
    """Excepción para levantar se presenta un error de calidad de datos.

    Attributes:
        suite que fallo.
    """

    def __init__(self, check_point_result: CheckpointResult, url_docs: str):
        """Incializa con el nombre de la tarea."""
        suites = ",".join(check_point_result.list_expectation_suite_names())
        self.message = f"¡Las expectativas {suites} Fallaron!."
        self.check_point_result = check_point_result
        self.url_docs = url_docs
        super().__init__(self.message)
