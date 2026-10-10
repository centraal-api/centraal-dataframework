"""Framework para ejecutar transformaciones y calidad de datos en Azure Functions."""

__author__ = """centraal.studio"""
__email__ = 'equipo@centraal.studio'
__version__ = '0.2.0'

from centraal_dataframework.excepciones import ErrorEnTarea, ErrorTareaCalidadDatos, TareaDuplicada, TareaNoDefinida
from centraal_dataframework.resources import DQResult
from centraal_dataframework.synapse import SynapseClient
from centraal_dataframework.tasks import TaskContext, task, task_dq

__all__ = [
    'task',
    'task_dq',
    'TaskContext',
    'DQResult',
    'SynapseClient',
    'ErrorEnTarea',
    'ErrorTareaCalidadDatos',
    'TareaDuplicada',
    'TareaNoDefinida',
    '__version__',
]
