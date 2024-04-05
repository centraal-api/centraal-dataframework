"""Utils."""

import logging
from typing import List

from azure.durable_functions import Blueprint


def parse_connection_string(connection_string):
    """Obtiene valores de string de conexion."""
    result = {}
    key_value_pairs = connection_string.split(';')

    for pair in key_value_pairs:
        index_of_equals = pair.find('=')

        if index_of_equals != -1:
            key = pair[:index_of_equals].strip()
            value = pair[index_of_equals + 1 :].strip()

            result[key] = value

    return result


def get_bindings_type(bp: Blueprint, key_name: str = "activity", binding_type: str = "activityTrigger") -> List[str]:
    list_of_bindings = [f._function.get_bindings_dict()['bindings'][0] for f in bp._function_builders]
    names_of_activities = [bd[key_name] for bd in list_of_bindings if bd["type"] == binding_type]
    return names_of_activities


def check_if_func_name_is_register(bp: Blueprint, name: str):
    if name in get_bindings_type(bp):
        if name == "alert":
            logging.debug("alert es una función del framework. No se hace log de la advertencia")
            return True
        logging.warning("Ignorando funcion con nombre duplicado %s", name)
        return True
    return False


def check_if_func_name_is_orchestator(bp: Blueprint, name: str):
    if name in get_bindings_type(bp, key_name="orchestration", binding_type="orchestrationTrigger"):
        logging.error("No es posible registrar pipeline %s. El nombre ya se encuentra definido", name)
        raise ValueError(f"{name} ya es un pipeline registrado. Los nombres de pipelines deben ser unicos.")
    return False
