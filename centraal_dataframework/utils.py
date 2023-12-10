"""Utils."""


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
