"""Particiones por carpeta en vistas de Synapse serverless sobre el lago.

Una vista de bronze expone la partición de la ruta (``.../fecha=*/*.parquet``) como columna derivada de
``filepath(n)``. Estas funciones leen la definición de la vista (``OBJECT_DEFINITION``) para saber qué columnas
son particiones y comprobar que la que declara una tarea lo es de verdad.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional

from centraal_dataframework.excepciones import ParticionInvalida

_FILEPATH_N_RE = re.compile(r"filepath\(\s*(\d+)\s*\)", re.IGNORECASE)
# alias al final del ítem: `AS fecha`, `AS [fecha]`, `AS "fecha"` o `... fecha` (sin AS)
_ALIAS_RE = re.compile(r"""(?:\bAS\s+|\s)[\["]?(\w+)[\]"]?\s*$""", re.IGNORECASE)
# alias a la izquierda: `fecha = CAST(...)`
_ALIAS_IZQ_RE = re.compile(r"""^[\["]?(\w+)[\]"]?\s*=(?!=)""")
_BULK_RE = re.compile(r"BULK\s*N?'([^']+)'", re.IGNORECASE)
_SELECT_RE = re.compile(r'\bSELECT\b(.*?)\bFROM\b', re.IGNORECASE | re.DOTALL)
_COMENTARIO_RE = re.compile(r'/\*.*?\*/|--[^\n]*', re.DOTALL)


def _sin_comentarios(definicion: str) -> str:
    """Quita comentarios ``/* */`` y ``--`` para no parsear SQL que no se ejecuta."""
    return _COMENTARIO_RE.sub(' ', definicion)


def _items_seleccion(texto: str) -> List[str]:
    """Separa la lista de selección por comas de primer nivel (respeta paréntesis)."""
    items, actual, nivel = [], [], 0
    for ch in texto:
        if ch == '(':
            nivel += 1
        elif ch == ')':
            nivel -= 1
        if ch == ',' and nivel == 0:
            items.append(''.join(actual))
            actual = []
        else:
            actual.append(ch)
    items.append(''.join(actual))
    return [i.strip() for i in items if i.strip()]


def columnas_filepath(definicion: str) -> Dict[str, int]:
    """Columnas de la vista derivadas de ``filepath(n)``: nombre en minúsculas → ``n``.

    Reconoce ``r.filepath(1) AS fecha``, ``CAST(r.filepath(1) AS date) AS [fecha]`` y
    ``CONVERT(date, r.filepath(1)) AS fecha``: el alias es el último ``AS`` del ítem de la selección.
    """
    limpio = _sin_comentarios(definicion)
    bloques = [m.group(1) for m in _SELECT_RE.finditer(limpio)] or [limpio]
    columnas: Dict[str, int] = {}
    for bloque in bloques:
        for item in _items_seleccion(bloque):
            n = _FILEPATH_N_RE.search(item)
            if not n:
                continue
            alias = _ALIAS_IZQ_RE.search(item) or _ALIAS_RE.search(item)
            if alias and alias.group(1).lower() != 'as':
                columnas[alias.group(1).lower()] = int(n.group(1))
    return columnas


def comodines_bulk(definicion: str) -> List[Optional[str]]:
    """Segmentos con ``*`` de la ruta ``BULK``, en orden: ``'fecha'`` para ``fecha=*``, ``None`` para ``*.parquet``."""
    ruta = _BULK_RE.search(_sin_comentarios(definicion))
    if not ruta:
        return []
    comodines: List[Optional[str]] = []
    for segmento in ruta.group(1).split('/'):
        if '*' not in segmento:
            continue
        nombre, separador, resto = segmento.partition('=')
        comodines.append(nombre.lower() if separador and resto.strip() == '*' else None)
    return comodines


def es_particionada(definicion: str) -> bool:
    """True si la vista expone una columna de ``filepath(n)`` o su ruta ``BULK`` tiene carpetas ``<col>=*``.

    La ruta cuenta por sí sola: si el parser no reconoce el alias de la columna, la vista se trata igual como
    particionada y la tarea tiene que declarar la partición (se falla cerrado, nunca escaneando entera).
    """
    return bool(columnas_filepath(definicion)) or any(c is not None for c in comodines_bulk(definicion))


def verificar_particion(definicion: str, columna: str, tabla: str) -> None:
    """Comprueba que ``columna`` es la partición de carpeta de la vista; si no, lanza ``ParticionInvalida``."""
    columnas = columnas_filepath(definicion)
    nombre = columna.lower()
    comodines = comodines_bulk(definicion)
    if nombre not in columnas:
        if nombre in comodines:
            raise ParticionInvalida(
                tabla,
                f"la ruta BULK tiene la carpeta '{nombre}=*' pero no se encontró una columna derivada de filepath(n) "
                f"con ese alias; expóngala como CAST(r.filepath({comodines.index(nombre) + 1}) AS ...) AS [{nombre}]",
            )
        raise ParticionInvalida(tabla, f"la columna {columna!r} no sale de filepath(n) en la definición de la vista")
    if len(columnas) > 1:
        lista = ', '.join(columnas)
        raise ParticionInvalida(tabla, f"la vista tiene particiones anidadas [{lista}]; este cambio admite una sola")
    n = columnas[nombre]
    carpeta = comodines[n - 1] if 0 < n <= len(comodines) else None
    if carpeta != nombre:
        encontrada = f"'{carpeta}=*'" if carpeta else 'un comodín sin nombre de carpeta'
        raise ParticionInvalida(tabla, f"filepath({n}) corresponde a la carpeta {encontrada}, no a '{nombre}=*'")
