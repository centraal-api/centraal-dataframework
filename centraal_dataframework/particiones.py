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
_ALIAS_RE = re.compile(r"\bAS\s+\[?(\w+)\]?\s*$", re.IGNORECASE)
_BULK_RE = re.compile(r"BULK\s*'([^']+)'", re.IGNORECASE)
_SELECT_RE = re.compile(r'\bSELECT\b(.*?)\bFROM\b', re.IGNORECASE | re.DOTALL)


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
    seleccion = _SELECT_RE.search(definicion)
    texto = seleccion.group(1) if seleccion else definicion
    columnas: Dict[str, int] = {}
    for item in _items_seleccion(texto):
        n = _FILEPATH_N_RE.search(item)
        alias = _ALIAS_RE.search(item)
        if n and alias:
            columnas[alias.group(1).lower()] = int(n.group(1))
    return columnas


def comodines_bulk(definicion: str) -> List[Optional[str]]:
    """Segmentos con ``*`` de la ruta ``BULK``, en orden: ``'fecha'`` para ``fecha=*``, ``None`` para ``*.parquet``."""
    ruta = _BULK_RE.search(definicion)
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
    """True si la vista tiene alguna columna derivada de ``filepath(n)``."""
    return bool(columnas_filepath(definicion))


def verificar_particion(definicion: str, columna: str, tabla: str) -> None:
    """Comprueba que ``columna`` es la partición de carpeta de la vista; si no, lanza ``ParticionInvalida``."""
    columnas = columnas_filepath(definicion)
    nombre = columna.lower()
    if nombre not in columnas:
        raise ParticionInvalida(tabla, f"la columna {columna!r} no sale de filepath(n) en la definición de la vista")
    if len(columnas) > 1:
        lista = ', '.join(columnas)
        raise ParticionInvalida(tabla, f"la vista tiene particiones anidadas [{lista}]; este cambio admite una sola")
    n = columnas[nombre]
    comodines = comodines_bulk(definicion)
    carpeta = comodines[n - 1] if 0 < n <= len(comodines) else None
    if carpeta != nombre:
        encontrada = f"'{carpeta}=*'" if carpeta else 'un comodín sin nombre de carpeta'
        raise ParticionInvalida(tabla, f"filepath({n}) corresponde a la carpeta {encontrada}, no a '{nombre}=*'")
