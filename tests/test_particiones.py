"""Parser de particiones filepath() en definiciones de vistas de Synapse serverless."""

from __future__ import annotations

import pytest

from centraal_dataframework.excepciones import ParticionInvalida
from centraal_dataframework.particiones import columnas_filepath, comodines_bulk, es_particionada, verificar_particion

DEF_SIMPLE = """CREATE VIEW demo.v AS
SELECT CAST(r.filepath(1) AS date) AS [fecha], r.*
FROM OPENROWSET(BULK 'capa/origen/tabla/fecha=*/*.parquet', DATA_SOURCE = 'Lago', FORMAT = 'PARQUET')
WITH ([id] bigint, [nombre] varchar(100)) AS r"""
DEF_SIN_CAST = DEF_SIMPLE.replace('CAST(r.filepath(1) AS date) AS [fecha]', 'r.filepath(1) AS fecha')
DEF_ANIDADA = DEF_SIMPLE.replace('AS [fecha], r.*', 'AS [anio], r.filepath(2) AS [mes], r.*').replace(
    'fecha=*/*.parquet', 'anio=*/mes=*/*.parquet'
)
DEF_ALIAS_DISTINTO = DEF_SIMPLE.replace('AS [fecha]', 'AS [dia]')
DEF_SIN_FILEPATH = 'CREATE VIEW demo.v AS SELECT id, nombre FROM demo.tabla_externa'


def test_columnas_filepath_simple():
    """Reconoce CAST(r.filepath(1) AS date) AS [fecha]."""
    assert columnas_filepath(DEF_SIMPLE) == {'fecha': 1}


def test_columnas_filepath_sin_cast():
    """Reconoce r.filepath(1) AS fecha sin corchetes ni CAST."""
    assert columnas_filepath(DEF_SIN_CAST) == {'fecha': 1}


def test_columnas_filepath_anidada():
    """Dos niveles de carpeta dan dos columnas."""
    assert columnas_filepath(DEF_ANIDADA) == {'anio': 1, 'mes': 2}


def test_columnas_filepath_ninguna():
    """Una vista sin filepath() no tiene columnas de partición."""
    assert columnas_filepath(DEF_SIN_FILEPATH) == {}


def test_comodines_bulk():
    """Los segmentos con * de la ruta BULK, en orden; None para el comodín de archivo."""
    assert comodines_bulk(DEF_SIMPLE) == ['fecha', None]


def test_es_particionada():
    """Particionada si tiene alguna columna derivada de filepath()."""
    assert es_particionada(DEF_SIMPLE) and not es_particionada(DEF_SIN_FILEPATH)


def test_verificar_particion_ok():
    """La columna declarada sale de filepath(n) y su comodín es <columna>=*; mayúsculas indiferentes."""
    verificar_particion(DEF_SIMPLE, 'FECHA', 't')


def test_verificar_particion_columna_no_filepath():
    """Una columna de datos no es partición."""
    with pytest.raises(ParticionInvalida, match='no sale de filepath'):
        verificar_particion(DEF_SIMPLE, 'id', 't')


def test_verificar_particion_anidada():
    """Con particiones anidadas se rechaza y se nombran todas."""
    with pytest.raises(ParticionInvalida, match=r'anidadas \[anio, mes\]'):
        verificar_particion(DEF_ANIDADA, 'anio', 't')


def test_verificar_particion_nombre_de_carpeta_distinto():
    """El alias de la columna debe coincidir con el nombre de la carpeta <col>=*."""
    with pytest.raises(ParticionInvalida, match=r"'fecha=\*', no a 'dia=\*'"):
        verificar_particion(DEF_ALIAS_DISTINTO, 'dia', 't')


# --- formas de alias que T-SQL admite y el parser debe reconocer; si no las reconoce, debe fallar cerrado ---

DEF_SIN_AS = DEF_SIMPLE.replace('CAST(r.filepath(1) AS date) AS [fecha]', 'CAST(r.filepath(1) AS date) fecha')
DEF_ALIAS_IZQ = DEF_SIMPLE.replace('CAST(r.filepath(1) AS date) AS [fecha]', 'fecha = CAST(r.filepath(1) AS date)')
DEF_COMILLAS = DEF_SIMPLE.replace('AS [fecha]', 'AS "fecha"')
DEF_SUBCONSULTA = 'CREATE VIEW demo.v AS SELECT * FROM (\n' + DEF_SIMPLE.split(' AS\n', 1)[1] + '\n) t'
DEF_COMENTARIO = '/* vista de bronze: SELECT x FROM y */\n-- CAST(r.filepath(9) AS date) AS [otra]\n' + DEF_SIMPLE
DEF_SOLO_RUTA = DEF_SIMPLE.replace('CAST(r.filepath(1) AS date) AS [fecha], ', '')  # fecha=* en la ruta, sin columna


@pytest.mark.parametrize('definicion', [DEF_SIN_AS, DEF_ALIAS_IZQ, DEF_COMILLAS, DEF_SUBCONSULTA, DEF_COMENTARIO])
def test_columnas_filepath_otras_formas_de_alias(definicion):
    """Alias sin AS, alias a la izquierda, con comillas dobles, en subconsulta y con comentarios."""
    assert columnas_filepath(definicion) == {'fecha': 1}


def test_es_particionada_por_la_ruta_aunque_no_se_reconozca_la_columna():
    """La ruta BULK con <carpeta>=* basta para tratar la vista como particionada (falla cerrado, no abierto)."""
    assert es_particionada(DEF_SOLO_RUTA)


def test_verificar_particion_carpeta_sin_columna_reconocida():
    """La carpeta existe en la ruta pero no hay columna derivada de filepath(n) con ese alias: el mensaje lo dice."""
    with pytest.raises(ParticionInvalida, match="carpeta 'fecha=\\*'.*AS \\[fecha\\]"):
        verificar_particion(DEF_SOLO_RUTA, 'fecha', 't')


def test_comentarios_no_se_parsean():
    """Un filepath() dentro de un comentario no cuenta como columna."""
    assert columnas_filepath(DEF_COMENTARIO) == {'fecha': 1}
