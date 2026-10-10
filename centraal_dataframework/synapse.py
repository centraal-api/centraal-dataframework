"""Cliente mínimo para Synapse SQL (serverless primero)."""

from __future__ import annotations

import logging
import os
import re
import struct
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from centraal_dataframework.excepciones import SynapseNoConfigurado
from centraal_dataframework.settings import Settings, get_settings

logger = logging.getLogger(__name__)

DRIVER = 'ODBC Driver 18 for SQL Server'
TOKEN_SCOPE = 'https://database.windows.net/.default'
SQL_COPT_SS_ACCESS_TOKEN = 1256
_sleep = time.sleep

_PARAM_RE = re.compile(r'\{\{\s*(\w+)\s*\}\}')
# Alfanumérico/_ separado por un solo '-', ':' o '.': fechas, timestamps, ids. Bloquea --, /*, comillas y espacios.
_SAFE_VALUE_RE = re.compile(r'^[A-Za-z0-9_]+(?:[-:.][A-Za-z0-9_]+)*$')
_GO_RE = re.compile(r'^\s*GO\s*;?\s*$', re.IGNORECASE | re.MULTILINE)

TRANSIENT_SQLSTATES = frozenset({'08S01', '08001', 'HYT00', 'HYT01', '40001'})
TRANSIENT_CODES = ('40613', '40501', '40197', '10928', '10929', '49918')


def _import_pyodbc():
    """Import diferido de pyodbc con mensaje claro si falta."""
    try:
        import pyodbc
    except ImportError as exc:
        raise SynapseNoConfigurado(
            "Falta pyodbc: instale centraal-dataframework[synapse] y el 'ODBC Driver 18 for SQL Server'."
        ) from exc
    return pyodbc


def _token_struct(token: str) -> bytes:
    """Empaqueta el token de Entra como lo espera el driver ODBC."""
    raw = token.encode('utf-16-le')
    return struct.pack(f'<I{len(raw)}s', len(raw), raw)


def _odbc_value(value: str) -> str:
    """Escapa un valor del connection string ODBC con llaves."""
    return '{' + value.replace('}', '}}') + '}'


def _fetch_rows(cursor: Any) -> List[Dict[str, Any]]:
    """Filas como diccionarios; lista vacía si no hay result set."""
    if not cursor.description:
        return []
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _fetch_scalar(cursor: Any) -> Any:
    """Primer valor de la primera fila o None."""
    if not cursor.description:
        return None
    row = cursor.fetchone()
    return row[0] if row else None


def _fetch_table(cursor: Any) -> tuple:
    """Columnas y filas crudas."""
    if not cursor.description:
        return [], []
    return [d[0] for d in cursor.description], [tuple(r) for r in cursor.fetchall()]


def _is_transient(exc: Exception) -> bool:
    """True para conexión, timeout y throttling; False para SQL y login (18456)."""
    sqlstate = str(exc.args[0]) if exc.args else ''
    message = ' '.join(str(a) for a in exc.args)
    if '18456' in message:
        return False
    return sqlstate in TRANSIENT_SQLSTATES or any(code in message for code in TRANSIENT_CODES)


def render(sql: str, **params: Any) -> str:
    """Sustituye {{nombre}} validando cada valor contra una lista blanca."""
    missing = sorted(set(_PARAM_RE.findall(sql)) - set(params))
    if missing:
        raise ValueError(f"Faltan parámetros: {', '.join(missing)}")

    def _sub(match: re.Match) -> str:
        name = match.group(1)
        value = str(params[name])
        if not _SAFE_VALUE_RE.match(value):
            raise ValueError(f"Valor no permitido para {name!r}: {value!r}")
        return value

    return _PARAM_RE.sub(_sub, sql)


@dataclass
class SynapseClient:
    """Cliente delgado de Synapse SQL: envía SQL y devuelve resultados pequeños."""

    server: str
    database: Optional[str] = None
    auth: str = 'identity'
    user: Optional[str] = None
    password: Optional[str] = field(default=None, repr=False)
    query_timeout: int = 1800
    login_timeout: int = 30
    max_attempts: int = 3
    credential: Any = field(default=None, repr=False)

    @classmethod
    def from_settings(cls, settings: Optional[Settings] = None) -> 'SynapseClient':
        """Crea el cliente desde Settings; serverless si solo hay workspace."""
        settings = settings or get_settings()
        server = settings.synapse_server
        if not server and settings.synapse_workspace:
            server = f"{settings.synapse_workspace}-ondemand.sql.azuresynapse.net"
        if not server:
            raise SynapseNoConfigurado("Configure SYNAPSE_WORKSPACE (serverless) o SYNAPSE_SERVER.")
        password = settings.synapse_password.get_secret_value() if settings.synapse_password else None
        if settings.synapse_auth == 'sql' and not (settings.synapse_user and password):
            raise SynapseNoConfigurado("SYNAPSE_AUTH=sql requiere SYNAPSE_USER y SYNAPSE_PASSWORD.")
        return cls(
            server=server,
            database=settings.synapse_database,
            auth=settings.synapse_auth,
            user=settings.synapse_user,
            password=password,
            query_timeout=settings.synapse_query_timeout,
        )

    def for_database(self, database: str) -> 'SynapseClient':
        """Copia del cliente apuntando a otra base (comparte credencial)."""
        return replace(self, database=database)

    def connection_string(self) -> str:
        """Connection string ODBC (sin token)."""
        parts = [
            f'Driver={{{DRIVER}}}',
            f'Server=tcp:{self.server},1433',
            'Encrypt=yes',
            'TrustServerCertificate=no',
            f'Connection Timeout={self.login_timeout}',
        ]
        if self.database:
            parts.append(f'Database={self.database}')
        if self.auth == 'sql':
            parts += [f'Uid={_odbc_value(self.user or "")}', f'Pwd={_odbc_value(self.password or "")}']
        return ';'.join(parts) + ';'

    def _get_token(self) -> str:
        """Token de Entra; la credencial se crea una vez y cachea/renueva."""
        if self.credential is None:
            from azure.identity import DefaultAzureCredential

            self.credential = DefaultAzureCredential()
        return self.credential.get_token(TOKEN_SCOPE).token

    def connect(self) -> Any:
        """Abre una conexión en autocommit (serverless no admite DDL en transacción)."""
        pyodbc = _import_pyodbc()
        attrs: Dict[int, bytes] = {}
        if self.auth == 'identity':
            attrs[SQL_COPT_SS_ACCESS_TOKEN] = _token_struct(self._get_token())
        conn = pyodbc.connect(self.connection_string(), autocommit=True, attrs_before=attrs)
        conn.timeout = self.query_timeout
        return conn

    def _run(self, sql: str, params: tuple, fetch: Callable[[Any], Any]) -> Any:
        """Ejecuta en una conexión nueva; reintenta solo errores transitorios."""
        pyodbc = _import_pyodbc()
        for attempt in range(1, self.max_attempts + 1):
            start = time.monotonic()
            try:
                conn = self.connect()
                try:
                    cursor = conn.cursor()
                    cursor.execute(sql, *params)
                    result = fetch(cursor)
                finally:
                    conn.close()
            except pyodbc.Error as exc:
                if attempt >= self.max_attempts or not _is_transient(exc):
                    raise
                wait = 2**attempt
                logger.warning(
                    "Synapse error transitorio (intento %s/%s), espera %ss: %s", attempt, self.max_attempts, wait, exc
                )
                _sleep(wait)
                continue
            logger.info("Synapse [%s] %.2fs", self.database or 'default', time.monotonic() - start)
            return result
        raise RuntimeError('unreachable')  # pragma: no cover

    def query(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        """Ejecuta un SELECT y devuelve filas como diccionarios."""
        return self._run(sql, params, _fetch_rows)

    def scalar(self, sql: str, *params: Any) -> Any:
        """Ejecuta y devuelve el primer valor."""
        return self._run(sql, params, _fetch_scalar)

    def execute(self, sql: str, *params: Any) -> int:
        """Ejecuta DDL/DML/CETAS y devuelve rowcount (-1 si no aplica)."""
        return self._run(sql, params, lambda cursor: cursor.rowcount)

    def execute_file(self, path: Union[str, os.PathLike], **params: Any) -> int:
        """Renderiza un .sql y ejecuta cada lote separado por GO."""
        text = render(Path(path).read_text(encoding='utf-8'), **params)
        total = 0
        for batch in (b.strip() for b in _GO_RE.split(text)):
            if batch:
                total += max(self.execute(batch), 0)
        return total

    def read_df(self, sql: str, *params: Any) -> Any:
        """Ejecuta un SELECT y devuelve un pandas.DataFrame (solo resultados pequeños)."""
        import pandas as pd

        cols, rows = self._run(sql, params, _fetch_table)
        return pd.DataFrame.from_records(rows, columns=cols)


def get_synapse(settings: Optional[Settings] = None) -> SynapseClient:
    """Obtiene un SynapseClient desde Settings."""
    return SynapseClient.from_settings(settings)
