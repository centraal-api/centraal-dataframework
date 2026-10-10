# Soporte mínimo de Synapse serverless — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Que una tarea del framework pueda enviar SQL y leer resultados de Synapse (serverless primero) con `ctx.synapse`, autenticada con identidad administrada.

**Architecture:** Un módulo nuevo `centraal_dataframework/synapse.py` con `SynapseClient`, un cliente delgado sobre `pyodbc` con token de Entra (`DefaultAzureCredential`). Abre una conexión por llamada en `autocommit`, reintenta solo errores transitorios y sustituye `{{param}}` con lista blanca. `TaskContext` crea `datalake` y `synapse` de forma perezosa, solo cuando la tarea los usa. Sin CETAS versionado, `paso_sql` ni GX sobre SQL: eso queda para planes posteriores (§13.c–f del spec).

**Tech Stack:** Python 3.10–3.14, `pyodbc` 5.x (extra opcional `[synapse]`) + `ODBC Driver 18 for SQL Server`, `azure-identity`, pydantic-settings, pytest.

**Spec:** `C:\Users\German\Centraal\cliente-summa\requerimientos\plataforma-digital-datos-company-in-a-box\27-arquitectura-bajo-nivel-ejecucion.md` — §2 (P3, P4), §8.1 (conexión), §5 (parámetros con lista blanca), §9.3 (reintentos), §10.1 (identidad), §13.b y §13.i (dependencias opcionales).

## Global Constraints

- **Precondición:** 0.2.0 está sin commitear (spec §13.a). Antes de la Task 1, el usuario commitea 0.2.0. Si no lo hace, se **omiten** los pasos de commit: `settings.py`, `tasks.py`, `pyproject.toml` y `CHANGELOG.md` ya tienen cambios de 0.2.0 sin commitear y se mezclarían.
- Python `>=3.10,<3.15`. Usar `from __future__ import annotations`, sin `match`.
- Estilo: black `line-length = 120`, `skip-string-normalization` (comillas simples), isort, flake8 con `flake8-docstrings` en convención google. Toda función, clase y método público lleva docstring, también en los tests.
- Textos, docstrings y mensajes de error en español, como el resto del paquete.
- Cambio **genérico**: nada de este cliente. Sin nombres de workspace, bases ni tablas del proyecto en el código (spec §13).
- **Serverless primero:** si solo hay `SYNAPSE_WORKSPACE`, el servidor es `<workspace>-ondemand.sql.azuresynapse.net`. Un pool dedicado solo se usa con `SYNAPSE_SERVER` explícito.
- `pyodbc` es **opcional**: importar el paquete sin `pyodbc` no falla. El error solo aparece al usar Synapse, con un mensaje que dice qué instalar.
- Auth por defecto: identidad, con scope `https://database.windows.net/.default` y `attrs_before={1256: token}`. Sin secretos en el código.
- Por la Function viajan resultados, no se guardan: no loguear filas, solo duración y base.
- Límite de consulta en serverless: 30 min, así que `SYNAPSE_QUERY_TIMEOUT` vale 1800 por defecto.

## Review Focus

1. **Valores de parámetro maliciosos o raros** en `render` (`'`, `;`, espacios, `--`, vacío, `/*`): deben dar `ValueError` antes de tocar Synapse. Test en la Task 2.
2. **Login fallido o bloqueado** (error 18456, R-042): **no** se reintenta, para no agravar el bloqueo. Test en la Task 3.
3. **Error de SQL** (sintaxis, objeto que ya existe en un CETAS): `ProgrammingError` no se reintenta y se propaga al primer intento. Test en la Task 3.
4. **Tarea que solo usa Synapse, sin storage configurado**: no debe fallar por `get_datalake`. Test en la Task 4.
5. **Password de auth SQL con `;` o `}`**: no debe romper el connection string. Test en la Task 1.

---

## File Structure

| Archivo | Acción | Responsabilidad |
|---|---|---|
| `centraal_dataframework/synapse.py` | Crear | `SynapseClient`, `get_synapse`, `render`, conexión, reintentos |
| `centraal_dataframework/settings.py` | Modificar | Variables `SYNAPSE_*` |
| `centraal_dataframework/excepciones.py` | Modificar | `SynapseNoConfigurado` |
| `centraal_dataframework/tasks.py` | Modificar | `TaskContext` perezoso + `synapse` + `synapse_for` |
| `centraal_dataframework/__init__.py` | Modificar | Exportar `SynapseClient` |
| `pyproject.toml`, `poetry.lock` | Modificar | Extra `[synapse]` con `pyodbc` |
| `setup.cfg` | Modificar | Marker `synapse` de pytest |
| `tests/test_synapse.py` | Crear | Unitarias con un `pyodbc` falso |
| `tests/test_synapse_integration.py` | Crear | `SELECT 1` real; se salta sin configuración |
| `tests/test_tasks.py` | Modificar | Pruebas de pereza de `TaskContext` |
| `README.md`, `CHANGELOG.md` | Modificar | Uso y entrada en `[Unreleased]` |

---

### Task 1: `SynapseClient` — configuración, conexión y lectura

**Files:**
- Create: `centraal_dataframework/synapse.py`
- Modify: `centraal_dataframework/settings.py` (imports y campos, después de `notify_on_success`)
- Modify: `centraal_dataframework/excepciones.py` (al final)
- Test: `tests/test_synapse.py`

**Interfaces:**
- Consumes: `Settings`, `get_settings` (`centraal_dataframework/settings.py`).
- Produces:
  - `SynapseClient(server: str, database: Optional[str] = None, auth: str = 'identity', user: Optional[str] = None, password: Optional[str] = None, query_timeout: int = 1800, login_timeout: int = 30, max_attempts: int = 3, credential: Any = None)`
  - `SynapseClient.from_settings(settings: Optional[Settings] = None) -> SynapseClient`
  - `SynapseClient.for_database(database: str) -> SynapseClient`
  - `SynapseClient.connection_string() -> str`
  - `SynapseClient.connect() -> pyodbc.Connection`
  - `SynapseClient.query(sql: str, *params) -> List[Dict[str, Any]]`
  - `SynapseClient.scalar(sql: str, *params) -> Any`
  - `SynapseClient.execute(sql: str, *params) -> int` (rowcount, `-1` si el driver no lo informa)
  - `SynapseClient.read_df(sql: str, *params) -> pandas.DataFrame`
  - `SynapseClient._run(sql: str, params: tuple, fetch: Callable[[cursor], Any]) -> Any` (la Task 3 le agrega reintentos)
  - `get_synapse(settings: Optional[Settings] = None) -> SynapseClient`
  - Módulo: `_import_pyodbc()`, `_sleep` (alias de `time.sleep`, se parchea en tests), `_token_struct(token: str) -> bytes`
  - `excepciones.SynapseNoConfigurado(Exception)`

- [ ] **Step 1: Write the failing tests**

Crear `tests/test_synapse.py`:

```python
"""Tests de SynapseClient con un pyodbc falso (sin red)."""

from __future__ import annotations

import struct
import sys
from types import SimpleNamespace

import pytest

from centraal_dataframework import synapse as syn
from centraal_dataframework.excepciones import SynapseNoConfigurado
from centraal_dataframework.settings import Settings
from centraal_dataframework.synapse import SynapseClient


class FakeError(Exception):
    """Base de errores del pyodbc falso."""


class FakeOperationalError(FakeError):
    """Error operacional (conexión, timeout)."""


class FakeProgrammingError(FakeError):
    """Error de SQL."""


class FakeCursor:
    """Cursor que consume el siguiente resultado programado."""

    def __init__(self, conn):
        """Guarda la conexión dueña."""
        self.conn = conn
        self.description = None
        self.rowcount = -1
        self._rows = []

    def execute(self, sql, *params):
        """Registra la sentencia y aplica el siguiente resultado."""
        self.conn.module.executed.append((sql, params))
        outcome = self.conn.module.outcomes.pop(0) if self.conn.module.outcomes else {}
        if isinstance(outcome, Exception):
            raise outcome
        cols = outcome.get('cols') or []
        self.description = [(c, None) for c in cols] or None
        self._rows = [tuple(r) for r in outcome.get('rows', [])]
        self.rowcount = outcome.get('rowcount', len(self._rows) if cols else -1)

    def fetchall(self):
        """Devuelve todas las filas."""
        return list(self._rows)

    def fetchone(self):
        """Devuelve la primera fila o None."""
        return self._rows[0] if self._rows else None


class FakeConnection:
    """Conexión falsa."""

    def __init__(self, module, conn_str, autocommit, attrs_before):
        """Guarda los argumentos de connect."""
        self.module = module
        self.conn_str = conn_str
        self.autocommit = autocommit
        self.attrs_before = attrs_before
        self.timeout = 0
        self.closed = False

    def cursor(self):
        """Crea un cursor."""
        return FakeCursor(self)

    def close(self):
        """Cierra la conexión."""
        self.closed = True


class FakePyodbc:
    """Módulo pyodbc falso con resultados programables."""

    Error = FakeError
    OperationalError = FakeOperationalError
    ProgrammingError = FakeProgrammingError

    def __init__(self):
        """Inicia sin resultados."""
        self.outcomes = []
        self.executed = []
        self.connections = []

    def connect(self, conn_str, autocommit=False, attrs_before=None):
        """Crea una conexión falsa."""
        conn = FakeConnection(self, conn_str, autocommit, attrs_before)
        self.connections.append(conn)
        return conn


class FakeCredential:
    """Credencial que entrega un token fijo."""

    def __init__(self):
        """Inicia el registro de scopes."""
        self.scopes = []

    def get_token(self, scope):
        """Devuelve un token falso."""
        self.scopes.append(scope)
        return SimpleNamespace(token='tok')


@pytest.fixture
def fake_odbc(monkeypatch):
    """Reemplaza pyodbc y sleep."""
    fake = FakePyodbc()
    monkeypatch.setattr(syn, '_import_pyodbc', lambda: fake)
    monkeypatch.setattr(syn, '_sleep', lambda s: fake.executed.append(('sleep', s)))
    return fake


@pytest.fixture
def client():
    """Cliente con credencial falsa."""
    return SynapseClient(server='ws-ondemand.sql.azuresynapse.net', database='silver', credential=FakeCredential())


def test_from_settings_derives_serverless_endpoint():
    """Solo workspace: usa el endpoint -ondemand."""
    c = SynapseClient.from_settings(Settings(synapse_workspace='ws', synapse_database='bronze'))
    assert c.server == 'ws-ondemand.sql.azuresynapse.net'
    assert c.database == 'bronze'
    assert c.auth == 'identity'
    assert c.query_timeout == 1800


def test_from_settings_server_overrides_workspace():
    """SYNAPSE_SERVER explícito gana sobre el workspace."""
    c = SynapseClient.from_settings(Settings(synapse_workspace='ws', synapse_server='ws.sql.azuresynapse.net'))
    assert c.server == 'ws.sql.azuresynapse.net'


def test_from_settings_without_config_raises():
    """Sin workspace ni server: error claro."""
    with pytest.raises(SynapseNoConfigurado):
        SynapseClient.from_settings(Settings())


def test_sql_auth_requires_user_and_password():
    """Auth sql sin usuario o clave: error claro."""
    with pytest.raises(SynapseNoConfigurado):
        SynapseClient.from_settings(Settings(synapse_workspace='ws', synapse_auth='sql', synapse_user='u'))


def test_invalid_auth_rejected_by_settings():
    """Auth desconocida no pasa la validación."""
    with pytest.raises(Exception):
        Settings(synapse_auth='kerberos')


def test_connection_string_identity_has_no_credentials(client):
    """Con identidad no hay Uid/Pwd y va cifrado."""
    cs = client.connection_string()
    assert 'Driver={ODBC Driver 18 for SQL Server}' in cs
    assert 'Server=tcp:ws-ondemand.sql.azuresynapse.net,1433' in cs
    assert 'Database=silver' in cs
    assert 'Encrypt=yes' in cs
    assert 'Uid=' not in cs and 'Pwd=' not in cs


def test_connection_string_escapes_password():
    """Password con ; y } no rompe el connection string."""
    c = SynapseClient(server='s', auth='sql', user='u', password='a;b}c')
    assert 'Pwd={a;b}}c};' in c.connection_string()


def test_query_uses_token_autocommit_and_timeout(fake_odbc, client):
    """query devuelve dicts; conecta con token, autocommit y timeout."""
    fake_odbc.outcomes.append({'cols': ['id', 'n'], 'rows': [(1, 'a'), (2, 'b')]})
    rows = client.query('SELECT id, n FROM t WHERE x = ?', 5)
    assert rows == [{'id': 1, 'n': 'a'}, {'id': 2, 'n': 'b'}]
    conn = fake_odbc.connections[0]
    assert conn.autocommit is True
    assert conn.timeout == 1800
    assert conn.closed is True
    raw = 'tok'.encode('utf-16-le')
    assert conn.attrs_before == {1256: struct.pack(f'<I{len(raw)}s', len(raw), raw)}
    assert client.credential.scopes == ['https://database.windows.net/.default']
    assert fake_odbc.executed[0] == ('SELECT id, n FROM t WHERE x = ?', (5,))


def test_sql_auth_sends_no_token(fake_odbc):
    """Con auth sql no se pasa token."""
    c = SynapseClient(server='s', auth='sql', user='u', password='p')
    fake_odbc.outcomes.append({'cols': ['x'], 'rows': [(1,)]})
    c.scalar('SELECT 1')
    assert fake_odbc.connections[0].attrs_before == {}


def test_scalar_execute_and_empty_results(fake_odbc, client):
    """scalar, execute y consultas sin result set."""
    fake_odbc.outcomes += [
        {'cols': ['c'], 'rows': [(42,)]},
        {'cols': ['c'], 'rows': []},
        {'rowcount': 3},
        {},
    ]
    assert client.scalar('SELECT 42') == 42
    assert client.scalar('SELECT c FROM vacia') is None
    assert client.execute('CREATE VIEW v AS SELECT 1') == 3
    assert client.query('DROP VIEW v') == []


def test_read_df(fake_odbc, client):
    """read_df arma un DataFrame con nombres de columna."""
    fake_odbc.outcomes.append({'cols': ['a', 'b'], 'rows': [(1, 'x')]})
    df = client.read_df('SELECT a, b FROM t')
    assert list(df.columns) == ['a', 'b']
    assert df.iloc[0]['b'] == 'x'


def test_for_database_keeps_config(client):
    """for_database cambia solo la base y comparte la credencial."""
    other = client.for_database('gold')
    assert other.database == 'gold'
    assert other.server == client.server
    assert other.credential is client.credential
    assert client.database == 'silver'


def test_missing_pyodbc_raises_clear_error(monkeypatch):
    """Sin pyodbc instalado: error que dice qué instalar."""
    monkeypatch.setitem(sys.modules, 'pyodbc', None)
    with pytest.raises(SynapseNoConfigurado, match=r'\[synapse\]'):
        syn._import_pyodbc()


def test_get_synapse_uses_settings(monkeypatch):
    """get_synapse lee las Settings del entorno."""
    monkeypatch.setenv('SYNAPSE_WORKSPACE', 'ws')
    assert syn.get_synapse().server == 'ws-ondemand.sql.azuresynapse.net'
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_synapse.py -v`
Expected: falla la colección con `ModuleNotFoundError: No module named 'centraal_dataframework.synapse'` o `ImportError: cannot import name 'SynapseNoConfigurado'`.

- [ ] **Step 3: Add exception and settings**

Al final de `centraal_dataframework/excepciones.py`:

```python
class SynapseNoConfigurado(Exception):
    """Excepción cuando falta configuración o dependencias de Synapse."""
```

En `centraal_dataframework/settings.py`, cambiar los imports:

```python
from typing import Literal, Optional
from zoneinfo import ZoneInfo

from pydantic import Field, SecretStr, field_validator
```

y agregar estos campos después de `notify_on_success`:

```python
    synapse_workspace: Optional[str] = Field(default=None, validation_alias='SYNAPSE_WORKSPACE')
    synapse_server: Optional[str] = Field(default=None, validation_alias='SYNAPSE_SERVER')
    synapse_database: Optional[str] = Field(default=None, validation_alias='SYNAPSE_DATABASE')
    synapse_auth: Literal['identity', 'sql'] = Field(default='identity', validation_alias='SYNAPSE_AUTH')
    synapse_user: Optional[str] = Field(default=None, validation_alias='SYNAPSE_USER')
    synapse_password: Optional[SecretStr] = Field(default=None, validation_alias='SYNAPSE_PASSWORD')
    synapse_query_timeout: int = Field(default=1800, validation_alias='SYNAPSE_QUERY_TIMEOUT')
```

- [ ] **Step 4: Create `centraal_dataframework/synapse.py`**

```python
"""Cliente mínimo para Synapse SQL (serverless primero)."""

from __future__ import annotations

import logging
import struct
import time
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Dict, List, Optional

from centraal_dataframework.excepciones import SynapseNoConfigurado
from centraal_dataframework.settings import Settings, get_settings

logger = logging.getLogger(__name__)

DRIVER = 'ODBC Driver 18 for SQL Server'
TOKEN_SCOPE = 'https://database.windows.net/.default'
SQL_COPT_SS_ACCESS_TOKEN = 1256
_sleep = time.sleep


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
        """Ejecuta una sentencia en una conexión nueva y aplica fetch."""
        start = time.monotonic()
        conn = self.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, *params)
            result = fetch(cursor)
        finally:
            conn.close()
        logger.info("Synapse [%s] %.2fs", self.database or 'default', time.monotonic() - start)
        return result

    def query(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        """Ejecuta un SELECT y devuelve filas como diccionarios."""
        return self._run(sql, params, _fetch_rows)

    def scalar(self, sql: str, *params: Any) -> Any:
        """Ejecuta y devuelve el primer valor."""
        return self._run(sql, params, _fetch_scalar)

    def execute(self, sql: str, *params: Any) -> int:
        """Ejecuta DDL/DML/CETAS y devuelve rowcount (-1 si no aplica)."""
        return self._run(sql, params, lambda cursor: cursor.rowcount)

    def read_df(self, sql: str, *params: Any) -> Any:
        """Ejecuta un SELECT y devuelve un pandas.DataFrame (solo resultados pequeños)."""
        import pandas as pd

        cols, rows = self._run(sql, params, _fetch_table)
        return pd.DataFrame.from_records(rows, columns=cols)


def get_synapse(settings: Optional[Settings] = None) -> SynapseClient:
    """Obtiene un SynapseClient desde Settings."""
    return SynapseClient.from_settings(settings)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `poetry run pytest tests/test_synapse.py -v`
Expected: todos PASS.

- [ ] **Step 6: Commit** (solo si se cumple la precondición)

```bash
git add centraal_dataframework/synapse.py centraal_dataframework/settings.py centraal_dataframework/excepciones.py tests/test_synapse.py
git commit -m "feat: cliente SynapseClient para consultas a Synapse serverless"
```

---

### Task 2: Plantillas SQL con parámetros en lista blanca y archivos con `GO`

**Files:**
- Modify: `centraal_dataframework/synapse.py`
- Test: `tests/test_synapse.py` (agregar al final)

**Interfaces:**
- Consumes: `SynapseClient.execute` (Task 1).
- Produces:
  - `render(sql: str, **params: Any) -> str` (función del módulo)
  - `SynapseClient.execute_file(path: str | os.PathLike, **params: Any) -> int` (suma de rowcounts no negativos)

Por qué: Synapse serverless no acepta parámetros `?` en DDL (`CREATE VIEW`, CETAS; spec §5). Los valores se sustituyen en el texto **solo** si pasan la lista blanca. `CREATE VIEW` tiene que ser la única sentencia de su lote, por eso un archivo puede separar lotes con `GO`.

- [ ] **Step 1: Write the failing tests**

Cambiar el import del inicio de `tests/test_synapse.py` a
`from centraal_dataframework.synapse import SynapseClient, render` y agregar al final:

```python
def test_render_substitutes_params():
    """Sustituye {{x}} y {{ x }}."""
    sql = "SELECT * FROM bronze.{{box}}.t WHERE fecha = '{{ lote }}'"
    assert render(sql, box='demo_1', lote='2026-10-09') == "SELECT * FROM bronze.demo_1.t WHERE fecha = '2026-10-09'"


def test_render_accepts_timestamps_and_hex_ids():
    """Fechas con hora e ids hex pasan."""
    assert render('{{a}}|{{b}}', a='2026-10-09T08:00:00', b='9f1c2a') == '2026-10-09T08:00:00|9f1c2a'


@pytest.mark.parametrize('bad', ["x'; DROP VIEW v; --", 'a b', 'a--b', '', 'a/*', 'a;b', "o'neil", '-a', 'a-'])
def test_render_rejects_unsafe_values(bad):
    """Valores fuera de la lista blanca: ValueError."""
    with pytest.raises(ValueError):
        render('SELECT {{v}}', v=bad)


def test_render_missing_param():
    """Parámetro faltante: ValueError con el nombre."""
    with pytest.raises(ValueError, match='lote'):
        render('SELECT {{lote}}')


def test_execute_file_splits_go_batches(fake_odbc, client, tmp_path):
    """execute_file renderiza y ejecuta cada lote GO por separado."""
    path = tmp_path / 'v.sql'
    path.write_text(
        "CREATE OR ALTER VIEW {{box}}.v AS SELECT 1 AS x\nGO\n"
        "  go ;  \nCREATE OR ALTER VIEW {{box}}.w AS SELECT 2 AS y\n",
        encoding='utf-8',
    )
    fake_odbc.outcomes += [{'rowcount': -1}, {'rowcount': 2}]
    assert client.execute_file(path, box='demo') == 2
    sqls = [s for s, _ in fake_odbc.executed]
    assert sqls == ['CREATE OR ALTER VIEW demo.v AS SELECT 1 AS x', 'CREATE OR ALTER VIEW demo.w AS SELECT 2 AS y']


def test_execute_file_does_not_split_go_inside_identifier(fake_odbc, client, tmp_path):
    """Solo una línea que es exactamente GO separa lotes."""
    path = tmp_path / 'q.sql'
    path.write_text('SELECT category FROM t -- GOod\n', encoding='utf-8')
    client.execute_file(path)
    assert len(fake_odbc.executed) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_synapse.py -v -k "render or execute_file"`
Expected: FAIL con `ImportError: cannot import name 'render'`.

- [ ] **Step 3: Implement**

En `synapse.py`, agregar `import os`, `import re` y `from pathlib import Path` a los imports. Agregar estas constantes después de `_sleep`:

```python
_PARAM_RE = re.compile(r'\{\{\s*(\w+)\s*\}\}')
# Alfanumérico/_ separado por un solo '-', ':' o '.': fechas, timestamps, ids. Bloquea --, /*, comillas y espacios.
_SAFE_VALUE_RE = re.compile(r'^[A-Za-z0-9_]+(?:[-:.][A-Za-z0-9_]+)*$')
_GO_RE = re.compile(r'^\s*GO\s*;?\s*$', re.IGNORECASE | re.MULTILINE)
```

Agregar esta función antes de la clase:

```python
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
```

Agregar este método a `SynapseClient`, después de `execute`:

```python
    def execute_file(self, path: str | os.PathLike, **params: Any) -> int:
        """Renderiza un .sql y ejecuta cada lote separado por GO."""
        text = render(Path(path).read_text(encoding='utf-8'), **params)
        total = 0
        for batch in (b.strip() for b in _GO_RE.split(text)):
            if batch:
                total += max(self.execute(batch), 0)
        return total
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_synapse.py -v`
Expected: todos PASS.

- [ ] **Step 5: Commit**

```bash
git add centraal_dataframework/synapse.py tests/test_synapse.py
git commit -m "feat: plantillas SQL con parametros en lista blanca y lotes GO"
```

---

### Task 3: Reintentos solo ante errores transitorios

**Files:**
- Modify: `centraal_dataframework/synapse.py` (`_run` y constantes)
- Test: `tests/test_synapse.py` (agregar al final)

**Interfaces:**
- Consumes: `SynapseClient._run`, `_sleep`, `_import_pyodbc` (Task 1).
- Produces: `_is_transient(exc: Exception) -> bool`. `_run` hace hasta `max_attempts` intentos con espera `2 ** intento` s.

Regla (spec §9.3): reintentar conexión, timeout y throttling. **No** reintentar errores de SQL ni login fallido o bloqueado (18456, R-042).

- [ ] **Step 1: Write the failing tests**

```python
def test_retries_transient_then_succeeds(fake_odbc, client):
    """08S01 se reintenta con espera exponencial."""
    fake_odbc.outcomes += [
        FakeOperationalError('08S01', 'Communication link failure'),
        FakeOperationalError('42000', '[40613] Database is not currently available'),
        {'cols': ['x'], 'rows': [(1,)]},
    ]
    assert client.scalar('SELECT 1') == 1
    assert [e for e in fake_odbc.executed if e[0] == 'sleep'] == [('sleep', 2), ('sleep', 4)]
    assert all(c.closed for c in fake_odbc.connections)


def test_gives_up_after_max_attempts(fake_odbc, client):
    """Tras max_attempts propaga el último error."""
    fake_odbc.outcomes += [FakeOperationalError('HYT00', 'timeout')] * 3
    with pytest.raises(FakeOperationalError):
        client.scalar('SELECT 1')
    assert len(fake_odbc.connections) == 3


def test_programming_error_not_retried(fake_odbc, client):
    """Error de SQL (p.ej. CETAS ya existe) falla al primer intento."""
    fake_odbc.outcomes.append(FakeProgrammingError('42S01', 'There is already an object named t'))
    with pytest.raises(FakeProgrammingError):
        client.execute('CREATE EXTERNAL TABLE t AS SELECT 1 AS x')
    assert len(fake_odbc.connections) == 1


def test_login_failure_not_retried(fake_odbc, client, monkeypatch):
    """Login fallido (18456) no se reintenta (R-042: no agravar el bloqueo)."""

    def boom(*a, **k):
        raise FakeError('28000', "[18456] Login failed for user '<token-identified principal>'")

    monkeypatch.setattr(fake_odbc, 'connect', boom)
    with pytest.raises(FakeError):
        client.scalar('SELECT 1')
    assert not [e for e in fake_odbc.executed if e[0] == 'sleep']


def test_connect_transient_error_is_retried(fake_odbc, client, monkeypatch):
    """Un error transitorio al conectar también se reintenta."""
    real_connect = fake_odbc.connect
    calls = []

    def flaky(*a, **k):
        calls.append(1)
        if len(calls) == 1:
            raise FakeOperationalError('08001', 'TCP Provider: timeout')
        return real_connect(*a, **k)

    monkeypatch.setattr(fake_odbc, 'connect', flaky)
    fake_odbc.outcomes.append({'cols': ['x'], 'rows': [(7,)]})
    assert client.scalar('SELECT 7') == 7
    assert len(calls) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_synapse.py -v -k "retr or not_retried or attempts"`
Expected: FAIL. El primer `FakeOperationalError` se propaga sin reintento.

- [ ] **Step 3: Implement**

Agregar estas constantes en `synapse.py`:

```python
TRANSIENT_SQLSTATES = frozenset({'08S01', '08001', 'HYT00', 'HYT01', '40001'})
TRANSIENT_CODES = ('40613', '40501', '40197', '10928', '10929', '49918')
```

Agregar esta función antes de la clase:

```python
def _is_transient(exc: Exception) -> bool:
    """True para conexión, timeout y throttling; False para SQL y login (18456)."""
    sqlstate = str(exc.args[0]) if exc.args else ''
    message = ' '.join(str(a) for a in exc.args)
    if '18456' in message:
        return False
    return sqlstate in TRANSIENT_SQLSTATES or any(code in message for code in TRANSIENT_CODES)
```

Reemplazar `_run` por:

```python
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
                logger.warning("Synapse error transitorio (intento %s/%s), espera %ss: %s", attempt, self.max_attempts, wait, exc)
                _sleep(wait)
                continue
            logger.info("Synapse [%s] %.2fs", self.database or 'default', time.monotonic() - start)
            return result
        raise RuntimeError('unreachable')  # pragma: no cover
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_synapse.py -v`
Expected: todos PASS.

- [ ] **Step 5: Commit**

```bash
git add centraal_dataframework/synapse.py tests/test_synapse.py
git commit -m "feat: reintentos de Synapse solo ante errores transitorios"
```

---

### Task 4: `TaskContext` perezoso con `ctx.synapse` y `ctx.synapse_for`

**Files:**
- Modify: `centraal_dataframework/tasks.py:5-29` (imports y `TaskContext`) y `:65-72` (construcción en `_wrap_task`)
- Modify: `centraal_dataframework/__init__.py`
- Test: `tests/test_tasks.py` (agregar al final)

**Interfaces:**
- Consumes: `get_synapse() -> SynapseClient`, `SynapseClient.for_database(db)` (Task 1); `get_datalake()` (`resources.py`).
- Produces:
  - `TaskContext(name, run_id, logger, params={}, gx=None, datalake_factory=get_datalake, synapse_factory=get_synapse)`
  - `TaskContext.datalake` (propiedad perezosa, se crea una vez)
  - `TaskContext.synapse -> SynapseClient` (propiedad perezosa, se crea una vez)
  - `TaskContext.synapse_for(database: str) -> SynapseClient`
  - `centraal_dataframework.SynapseClient` exportado.

Nota de compatibilidad: `datalake` deja de ser argumento de construcción. En el repo nadie construye `TaskContext` a mano; solo `_wrap_task` (verificado con grep). Los tests existentes parchean `centraal_dataframework.tasks.get_datalake`. Por eso `_wrap_task` pasa `datalake_factory=get_datalake`, resuelto en el momento de la llamada, y esos tests siguen funcionando sin cambios.

- [ ] **Step 1: Write the failing tests**

Agregar al final de `tests/test_tasks.py`:

```python
class _FakeSynapse:
    """SynapseClient falso."""

    def __init__(self, database=None):
        """Guarda la base."""
        self.database = database

    def for_database(self, database):
        """Copia con otra base."""
        return _FakeSynapse(database)


def test_context_is_lazy_when_resources_unused(runner, monkeypatch):
    """Si la tarea no usa datalake ni synapse, no se crean (tarea sin storage configurado)."""
    calls = []

    def no_storage():
        calls.append('dl')
        raise AssertionError('no debe crearse')

    monkeypatch.setattr('centraal_dataframework.tasks.get_datalake', no_storage)
    monkeypatch.setattr('centraal_dataframework.tasks.get_synapse', lambda: calls.append('syn'))

    @task(name='alpha')
    def alpha(ctx: TaskContext):
        return 'ok'

    assert runner.run_task('alpha', notify=False) == 'ok'
    assert calls == []


def test_context_synapse_created_once_and_synapse_for(runner, monkeypatch):
    """ctx.synapse se crea una vez; synapse_for cambia la base."""
    created = []

    def factory():
        created.append(1)
        return _FakeSynapse('default')

    monkeypatch.setattr('centraal_dataframework.tasks.get_synapse', factory)

    @task(name='alpha')
    def alpha(ctx: TaskContext):
        assert ctx.synapse is ctx.synapse
        return ctx.synapse_for('silver').database

    assert runner.run_task('alpha', notify=False) == 'silver'
    assert created == [1]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_tasks.py -v`
Expected: FAIL. `get_synapse` no existe en `tasks` (`AttributeError` en monkeypatch) y `get_datalake` se llama de forma ansiosa.

- [ ] **Step 3: Implement**

En `centraal_dataframework/tasks.py`, ajustar los imports:

```python
from typing import Any, Callable, Dict, Optional, Union

from centraal_dataframework.excepciones import ErrorTareaCalidadDatos
from centraal_dataframework.resources import DQResult, GreatExpectationsToolKit, get_context, get_datalake
from centraal_dataframework.runner import get_runner
from centraal_dataframework.synapse import SynapseClient, get_synapse
from centraal_dataframework.utils import new_run_id
```

Reemplazar la clase `TaskContext` por:

```python
@dataclass
class TaskContext:
    """Contexto inyectado a cada tarea. datalake y synapse se crean al primer uso."""

    name: str
    run_id: str
    logger: logging.Logger
    params: Dict[str, Any] = field(default_factory=dict)
    gx: Optional[GreatExpectationsToolKit] = None
    datalake_factory: Callable[[], Any] = field(default=get_datalake, repr=False)
    synapse_factory: Callable[[], SynapseClient] = field(default=get_synapse, repr=False)
    _datalake: Any = field(default=None, init=False, repr=False)
    _synapse: Optional[SynapseClient] = field(default=None, init=False, repr=False)

    @property
    def datalake(self) -> Any:
        """Cliente Datalake (perezoso)."""
        if self._datalake is None:
            self._datalake = self.datalake_factory()
        return self._datalake

    @property
    def synapse(self) -> SynapseClient:
        """Cliente Synapse con la base por defecto (perezoso)."""
        if self._synapse is None:
            self._synapse = self.synapse_factory()
        return self._synapse

    def synapse_for(self, database: str) -> SynapseClient:
        """Cliente Synapse para otra base (p.ej. bronze, silver, gold)."""
        return self.synapse.for_database(database)
```

En `_wrap_task`, reemplazar la construcción de `ctx` por:

```python
        ctx = TaskContext(
            name=task_name,
            run_id=run_id,
            logger=adapter,  # type: ignore[arg-type]
            params={**params, **kwargs},
            gx=gx_toolkit,
            datalake_factory=get_datalake,
            synapse_factory=get_synapse,
        )
```

En `centraal_dataframework/__init__.py`, agregar `from centraal_dataframework.synapse import SynapseClient` antes del import de `tasks` y `'SynapseClient',` a `__all__`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest -v`
Expected: toda la suite PASS, incluidos `tests/test_tasks.py` y `tests/test_runner.py`, que parchean `tasks.get_datalake`.

- [ ] **Step 5: Commit**

```bash
git add centraal_dataframework/tasks.py centraal_dataframework/__init__.py tests/test_tasks.py
git commit -m "feat: TaskContext perezoso con ctx.synapse y synapse_for"
```

---

### Task 5: Empaquetado, prueba de integración y documentación

**Files:**
- Modify: `pyproject.toml` (después de `croniter`, y una sección nueva antes de `[tool.poetry.group.test.dependencies]`)
- Modify: `poetry.lock` (regenerado)
- Modify: `setup.cfg` (sección nueva)
- Create: `tests/test_synapse_integration.py`
- Modify: `README.md`, `CHANGELOG.md` (`[Unreleased]`)

**Interfaces:**
- Consumes: `get_synapse`, `SynapseClient.scalar/query/execute_file` (Tasks 1–3).
- Produces: extra instalable `centraal-dataframework[synapse]` y marker `synapse` de pytest.

- [ ] **Step 1: Write the integration test**

Crear `tests/test_synapse_integration.py`:

```python
"""Integración contra un Synapse real. Se salta sin SYNAPSE_WORKSPACE/SYNAPSE_SERVER o sin pyodbc."""

import os

import pytest

pytestmark = [
    pytest.mark.synapse,
    pytest.mark.skipif(
        not (os.environ.get('SYNAPSE_WORKSPACE') or os.environ.get('SYNAPSE_SERVER')),
        reason='Synapse no configurado',
    ),
]


@pytest.fixture
def client():
    """Cliente real desde el entorno."""
    pytest.importorskip('pyodbc')
    from centraal_dataframework.synapse import get_synapse

    return get_synapse()


def test_select_1(client):
    """Conecta con identidad y ejecuta SELECT 1."""
    assert client.scalar('SELECT 1') == 1


def test_parameters_supported_on_serverless(client):
    """[supuesto del spec] serverless acepta parámetros ? en un SELECT."""
    assert client.query('SELECT ? AS x', 'a') == [{'x': 'a'}]
```

- [ ] **Step 2: Run it to verify it is skipped without configuration**

Run: `poetry run pytest tests/test_synapse_integration.py -v`
Expected: 2 SKIPPED (`Synapse no configurado`).

- [ ] **Step 3: Register the marker and the optional dependency**

En `setup.cfg`, agregar esta sección después de `[coverage:report]`:

```ini
[tool:pytest]
markers =
    synapse: integración contra Synapse real (requiere SYNAPSE_WORKSPACE o SYNAPSE_SERVER y pyodbc)
```

En `pyproject.toml`, agregar esto dentro de `[tool.poetry.dependencies]`, después de `croniter`:

```toml
pyodbc = { version = ">=5.1,<6.0", optional = true }
```

y esta sección nueva antes de `[tool.poetry.group.test.dependencies]`:

```toml
[tool.poetry.extras]
synapse = ["pyodbc"]
```

Run: `poetry --version`, después `poetry lock` en Poetry 2.x o `poetry lock --no-update` en Poetry 1.x.
Expected: `poetry.lock` actualizado sin subir versiones de otras dependencias. Revisar con `git diff --stat poetry.lock`.

- [ ] **Step 4: Document**

En `README.md`, agregar esta sección después del ejemplo de tareas:

````markdown
## Synapse (serverless primero)

Instalar con el extra y el driver ODBC del sistema (`ODBC Driver 18 for SQL Server`):

```bash
pip install "centraal-dataframework[synapse]"
```

Variables de entorno:

| Variable | Default | Uso |
|---|---|---|
| `SYNAPSE_WORKSPACE` | — | Nombre del workspace; usa `<workspace>-ondemand.sql.azuresynapse.net` (serverless) |
| `SYNAPSE_SERVER` | — | Servidor explícito (p.ej. pool dedicado); tiene prioridad |
| `SYNAPSE_DATABASE` | — | Base por defecto |
| `SYNAPSE_AUTH` | `identity` | `identity` (Managed Identity / `DefaultAzureCredential`) o `sql` |
| `SYNAPSE_USER` / `SYNAPSE_PASSWORD` | — | Solo con `SYNAPSE_AUTH=sql` (desarrollo local) |
| `SYNAPSE_QUERY_TIMEOUT` | `1800` | Segundos por consulta |

```python
@task
def contar(ctx: TaskContext):
    bronze = ctx.synapse_for('bronze')
    n = bronze.scalar("SELECT COUNT(*) FROM demo.ventas WHERE fecha = ?", ctx.params['lote'])
    ctx.logger.info("filas=%s", n)

@task
def crear_vista(ctx: TaskContext):
    # DDL: sin parámetros ?, se usa {{param}} con lista blanca; lotes separados por GO
    ctx.synapse_for('silver').execute_file('sql/vista.sql', box='demo', lote=ctx.params['lote'])
```

`query`, `scalar` y `read_df` traen resultados a la Function: úselos para conteos y metadata,
no para mover datos. El procesamiento pesado va en SQL dentro de Synapse.
````

En `CHANGELOG.md`, bajo `## [Unreleased]`:

```markdown
### Added

- `SynapseClient` (`ctx.synapse`, `ctx.synapse_for(base)`): consultas a Synapse SQL, serverless por defecto, con Managed Identity, reintentos ante errores transitorios y plantillas `{{param}}` con lista blanca. Extra opcional `[synapse]` (`pyodbc`).

### Changed

- `TaskContext.datalake` se crea al primer uso; `TaskContext` ya no recibe `datalake` en el constructor.
```

- [ ] **Step 5: Full verification**

Run:
```bash
poetry run pytest -v
poetry run flake8 centraal_dataframework tests
poetry run black --check centraal_dataframework tests
poetry run isort --check-only centraal_dataframework tests
```
Expected: pytest todo PASS, salvo los 2 SKIPPED de integración. flake8 sin errores. black e isort sin cambios; si reportan algo, correr `black` o `isort` sin `--check` y repetir.

- [ ] **Step 6 (opcional, manual): integración real**

Con `az login` y la IP permitida en el firewall del workspace:

```bash
poetry install --extras synapse
SYNAPSE_WORKSPACE=<workspace> SYNAPSE_DATABASE=master poetry run pytest -m synapse -v
```
Expected: 2 PASS. Si `test_parameters_supported_on_serverless` falla, anotarlo: los `?` no sirven en serverless y el README debe recomendar `render` también para `SELECT`.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml poetry.lock setup.cfg tests/test_synapse_integration.py README.md CHANGELOG.md
git commit -m "build: extra synapse con pyodbc, prueba de integracion y documentacion"
```

---

## Fuera de alcance (planes siguientes, spec §13)

`paso_sql` declarativo (c), GX sobre SQL y esquema `<box>_calidad` (d), CETAS versionado y vistas puntero (e), encadenamiento por cola y lote (f), triggers (g) y jobs (h). El cliente de este plan es la base sobre la que se construyen.
