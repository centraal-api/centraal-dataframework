"""Tests de SynapseClient con un pyodbc falso (sin red)."""

from __future__ import annotations

import struct
import sys
from types import SimpleNamespace

import pytest

from centraal_dataframework import synapse as syn
from centraal_dataframework.excepciones import SynapseNoConfigurado
from centraal_dataframework.settings import Settings
from centraal_dataframework.synapse import SynapseClient, render


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


@pytest.fixture(autouse=True)
def _clean_synapse_env(monkeypatch):
    """Aísla los tests de variables SYNAPSE_* exportadas en la máquina."""
    for name in ('WORKSPACE', 'SERVER', 'DATABASE', 'AUTH', 'USER', 'PASSWORD', 'QUERY_TIMEOUT', 'ODBC_DRIVER'):
        monkeypatch.delenv(f'SYNAPSE_{name}', raising=False)


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
    """Query devuelve dicts; conecta con token, autocommit y timeout."""
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


def test_retries_transient_then_succeeds(fake_odbc, client):
    """08S01 y 40613 se reintentan con espera exponencial."""
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
    """Login fallido (18456) no se reintenta (no agravar el bloqueo de la cuenta)."""

    def boom(*a, **k):
        raise FakeOperationalError('08001', "[18456] Login failed for user '<token-identified principal>'")

    monkeypatch.setattr(fake_odbc, 'connect', boom)
    with pytest.raises(FakeOperationalError):
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


def test_odbc_driver_is_configurable():
    """SYNAPSE_ODBC_DRIVER cambia el driver del connection string (por si la imagen solo trae el 17)."""
    c = SynapseClient.from_settings(
        Settings(synapse_workspace='ws', synapse_odbc_driver='ODBC Driver 17 for SQL Server')
    )
    assert c.odbc_driver == 'ODBC Driver 17 for SQL Server'
    assert 'Driver={ODBC Driver 17 for SQL Server}' in c.connection_string()


def test_odbc_driver_default_is_18():
    """Por defecto se usa el ODBC Driver 18."""
    assert Settings().synapse_odbc_driver == 'ODBC Driver 18 for SQL Server'
    assert SynapseClient(server='s').odbc_driver == 'ODBC Driver 18 for SQL Server'


def test_sqlalchemy_url_wraps_the_odbc_connection_string(client):
    """La URL de SQLAlchemy lleva el mismo connection string ODBC en odbc_connect."""
    from urllib.parse import parse_qs, urlsplit

    url = client.sqlalchemy_url()
    assert url.startswith('mssql+pyodbc:///?odbc_connect=')
    assert parse_qs(urlsplit(url).query)['odbc_connect'] == [client.connection_string()]


def test_sqlalchemy_kwargs_identity_passes_token(client):
    """Con identidad, el token de Entra va en attrs_before como en connect()."""
    raw = 'tok'.encode('utf-16-le')
    assert client.sqlalchemy_kwargs() == {
        'connect_args': {'attrs_before': {1256: struct.pack(f'<I{len(raw)}s', len(raw), raw)}}
    }


def test_sqlalchemy_kwargs_sql_auth_is_empty():
    """Con auth sql las credenciales ya van en el connection string."""
    assert SynapseClient(server='s', auth='sql', user='u', password='p').sqlalchemy_kwargs() == {}


def test_data_processed_mb_reads_daily_counter(fake_odbc, client):
    """data_processed_mb lee el contador diario de serverless."""
    fake_odbc.outcomes.append({'cols': ['data_processed_mb'], 'rows': [(123,)]})
    assert client.data_processed_mb() == 123.0
    sql, _ = fake_odbc.executed[0]
    assert 'sys.dm_external_data_processed' in sql and "'daily'" in sql


def test_data_processed_mb_without_row_is_zero(fake_odbc, client):
    """Sin consumo en el día el contador no tiene fila: 0."""
    fake_odbc.outcomes.append({'cols': ['data_processed_mb'], 'rows': []})
    assert client.data_processed_mb() == 0.0


def test_separar_tabla_dos_partes():
    """esquema.tabla se separa tal cual."""
    assert syn.separar_tabla('bronze_x.vista', 'bronze') == ('bronze_x', 'vista')


def test_separar_tabla_tres_partes_misma_base():
    """base.esquema.tabla se acepta si la base es la del cliente (sin distinguir mayúsculas)."""
    assert syn.separar_tabla('Bronze.s.v', 'bronze') == ('s', 'v')


def test_separar_tabla_base_distinta():
    """Otra base no se acepta: el datasource está atado a la base del cliente."""
    with pytest.raises(ValueError, match='no coincide'):
        syn.separar_tabla('silver.s.v', 'bronze')


def test_separar_tabla_una_parte():
    """Hace falta el esquema."""
    with pytest.raises(ValueError, match='esquema.tabla'):
        syn.separar_tabla('v', 'bronze')


def test_separar_tabla_caracteres():
    """Nombres solo con letras, números y guion bajo."""
    with pytest.raises(ValueError):
        syn.separar_tabla('s.v; DROP', 'bronze')


def test_tipo_y_definicion_objeto(fake_odbc, client):
    """tipo_objeto consulta sys.objects y definicion_objeto OBJECT_DEFINITION, con el nombre como parámetro."""
    fake_odbc.outcomes.append({'cols': ['type'], 'rows': [('V ',)]})
    fake_odbc.outcomes.append({'cols': ['d'], 'rows': [('CREATE VIEW s.v AS SELECT 1 FROM OPENROWSET(...)',)]})
    assert client.tipo_objeto('s.v') == 'V'
    assert 'OPENROWSET' in client.definicion_objeto('s.v')
    sqls = [(sql, params) for sql, params in fake_odbc.executed if isinstance(sql, str)]
    assert 'sys.objects' in sqls[0][0] and sqls[0][1] == ('s.v',)
    assert 'OBJECT_DEFINITION' in sqls[1][0] and sqls[1][1] == ('s.v',)


def test_tipo_objeto_inexistente(fake_odbc, client):
    """Sin fila, tipo_objeto devuelve None."""
    fake_odbc.outcomes.append({'cols': ['type'], 'rows': []})
    assert client.tipo_objeto('s.no') is None


def test_sql_pool_warming_up_is_retried(fake_odbc, client, monkeypatch):
    """42109 (pool serverless en frío) al conectar se reintenta como transitorio."""
    real_connect = fake_odbc.connect
    calls = []

    def frio(*a, **k):
        calls.append(1)
        if len(calls) == 1:
            raise FakeError(
                'HY000', '[HY000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]The SQL pool is warming up. '
                'Please try again. (42109) (SQLDriverConnect)'
            )
        return real_connect(*a, **k)

    monkeypatch.setattr(fake_odbc, 'connect', frio)
    fake_odbc.outcomes.append({'cols': ['x'], 'rows': [(1,)]})
    assert client.scalar('SELECT 1') == 1
    assert [e for e in fake_odbc.executed if e[0] == 'sleep'] == [('sleep', 2)]
