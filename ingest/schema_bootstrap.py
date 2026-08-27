"""
Ідемпотентне створення схеми БД. Викликається на старті кожного запуску
ingest.py: якщо таблиць ще нема - створює їх, якщо вже є - нічого не робить
(і не падає). Це дозволяє не покладатись на ручний `psql -f schema.sql`
перед першим запуском.

Важливо: кожен логічний крок комітиться ОКРЕМО. Це навмисно - якщо
конкретна TimescaleDB-специфічна дія (перетворення на гіпертаблицю,
політики компресії/retention) не спрацює через відмінності версії
розширення, базові таблиці (netw_node, netw_node_ip, ingest_state тощо)
все одно залишаться створеними, а не відкотяться разом з невдалим кроком.

netw_xact створюється класичним, максимально сумісним способом:
  1. звичайний CREATE TABLE
  2. SELECT create_hypertable(...)
  3. ALTER TABLE ... SET (timescaledb.compress, ...)
  4. add_compression_policy(...) / add_retention_policy(...)
замість декларативного "WITH (timescaledb.hypertable, ...)", який працює
лише в TimescaleDB 2.13+ і на старіших версіях просто впаде з помилкою
синтаксису, відкотивши все, що було в тій самій транзакції.
"""
import logging

import psycopg2

log = logging.getLogger("ingest.schema")


_PLAIN_TABLES_SQL = """
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS netw_node (
    netw_node_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS netw_node_mac (
    netw_node_id INT REFERENCES netw_node(netw_node_id) ON DELETE CASCADE,
    mac_address MACADDR NOT NULL,
    PRIMARY KEY (netw_node_id, mac_address)
);
CREATE INDEX IF NOT EXISTS idx_netw_node_mac_addr ON netw_node_mac (mac_address);

CREATE TABLE IF NOT EXISTS netw_node_ip (
    netw_node_id INT REFERENCES netw_node(netw_node_id) ON DELETE CASCADE,
    ip_address INET NOT NULL,
    PRIMARY KEY (netw_node_id, ip_address)
);
CREATE INDEX IF NOT EXISTS idx_netw_node_ip_addr ON netw_node_ip (ip_address);

CREATE TABLE IF NOT EXISTS netw_node_os (
    netw_node_id INT REFERENCES netw_node(netw_node_id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    os_name VARCHAR(255),
    os_version VARCHAR(255),
    os_type VARCHAR(255),
    hostname VARCHAR(255) NOT NULL,
    PRIMARY KEY (netw_node_id, created_at)
);
CREATE INDEX IF NOT EXISTS idx_netw_node_os_node ON netw_node_os (netw_node_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ingest_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS netw_agent_state (
    agent_host_id TEXT PRIMARY KEY,
    netw_node_id INT NOT NULL REFERENCES netw_node(netw_node_id) ON DELETE CASCADE,
    mac_set TEXT NOT NULL,
    ip_set TEXT NOT NULL,
    hostname VARCHAR(255),
    os_name VARCHAR(255),
    os_version VARCHAR(255),
    os_type VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_NETW_XACT_PLAIN_TABLE_SQL = """
CREATE TABLE netw_xact (
    effective_at TIMESTAMP NOT NULL,
    count INT NOT NULL,
    src_node_id INT REFERENCES netw_node(netw_node_id) ON DELETE CASCADE,
    dst_node_id INT REFERENCES netw_node(netw_node_id) ON DELETE CASCADE,
    src_port INT NOT NULL,
    dst_port INT NOT NULL,
    protocol VARCHAR(10) NOT NULL
);
"""

_NETW_XACT_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_netw_xact_ident ON netw_xact (
    dst_node_id, dst_port, src_node_id, src_port, protocol
);
CREATE INDEX IF NOT EXISTS idx_netw_xact_time ON netw_xact (effective_at DESC);
"""


def _try(cur, sql: str, label: str) -> bool:
    """Виконує один statement під SAVEPOINT. Повертає True/False, не кидає
    виняток - для дій, які нормально можуть "вже бути застосовані" (політики,
    вже існуючі індекси тощо). Не використовувати для кроків, чия невдача
    має зупинити процес (там нехай виняток летить далі)."""
    cur.execute("SAVEPOINT sp_schema")
    try:
        cur.execute(sql)
        cur.execute("RELEASE SAVEPOINT sp_schema")
        return True
    except psycopg2.Error as e:
        cur.execute("ROLLBACK TO SAVEPOINT sp_schema")
        log.debug("Пропускаю (%s): %s", label, str(e).strip().splitlines()[0])
        return False


def ensure_schema(conn) -> None:
    """Гарантує наявність усіх потрібних таблиць/індексів/політик. Безпечно
    викликати на кожному запуску. Кожен крок комітиться окремо."""

    # --- крок 1: базові (не-timescale) таблиці --------------------------
    with conn.cursor() as cur:
        cur.execute(_PLAIN_TABLES_SQL)
    conn.commit()
    log.info("Базові таблиці (netw_node*, ingest_state, netw_agent_state) готові")

    # --- крок 2: netw_xact як гіпертаблиця -------------------------------
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.netw_xact')")
        xact_exists = cur.fetchone()[0] is not None

    if not xact_exists:
        log.info("netw_xact відсутня - створюю таблицю і перетворюю на гіпертаблицю TimescaleDB")
        try:
            with conn.cursor() as cur:
                cur.execute(_NETW_XACT_PLAIN_TABLE_SQL)
                cur.execute(
                    "SELECT create_hypertable('netw_xact', 'effective_at', "
                    "chunk_time_interval => INTERVAL '7 days')"
                )
            conn.commit()
            log.info("netw_xact створено і перетворено на гіпертаблицю")
        except psycopg2.Error:
            conn.rollback()
            log.exception(
                "Не вдалось створити netw_xact як гіпертаблицю TimescaleDB. "
                "Перевірте версію розширення (`SELECT extversion FROM pg_extension "
                "WHERE extname='timescaledb';`) і права користувача БД."
            )
            raise

        # компресія - вже не критично, якщо не вийде: просто без компресії
        with conn.cursor() as cur:
            ok = _try(
                cur,
                "ALTER TABLE netw_xact SET ("
                " timescaledb.compress,"
                " timescaledb.compress_segmentby = 'src_node_id',"
                " timescaledb.compress_orderby = 'effective_at DESC'"
                ")",
                "enable_compression",
            )
        conn.commit()
        if not ok:
            log.warning("Компресію для netw_xact увімкнути не вдалось - таблиця працюватиме без неї")

    # --- крок 3: політики компресії/retention (незалежно від того, чи щойно створили таблицю) ---
    with conn.cursor() as cur:
        # назва функції для політики компресії відрізняється між версіями TimescaleDB
        applied = _try(cur, "SELECT add_compression_policy('netw_xact', INTERVAL '7 days')", "add_compression_policy")
        if not applied:
            _try(cur, "SELECT create_compression_policy('netw_xact', INTERVAL '7 days')", "create_compression_policy")
        _try(cur, "SELECT add_retention_policy('netw_xact', INTERVAL '91 days')", "add_retention_policy")
    conn.commit()

    # --- крок 4: індекси ---------------------------------------------------
    with conn.cursor() as cur:
        cur.execute(_NETW_XACT_INDEXES_SQL)
    conn.commit()

    log.info("Схема БД перевірена/створена успішно")