"""
Робота з PostgreSQL: підтримує in-memory кеші відповідностей
IP -> netw_node_id та host.id -> сигнатура агента, щоб не ходити в БД
на кожен документ (при мільйоні документів/год це критично для швидкості).

Кеші будуються один раз на старті прогону скрипта і живуть лише в межах
одного запуску cron — це нормально, бо кожен запуск все одно підвантажує
їх заново з БД (джерело правди — таблиці, кеш лише пришвидшує процес).
"""
import logging
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras as pg_extras

import ingest.config as config

log = logging.getLogger("ingest.pg")


def connect():
    conn = psycopg2.connect(config.PG_DSN)
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# Курсор інкрементального читання (ingest_state)
# ---------------------------------------------------------------------------

def get_last_ingested(conn) -> Optional[datetime]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT value FROM ingest_state WHERE key = %s",
            (config.STATE_KEY_LAST_INGESTED,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return datetime.fromisoformat(row[0])


def set_last_ingested(conn, ts: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_state (key, value, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
            """,
            (config.STATE_KEY_LAST_INGESTED, ts.isoformat()),
        )


# ---------------------------------------------------------------------------
# Кеші вузлів
# ---------------------------------------------------------------------------

class NodeCache:
    """
    Тримає в пам'яті:
      - ip_to_node: IP -> найновіший (max netw_node_id) вузол з таким IP
      - agent_state: host.id -> (netw_node_id, mac_set:set[str], ip_set:set[str],
                                   hostname, os_name, os_version, os_type)
    """

    def __init__(self, conn):
        self.conn = conn
        self.ip_to_node: dict[str, int] = {}
        self.agent_state: dict[str, dict] = {}
        self._load()

    def _load(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT ip_address::text, netw_node_id
                FROM netw_node_ip
                ORDER BY netw_node_id ASC
                """
            )
            for ip, node_id in cur.fetchall():
                # ORDER BY ASC + перезапис -> в кеші лишається найновіший (max id)
                self.ip_to_node[ip] = node_id

            cur.execute(
                """
                SELECT agent_host_id, netw_node_id, mac_set, ip_set,
                       hostname, os_name, os_version, os_type
                FROM netw_agent_state
                """
            )
            for row in cur.fetchall():
                self.agent_state[row[0]] = dict(
                    netw_node_id=row[1],
                    mac_set=set(row[2].split(",")) if row[2] else set(),
                    ip_set=set(row[3].split(",")) if row[3] else set(),
                    hostname=row[4],
                    os_name=row[5],
                    os_version=row[6],
                    os_type=row[7],
                )
        log.info(
            "Кеш завантажено: %d IP-відповідностей, %d відомих агентів",
            len(self.ip_to_node), len(self.agent_state),
        )

    # -- створення нових вузлів --------------------------------------------

    def create_nodes(self, count: int) -> list[int]:
        """Створює `count` нових netw_node одним запитом, повертає їх id по порядку."""
        if count <= 0:
            return []
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO netw_node (created_at)
                SELECT now() FROM generate_series(1, %s)
                RETURNING netw_node_id
                """,
                (count,),
            )
            ids = [r[0] for r in cur.fetchall()]
        return ids

    def get_or_create_ip_node(self, ip: str, pending_new_ips: list, pending_ip_rows: list) -> Optional[int]:
        """
        Повертає node_id для "легкого" вузла (лише IP, без MAC).
        Якщо вузла ще нема — резервує його у списках pending_* для
        подальшого масового створення (див. flush_new_ip_nodes).
        Повертає None, якщо вузол ще не створено (буде відомий після flush).
        """
        if ip in self.ip_to_node:
            return self.ip_to_node[ip]
        pending_new_ips.append(ip)
        return None


# ---------------------------------------------------------------------------
# Масові INSERT-и
# ---------------------------------------------------------------------------

def bulk_insert_ip_rows(conn, rows: list[tuple[int, str]]):
    """rows: [(netw_node_id, ip_address), ...]"""
    if not rows:
        return
    with conn.cursor() as cur:
        pg_extras.execute_values(
            cur,
            "INSERT INTO netw_node_ip (netw_node_id, ip_address) VALUES %s "
            "ON CONFLICT DO NOTHING",
            rows,
        )


def bulk_insert_mac_rows(conn, rows: list[tuple[int, str]]):
    """rows: [(netw_node_id, mac_address), ...]"""
    if not rows:
        return
    with conn.cursor() as cur:
        pg_extras.execute_values(
            cur,
            "INSERT INTO netw_node_mac (netw_node_id, mac_address) VALUES %s "
            "ON CONFLICT DO NOTHING",
            rows,
        )


def insert_os_row(conn, node_id: int, os_name, os_version, os_type, hostname):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO netw_node_os (netw_node_id, os_name, os_version, os_type, hostname)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (node_id, os_name, os_version, os_type, hostname or "unknown"),
        )


def upsert_agent_state(conn, agent_host_id, node_id, mac_set, ip_set,
                        hostname, os_name, os_version, os_type):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO netw_agent_state
                (agent_host_id, netw_node_id, mac_set, ip_set,
                 hostname, os_name, os_version, os_type, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (agent_host_id) DO UPDATE SET
                netw_node_id = EXCLUDED.netw_node_id,
                mac_set = EXCLUDED.mac_set,
                ip_set = EXCLUDED.ip_set,
                hostname = EXCLUDED.hostname,
                os_name = EXCLUDED.os_name,
                os_version = EXCLUDED.os_version,
                os_type = EXCLUDED.os_type,
                updated_at = now()
            """,
            (
                agent_host_id, node_id,
                ",".join(sorted(mac_set)), ",".join(sorted(ip_set)),
                hostname, os_name, os_version, os_type,
            ),
        )


def bulk_insert_xact(conn, rows: list[tuple]):
    """
    rows: [(effective_at, count, src_node_id, dst_node_id,
             src_port, dst_port, protocol), ...]
    """
    if not rows:
        return
    with conn.cursor() as cur:
        pg_extras.execute_values(
            cur,
            """
            INSERT INTO netw_xact
                (effective_at, count, src_node_id, dst_node_id, src_port, dst_port, protocol)
            VALUES %s
            """,
            rows,
        )
    log.info("Записано %d агрегованих рядків у netw_xact", len(rows))
