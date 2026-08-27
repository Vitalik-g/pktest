#!/usr/bin/env python3
"""
Cron-скрипт: тягне packetbeat flow-документи з Elasticsearch за вікно часу
з моменту останнього запуску, резолвить/створює мережеві вузли (netw_node +
netw_node_mac/ip/os) за правилами з ТЗ, агрегує взаємодії і записує їх у
TimescaleDB-таблицю netw_xact.

Розрахований на об'єми в мільйони документів/годину:
  - читання з ES стрімом (PIT + search_after), без завантаження всього в ES-клієнті одразу
  - агрегація в пам'яті по (src_node, dst_node, src_port, dst_port, protocol) -
    кардинальність цього словника на порядки менша за кількість сирих
    документів (не мільйони, а типово тисячі-десятки тисяч унікальних пар)
  - усі вставки в БД пакетні (execute_values / bulk INSERT ... RETURNING)
  - вузли хостів-агентів кешуються в пам'яті на весь прогін, тож повторне
    розпізнавання одного й того ж хоста коштує O(1), без походу в БД

Запуск (приклад crontab, кожні 5 хвилин):
    */5 * * * * /usr/bin/python3 /opt/netgraph/ingest/ingest.py >> /var/log/netgraph-ingest.log 2>&1
"""
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import ingest.config as config
import ingest.es_client as es_client
import ingest.pg_client as pg_client
import ingest.schema_bootstrap as schema_bootstrap

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("ingest")


def get_nested(doc: dict, path: str, default=None):
    cur = doc
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def resolve_host(doc: dict, cache: pg_client.NodeCache, conn) -> tuple[int | None, set]:
    """
    Резолвить/створює вузол для хоста-агента packetbeat (host.* в документі).
    Повертає (netw_node_id хоста або None, множина його поточних IP).
    """
    host_id = get_nested(doc, "host.id")
    if not host_id:
        return None, set()

    macs = {m.upper() for m in as_list(get_nested(doc, "host.mac")) if m}
    ips = {ip for ip in as_list(get_nested(doc, "host.ip")) if ip}
    hostname = get_nested(doc, "host.hostname")
    os_name = get_nested(doc, "host.os.name")
    os_version = get_nested(doc, "host.os.version")
    os_type = get_nested(doc, "host.os.type")

    existing = cache.agent_state.get(host_id)

    def _create_new_node():
        node_id = cache.create_nodes(1)[0]
        pg_client.bulk_insert_mac_rows(conn, [(node_id, m) for m in macs])
        pg_client.bulk_insert_ip_rows(conn, [(node_id, ip) for ip in ips])
        pg_client.insert_os_row(conn, node_id, os_name, os_version, os_type, hostname)
        pg_client.upsert_agent_state(
            conn, host_id, node_id, macs, ips, hostname, os_name, os_version, os_type
        )
        cache.agent_state[host_id] = dict(
            netw_node_id=node_id, mac_set=macs, ip_set=ips,
            hostname=hostname, os_name=os_name, os_version=os_version, os_type=os_type,
        )
        for ip in ips:
            cache.ip_to_node[ip] = node_id
        return node_id

    if existing is None:
        log.info("Новий вузол-агент host.id=%s (hostname=%s)", host_id, hostname)
        return _create_new_node(), ips

    if existing["mac_set"] != macs:
        new_macs = macs - existing["mac_set"]
        if new_macs:
            pg_client.bulk_insert_mac_rows(
                conn,
                [(existing["netw_node_id"], m) for m in new_macs]
            )

    if existing["ip_set"] != ips:
        new_ips = ips - existing["ip_set"]

        if new_ips:
            pg_client.bulk_insert_ip_rows(
                conn,
                [(existing["netw_node_id"], ip) for ip in new_ips]
            )

            for ip in new_ips:
                cache.ip_to_node[ip] = existing["netw_node_id"]

    existing["mac_set"] = existing["mac_set"] | macs
    existing["ip_set"] = existing["ip_set"] | ips

    node_id = existing["netw_node_id"]
    if (existing["hostname"], existing["os_name"], existing["os_version"], existing["os_type"]) \
            != (hostname, os_name, os_version, os_type):
        pg_client.insert_os_row(conn, node_id, os_name, os_version, os_type, hostname)
        pg_client.upsert_agent_state(
            conn, host_id, node_id, macs, ips, hostname, os_name, os_version, os_type
        )
        existing.update(hostname=hostname, os_name=os_name, os_version=os_version, os_type=os_type)

    return node_id, ips


NEWIP_PREFIX = "__NEWIP__"


def resolve_ip_repr(ip: str, cache: pg_client.NodeCache, new_ips: set) -> str | int:
    """
    Повертає або вже відомий netw_node_id (int), або тимчасовий маркер-рядок
    для IP, вузол якого буде створено масово наприкінці проходу.
    """
    node_id = cache.ip_to_node.get(ip)
    if node_id is not None:
        return node_id
    new_ips.add(ip)
    return NEWIP_PREFIX + ip


def _redact_dsn(dsn: str) -> str:
    return " ".join(p for p in dsn.split() if not p.lower().startswith("password="))


def run():
    conn = pg_client.connect()
    log.info("Підключено до PostgreSQL: %s", _redact_dsn(config.PG_DSN))
    try:
        schema_bootstrap.ensure_schema(conn)

        last_ingested = pg_client.get_last_ingested(conn)
        now = datetime.now(timezone.utc)
        end = now - timedelta(seconds=config.INGEST_SAFETY_LAG_SECONDS)

        if last_ingested is None:
            start = now - timedelta(minutes=config.INGEST_INITIAL_LOOKBACK_MINUTES)
            log.info("Курсор не знайдено, перший запуск: беремо останні %d хв",
                      config.INGEST_INITIAL_LOOKBACK_MINUTES)
        else:
            start = last_ingested - timedelta(seconds=config.INGEST_OVERLAP_SECONDS)

        if start >= end:
            log.info("Немає нового вікна для обробки (start=%s >= end=%s)", start, end)
            return

        log.info("Обробляю вікно [%s, %s)", start, end)

        cache = pg_client.NodeCache(conn)
        es = es_client.build_client()

        # агрегація: (src_repr, dst_repr, src_port, dst_port, protocol) -> {"count", "effective_at"}
        agg: dict[tuple, dict] = defaultdict(lambda: {"count": 0, "effective_at": None})
        new_ips: set[str] = set()

        docs_seen = 0
        max_ingested_seen = start

        for doc in es_client.stream_flow_events(es, start, end):
            docs_seen += 1

            host_node_id, host_ips = resolve_host(doc, cache, conn)

            src_ip = get_nested(doc, "source.ip")
            dst_ip = get_nested(doc, "destination.ip")
            src_port = get_nested(doc, "source.port")
            dst_port = get_nested(doc, "destination.port")
            protocol = (get_nested(doc, "network.transport") or "unknown").upper()

            if src_ip is None or dst_ip is None or src_port is None or dst_port is None:
                continue  # неповний документ (наприклад, non-flow подія) - пропускаємо

            if host_node_id is not None and src_ip in host_ips:
                src_repr = host_node_id
            else:
                src_repr = resolve_ip_repr(src_ip, cache, new_ips)

            if host_node_id is not None and dst_ip in host_ips:
                dst_repr = host_node_id
            else:
                dst_repr = resolve_ip_repr(dst_ip, cache, new_ips)

            effective_raw = (
                get_nested(doc, "event.end")
                or get_nested(doc, "event.ingested")
                or get_nested(doc, "@timestamp")
            )
            effective_at = _parse_es_ts(effective_raw)

            key = (src_repr, dst_repr, src_port, dst_port, protocol)
            bucket = agg[key]
            bucket["count"] += 1
            if bucket["effective_at"] is None or effective_at > bucket["effective_at"]:
                bucket["effective_at"] = effective_at

            ingested_ts = _parse_es_ts(get_nested(doc, "event.ingested"))
            if ingested_ts and ingested_ts > max_ingested_seen:
                max_ingested_seen = ingested_ts

        log.info("Оброблено документів: %d, унікальних пар взаємодії: %d, нових IP: %d",
                  docs_seen, len(agg), len(new_ips))

        # --- масово створюємо "легкі" вузли для нових IP-адрес ---------------
        if new_ips:
            new_ip_list = sorted(new_ips)
            ids = cache.create_nodes(len(new_ip_list))
            ip_to_new_id = dict(zip(new_ip_list, ids))
            pg_client.bulk_insert_ip_rows(
                conn, [(node_id, ip) for ip, node_id in ip_to_new_id.items()]
            )
            for ip, node_id in ip_to_new_id.items():
                cache.ip_to_node[ip] = node_id
            log.info("Створено %d нових вузлів за IP", len(new_ip_list))
        else:
            ip_to_new_id = {}

        # --- ремапимо тимчасові маркери в реальні node_id та зливаємо дублікати ---
        final_agg: dict[tuple, dict] = defaultdict(lambda: {"count": 0, "effective_at": None})

        def _resolve_repr(repr_):
            if isinstance(repr_, str) and repr_.startswith(NEWIP_PREFIX):
                ip = repr_[len(NEWIP_PREFIX):]
                return ip_to_new_id[ip]
            return repr_

        for (src_repr, dst_repr, sport, dport, proto), bucket in agg.items():
            key = (_resolve_repr(src_repr), _resolve_repr(dst_repr), sport, dport, proto)
            fb = final_agg[key]
            fb["count"] += bucket["count"]
            if fb["effective_at"] is None or bucket["effective_at"] > fb["effective_at"]:
                fb["effective_at"] = bucket["effective_at"]

        rows = [
            (bucket["effective_at"], bucket["count"], src, dst, sport, dport, proto)
            for (src, dst, sport, dport, proto), bucket in final_agg.items()
        ]
        if not rows:
            log.info("Немає жодного агрегованого рядка для запису в netw_xact за це вікно")
        pg_client.bulk_insert_xact(conn, rows)

        pg_client.set_last_ingested(conn, max_ingested_seen)
        conn.commit()
        log.info("Готово. Курсор пересунуто на %s", max_ingested_seen)

    except Exception:
        conn.rollback()
        log.exception("Помилка інжесту, транзакцію відкочено")
        raise
    finally:
        conn.close()


def _parse_es_ts(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, list):
        value = value[0]
    v = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(v)
    except ValueError:
        # мілісекунди/наносекунди можуть мати інший формат
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        raise


if __name__ == "__main__":
    try:
        run()
    except Exception:
        sys.exit(1)