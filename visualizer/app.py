#!/usr/bin/env python3
"""
Веб-візуалізатор графа мережевої взаємодії.
Читає вже готові дані з PostgreSQL (netw_node* + netw_xact) і віддає:
  - HTML сторінку з інтерактивним графом (vis-network)
  - JSON API /api/graph, який граф і споживає

Ніяких записів у БД цей додаток не робить - лише читання.
"""
import logging
import os
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras as pg_extras
from flask import Flask, jsonify, render_template, request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("visualizer")

PG_DSN = os.environ.get(
    "PG_DSN",
    "host=localhost port=40001 dbname=inframonitor user=inframonitor password=password",
)

app = Flask(__name__)


def get_conn():
    return psycopg2.connect(PG_DSN)


def _missing_table_response(exc: psycopg2.errors.UndefinedTable):
    """
    Найчастіша причина: візуалізатор і ingest.py дивляться на РІЗНІ бази/хости
    (різні PG_DSN), або ingest.py ще жодного разу не запускався для цієї БД.
    Повертаємо порожній граф + пояснення замість голої 500-ки.
    """
    log.warning("Таблиця відсутня в БД, на яку вказує поточний PG_DSN: %s", exc)
    return jsonify({
        "nodes": [],
        "edges": [],
        "error": (
            "У базі даних, до якої підключено візуалізатор, не знайдено потрібних "
            "таблиць. Перевірте, що змінна PG_DSN тут вказує на ТУ САМУ базу/хост, "
            "куди пише ingest.py (перевірте /api/health), і що ingest.py вже хоча б "
            "раз відпрацював успішно."
        ),
    }), 200


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/health")
def api_health():
    """Швидка діагностика: чи взагалі доступна БД і чи є в ній очікувані таблиці."""
    result = {"pg_dsn": _redact_dsn(PG_DSN), "connected": False, "tables": {}}
    expected_tables = ["netw_node", "netw_node_mac", "netw_node_ip",
                       "netw_node_os", "netw_xact", "ingest_state", "netw_agent_state"]
    try:
        with get_conn() as conn, conn.cursor() as cur:
            result["connected"] = True
            for t in expected_tables:
                cur.execute("SELECT to_regclass(%s)", (t,))
                result["tables"][t] = cur.fetchone()[0] is not None
            if result["tables"].get("netw_xact"):
                cur.execute("SELECT count(*) FROM netw_xact")
                result["netw_xact_rows"] = cur.fetchone()[0]
    except psycopg2.OperationalError as e:
        result["error"] = f"не вдалось підключитись до PostgreSQL: {e}"
    return jsonify(result)


def _redact_dsn(dsn: str) -> str:
    return " ".join(p for p in dsn.split() if not p.startswith("password="))


@app.route("/api/graph")
def api_graph():
    """
    Параметри:
      hours     - за скільки годин назад брати взаємодії (за замовчуванням 24)
      protocol  - фільтр по протоколу (TCP/UDP/...), необов'язково
      limit_edges - обмеження кількості ребер для дуже великих графів (за замовч. 2000)
    """
    hours = request.args.get("hours", default=24, type=int)
    protocol = request.args.get("protocol", default=None, type=str)
    limit_edges = request.args.get("limit_edges", default=2000, type=int)

    since = datetime.utcnow() - timedelta(hours=hours)

    where = ["effective_at >= %s"]
    params = [since]
    if protocol:
        where.append("protocol = %s")
        params.append(protocol.upper())
    where_sql = " AND ".join(where)

    try:
        with get_conn() as conn, conn.cursor(cursor_factory=pg_extras.RealDictCursor) as cur:
            # Агрегуємо по парі вузлів (без розбиття по портах) - для наочності
            # графа; деталі по портах/протоколах віддаємо окремо в edge_details.
            cur.execute(
                f"""
                SELECT src_node_id, dst_node_id,
                       SUM(count) AS total_count,
                       MAX(effective_at) AS last_seen,
                       array_agg(DISTINCT protocol) AS protocols,
                       array_agg(DISTINCT dst_port) AS dst_ports
                FROM netw_xact
                WHERE {where_sql}
                GROUP BY src_node_id, dst_node_id
                ORDER BY total_count DESC
                LIMIT %s
                """,
                params + [limit_edges],
            )
            edges_raw = cur.fetchall()

            node_ids = set()
            for e in edges_raw:
                node_ids.add(e["src_node_id"])
                node_ids.add(e["dst_node_id"])

            nodes = []
            if node_ids:
                cur.execute(
                    """
                    SELECT n.netw_node_id,
                           COALESCE(
                               (SELECT hostname FROM netw_node_os o
                                WHERE o.netw_node_id = n.netw_node_id
                                ORDER BY o.created_at DESC LIMIT 1),
                               NULL
                           ) AS hostname,
                           (SELECT os_name FROM netw_node_os o
                            WHERE o.netw_node_id = n.netw_node_id
                            ORDER BY o.created_at DESC LIMIT 1) AS os_name,
                           (SELECT os_version FROM netw_node_os o
                            WHERE o.netw_node_id = n.netw_node_id
                            ORDER BY o.created_at DESC LIMIT 1) AS os_version,
                           array_remove(array_agg(DISTINCT ip.ip_address::text), NULL) AS ips,
                           array_remove(array_agg(DISTINCT mac.mac_address::text), NULL) AS macs
                    FROM netw_node n
                    LEFT JOIN netw_node_ip ip ON ip.netw_node_id = n.netw_node_id
                    LEFT JOIN netw_node_mac mac ON mac.netw_node_id = n.netw_node_id
                    WHERE n.netw_node_id = ANY(%s)
                    GROUP BY n.netw_node_id
                    """,
                    (list(node_ids),),
                )
                for row in cur.fetchall():
                    label = row["hostname"] or (row["ips"][0] if row["ips"] else f"node#{row['netw_node_id']}")
                    nodes.append({
                        "id": row["netw_node_id"],
                        "label": label,
                        "hostname": row["hostname"],
                        "os": f"{row['os_name'] or ''} {row['os_version'] or ''}".strip() or None,
                        "ips": row["ips"],
                        "macs": row["macs"],
                        "is_agent": bool(row["macs"]),  # вузли без MAC - це "легкі" IP-only вузли
                    })

            edges = [
                {
                    "from": e["src_node_id"],
                    "to": e["dst_node_id"],
                    "count": e["total_count"],
                    "last_seen": e["last_seen"].isoformat() if e["last_seen"] else None,
                    "protocols": e["protocols"],
                    "dst_ports": e["dst_ports"][:20],  # не роздувати payload
                }
                for e in edges_raw
            ]

        return jsonify({"nodes": nodes, "edges": edges})

    except psycopg2.errors.UndefinedTable as e:
        return _missing_table_response(e)
    except psycopg2.OperationalError as e:
        log.error("Не вдалось підключитись до PostgreSQL: %s", e)
        return jsonify({
            "nodes": [], "edges": [],
            "error": f"Не вдалось підключитись до бази даних (перевірте PG_DSN): {e}",
        }), 200


@app.route("/api/node/<int:node_id>")
def api_node_detail(node_id: int):
    """Детальна історія вузла: всі IP/MAC коли-небудь пов'язані, історія OS/hostname."""
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=pg_extras.RealDictCursor) as cur:
            cur.execute("SELECT ip_address::text FROM netw_node_ip WHERE netw_node_id = %s", (node_id,))
            ips = [r["ip_address"] for r in cur.fetchall()]

            cur.execute("SELECT mac_address::text FROM netw_node_mac WHERE netw_node_id = %s", (node_id,))
            macs = [r["mac_address"] for r in cur.fetchall()]

            cur.execute(
                """
                SELECT created_at, os_name, os_version, os_type, hostname
                FROM netw_node_os WHERE netw_node_id = %s
                ORDER BY created_at DESC
                """,
                (node_id,),
            )
            os_history = [
                {
                    "created_at": r["created_at"].isoformat(),
                    "os_name": r["os_name"],
                    "os_version": r["os_version"],
                    "os_type": r["os_type"],
                    "hostname": r["hostname"],
                }
                for r in cur.fetchall()
            ]

        return jsonify({"node_id": node_id, "ips": ips, "macs": macs, "os_history": os_history})

    except psycopg2.errors.UndefinedTable as e:
        return _missing_table_response(e)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)