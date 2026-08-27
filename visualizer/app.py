#!/usr/bin/env python3
"""
Веб-візуалізатор графа мережевої взаємодії.
Читає вже готові дані з PostgreSQL (netw_node* + netw_xact) і віддає:
  - HTML сторінку з інтерактивним графом (vis-network)
  - JSON API /api/graph, який граф і споживає

Ніяких записів у БД цей додаток не робить - лише читання.

--- Об'єднання вузлів (дедуплікація) -----------------------------------
Через особливості ingest.py одна й та сама фізична машина інколи отримує
кілька різних netw_node_id (наприклад: "легкий" вузол, створений лише за
IP до того, як packetbeat прислав host.*-поля, і окремий вузол-агент,
створений пізніше для того ж самого host.id).

ВАЖЛИВО: об'єднувати такі вузли за спільною IP-адресою НЕ можна.
В мережі з DHCP/динамічною адресацією одна й та сама IP з часом легально
переходить до РІЗНИХ фізичних пристроїв - це не ознака дублікату. А що
ще гірше, netw_agent_state.ip_set в ingest.py накопичує ВСІ IP-адреси,
які хост коли-небудь мав, без обмеження в часі (див. resolve_host у
ingest.py: existing["ip_set"] | ips) - тобто один хост, що просто отримав
нову адресу по DHCP, з часом "успадковує" в своєму наборі чужі колишні
адреси. Об'єднання за IP транзитивно зліплювало б через такі вузли
зовсім не пов'язані фізичні машини в одну величезну групу.

Натомість тут вузли об'єднуються за спільною MAC-адресою (netw_node_mac).
MAC прив'язана до фізичного мережевого інтерфейсу і не "переїжджає" між
пристроями через DHCP, тому це набагато надійніший сигнал "це один і той
самий пристрій". Плата за надійність: "легкі" IP-only вузли (які взагалі
не мають жодної MAC, бо створені лише з мережевого трафіку, без даних
агента) ніколи ні з чим не об'єднуються - це свідомий компроміс на
користь того, щоб краще НЕ об'єднати два різних пристрої, ніж помилково
злити їх в один.

Усі ребра при об'єднанні НЕ губляться: вони переадресовуються на
представника об'єднаної групи і підсумовуються, включно з ребрами "сам
на себе" (якщо взаємодія колись була записана між двома вузлами, що
потім об'єдналися за MAC) - такі ребра показуються як самопетлі, а не
відкидаються.
"""
import logging
import os
from collections import defaultdict
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


# ---------------------------------------------------------------------------
# Union-Find для об'єднання netw_node_id, що ділять спільну IP-адресу
# ---------------------------------------------------------------------------

class _UnionFind:
    def __init__(self, ids):
        self.parent = {i: i for i in ids}

    def find(self, x):
        self.parent.setdefault(x, x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # менший id лишаємо "стабільним" представником групи
            if ra < rb:
                self.parent[rb] = ra
            else:
                self.parent[ra] = rb


@app.route("/api/graph")
def api_graph():
    """
    Параметри:
      hours     - за скільки годин назад брати взаємодії (за замовчуванням 24)
      protocol  - фільтр по протоколу (TCP/UDP/...), необов'язково
      limit_edges - обмеження кількості (вже об'єднаних) ребер у відповіді
                    (за замовч. 2000)
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

    # Скільки "сирих" (до об'єднання дублікатів-вузлів) пар вузлів тягнути
    # з БД. Об'єднання може злити кілька сирих пар в одну, тому беремо із
    # запасом відносно limit_edges, але з жорсткою верхньою межею, щоб не
    # витягнути невиправдано багато даних одним запитом.
    raw_pair_cap = max(limit_edges * 5, 5000)
    raw_pair_cap = min(raw_pair_cap, 50000)

    try:
        with get_conn() as conn, conn.cursor(cursor_factory=pg_extras.RealDictCursor) as cur:
            # Агрегуємо по парі "сирих" вузлів (без розбиття по портах) -
            # об'єднання дублікатів-вузлів і фінальне групування ще попереду.
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
                params + [raw_pair_cap],
            )
            edges_raw = cur.fetchall()

            raw_node_ids = set()
            for e in edges_raw:
                raw_node_ids.add(e["src_node_id"])
                raw_node_ids.add(e["dst_node_id"])

            # --- об'єднуємо вузли, що ділять ту саму MAC-адресу ---
            # (НЕ за IP - див. пояснення у docstring на початку файлу: IP
            # легально переходить між пристроями через DHCP і накопичується
            # в ip_set хоста без обмеження в часі, тому об'єднання за IP
            # транзитивно зліплює непов'язані фізичні машини).
            uf = _UnionFind(raw_node_ids)
            if raw_node_ids:
                cur.execute(
                    "SELECT netw_node_id, mac_address::text AS mac "
                    "FROM netw_node_mac WHERE netw_node_id = ANY(%s)",
                    (list(raw_node_ids),),
                )
                mac_owner: dict[str, int] = {}
                for row in cur.fetchall():
                    mac, nid = row["mac"], row["netw_node_id"]
                    if mac in mac_owner:
                        uf.union(mac_owner[mac], nid)
                    else:
                        mac_owner[mac] = nid

            # --- перегруповуємо ребра за представниками об'єднаних вузлів ---
            merged_edges: dict[tuple, dict] = {}
            for e in edges_raw:
                key = (uf.find(e["src_node_id"]), uf.find(e["dst_node_id"]))
                bucket = merged_edges.setdefault(key, {
                    "count": 0, "last_seen": None,
                    "protocols": set(), "dst_ports": set(),
                })
                bucket["count"] += e["total_count"]
                if bucket["last_seen"] is None or (
                    e["last_seen"] and e["last_seen"] > bucket["last_seen"]
                ):
                    bucket["last_seen"] = e["last_seen"]
                bucket["protocols"].update(e["protocols"] or [])
                bucket["dst_ports"].update(e["dst_ports"] or [])

            top_edges = sorted(
                merged_edges.items(), key=lambda kv: kv[1]["count"], reverse=True
            )[:limit_edges]

            node_ids = set()
            for (src, dst), _ in top_edges:
                node_ids.add(src)
                node_ids.add(dst)

            # --- зводимо інформацію про вузли по групах об'єднання ---
            nodes = []
            if node_ids:
                group_members: dict[int, list[int]] = defaultdict(list)
                for nid in raw_node_ids:
                    rep = uf.find(nid)
                    if rep in node_ids:
                        group_members[rep].append(nid)
                for rep in node_ids:
                    if rep not in group_members:
                        group_members[rep].append(rep)

                all_member_ids = [m for members in group_members.values() for m in members]

                cur.execute(
                    """
                    SELECT n.netw_node_id,
                           (SELECT hostname FROM netw_node_os o
                            WHERE o.netw_node_id = n.netw_node_id
                            ORDER BY o.created_at DESC LIMIT 1) AS hostname,
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
                    (all_member_ids,),
                )
                by_id = {row["netw_node_id"]: row for row in cur.fetchall()}

                for rep, members in group_members.items():
                    hostname = os_name = os_version = None
                    ips: set[str] = set()
                    macs: set[str] = set()
                    for m in members:
                        row = by_id.get(m)
                        if not row:
                            continue
                        ips.update(row["ips"] or [])
                        macs.update(row["macs"] or [])
                        if row["hostname"] and not hostname:
                            hostname, os_name, os_version = (
                                row["hostname"], row["os_name"], row["os_version"],
                            )
                    ips_sorted = sorted(ips)
                    label = hostname or (ips_sorted[0] if ips_sorted else f"node#{rep}")
                    nodes.append({
                        "id": rep,
                        "label": label,
                        "hostname": hostname,
                        "os": f"{os_name or ''} {os_version or ''}".strip() or None,
                        "ips": ips_sorted,
                        "macs": sorted(macs),
                        "is_agent": bool(macs),  # вузли без MAC - "легкі" IP-only вузли
                        "merged_from": sorted(members),
                        "merged_count": len(members),
                    })

            edges = [
                {
                    "from": src,
                    "to": dst,
                    "count": bucket["count"],
                    "last_seen": bucket["last_seen"].isoformat() if bucket["last_seen"] else None,
                    "protocols": sorted(bucket["protocols"]),
                    "dst_ports": sorted(bucket["dst_ports"])[:20],  # не роздувати payload
                    "self_loop": src == dst,
                }
                for (src, dst), bucket in top_edges
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
    """
    Детальна історія вузла - з урахуванням об'єднання за спільними MAC:
    повертає IP/MAC та історію OS/hostname для node_id і будь-яких інших
    netw_node_id, що коли-небудь (транзитивно) ділили з ним ту саму
    MAC-адресу, тобто ту саму об'єднану групу, що й на графі /api/graph.
    Об'єднання навмисно НЕ робиться за IP - див. пояснення у docstring
    на початку файлу.
    """
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=pg_extras.RealDictCursor) as cur:
            # BFS по спільних MAC-адресах, щоб зібрати повну групу об'єднання
            member_ids = {node_id}
            frontier = {node_id}
            while frontier:
                cur.execute(
                    "SELECT DISTINCT mac_address::text AS mac FROM netw_node_mac "
                    "WHERE netw_node_id = ANY(%s)",
                    (list(frontier),),
                )
                macs_seen = [r["mac"] for r in cur.fetchall()]
                new_ids = set()
                if macs_seen:
                    cur.execute(
                        "SELECT DISTINCT netw_node_id FROM netw_node_mac "
                        "WHERE mac_address = ANY(%s)",
                        (macs_seen,),
                    )
                    for r in cur.fetchall():
                        if r["netw_node_id"] not in member_ids:
                            new_ids.add(r["netw_node_id"])
                member_ids |= new_ids
                frontier = new_ids

            member_ids = sorted(member_ids)

            cur.execute(
                "SELECT DISTINCT ip_address::text FROM netw_node_ip WHERE netw_node_id = ANY(%s)",
                (member_ids,),
            )
            ips = [r["ip_address"] for r in cur.fetchall()]

            cur.execute(
                "SELECT DISTINCT mac_address::text FROM netw_node_mac WHERE netw_node_id = ANY(%s)",
                (member_ids,),
            )
            macs = [r["mac_address"] for r in cur.fetchall()]

            cur.execute(
                """
                SELECT netw_node_id, created_at, os_name, os_version, os_type, hostname
                FROM netw_node_os WHERE netw_node_id = ANY(%s)
                ORDER BY created_at DESC
                """,
                (member_ids,),
            )
            os_history = [
                {
                    "netw_node_id": r["netw_node_id"],
                    "created_at": r["created_at"].isoformat(),
                    "os_name": r["os_name"],
                    "os_version": r["os_version"],
                    "os_type": r["os_type"],
                    "hostname": r["hostname"],
                }
                for r in cur.fetchall()
            ]

        return jsonify({
            "node_id": node_id,
            "merged_node_ids": member_ids,
            "ips": ips,
            "macs": macs,
            "os_history": os_history,
        })

    except psycopg2.errors.UndefinedTable as e:
        return _missing_table_response(e)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)