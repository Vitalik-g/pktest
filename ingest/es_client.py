"""
Обгортка над Elasticsearch для стрімінгового читання packetbeat flow-подій
у заданому часовому вікні. Використовує Point-In-Time + search_after замість
scroll — це рекомендований Elastic спосіб для "глибокої" пагінації великих
обсягів (за годину може прилетіти ~1 млн документів і більше), бо не тримає
scroll-контекст живим і краще паралелиться по шардах.
"""
import logging
from datetime import datetime, timezone
from typing import Iterator

from elasticsearch import Elasticsearch

import ingest.config as config

log = logging.getLogger("ingest.es")

# Поля, які нам реально потрібні з кожного flow-документа.
# Обмежуємо _source, щоб не тягнути з ES зайве (host.mac має до 8+ значень,
# але сам документ важить небагато) — на мільйонах документів це відчутно
# зменшує обсяг трафіку ES<->скрипт.
_SOURCE_FIELDS = [
    "@timestamp",
    "event.start",
    "event.end",
    "event.ingested",
    "event.dataset",
    "event.action",
    "source.ip",
    "source.port",
    "destination.ip",
    "destination.port",
    "network.transport",
    "network.type",
    "host.id",
    "host.hostname",
    "host.mac",
    "host.ip",
    "host.os.name",
    "host.os.version",
    "host.os.type",
]


def build_client() -> Elasticsearch:
    kwargs = dict(
        hosts=config.ES_HOSTS,
        verify_certs=config.ES_VERIFY_CERTS,
        request_timeout=60,
    )
    if config.ES_CA_CERTS:
        kwargs["ca_certs"] = config.ES_CA_CERTS
    if config.ES_API_KEY:
        kwargs["api_key"] = config.ES_API_KEY
    elif config.ES_USERNAME:
        kwargs["basic_auth"] = (config.ES_USERNAME, config.ES_PASSWORD)
    return Elasticsearch(**kwargs)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def stream_flow_events(
    es: Elasticsearch,
    start: datetime,
    end: datetime,
    page_size: int = None,
) -> Iterator[dict]:
    """
    Генератор, що віддає документи packetbeat flow, у яких
    event.ingested належить [start, end), сторінками через PIT+search_after.

    Кожен yield — це вже "плоский" dict з потрібними полями
    (значення взяті з _source, без списків-обгорток).
    """
    page_size = page_size or config.ES_PAGE_SIZE

    pit = es.open_point_in_time(
        index=config.ES_INDEX_PATTERN, keep_alive="5m"
    )
    pit_id = pit["id"]

    query = {
        "bool": {
            "filter": [
                {"term": {"event.dataset": "flow"}},
                {
                    "range": {
                        "event.ingested": {
                            "gte": _iso(start),
                            "lt": _iso(end),
                        }
                    }
                },
            ]
        }
    }
    sort = [{"event.ingested": "asc"}, {"_shard_doc": "asc"}]

    search_after = None
    total = 0
    try:
        while True:
            body = dict(
                size=page_size,
                query=query,
                sort=sort,
                source=_SOURCE_FIELDS,
                pit={"id": pit_id, "keep_alive": "5m"},
                track_total_hits=False,
            )
            if search_after is not None:
                body["search_after"] = search_after

            resp = es.search(**body)
            hits = resp["hits"]["hits"]
            if not hits:
                break

            # PIT id може оновлюватись між запитами
            pit_id = resp.get("pit_id", pit_id)

            for hit in hits:
                total += 1
                yield hit["_source"]

            search_after = hits[-1]["sort"]

            if len(hits) < page_size:
                break

        log.info("Прочитано з Elasticsearch %d flow-документів [%s, %s)", total, start, end)
    finally:
        try:
            es.close_point_in_time(id=pit_id)
        except Exception:
            log.warning("Не вдалось закрити PIT (не критично)", exc_info=True)
