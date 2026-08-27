"""
Конфігурація ingest-скрипта. Усі значення беруться зі змінних середовища
(зручно для cron / systemd timer / docker), з розумними значеннями за
замовчуванням для локальної розробки.
"""
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


# --- Elasticsearch -----------------------------------------------------
ES_HOSTS = os.environ.get("ES_HOSTS", "https://localhost:9200").split(",")
ES_API_KEY = os.environ.get("ES_API_KEY")            # переважно, якщо є
ES_USERNAME = os.environ.get("ES_USERNAME")           # або basic auth
ES_PASSWORD = os.environ.get("ES_PASSWORD")
ES_VERIFY_CERTS = os.environ.get("ES_VERIFY_CERTS", "true").lower() == "true"
ES_CA_CERTS = os.environ.get("ES_CA_CERTS")           # шлях до CA, якщо треба

# індекс/патерн, куди пише packetbeat flow-дані
ES_INDEX_PATTERN = os.environ.get("ES_INDEX_PATTERN", "packetbeat-*")

# --- PostgreSQL ----------------------------------------------------------
PG_DSN = os.environ.get(
    "PG_DSN",
    "host=localhost port=5432 dbname=netgraph user=netgraph password=netgraph",
)

# --- Параметри інжесту ----------------------------------------------------
# скільки документів забирати за одну сторінку з ES (PIT + search_after)
ES_PAGE_SIZE = _env_int("ES_PAGE_SIZE", 8000)

# наскільки "відстає" верхня межа вікна від поточного часу — щоб не
# захопити ще не до кінця проіндексовані документи (packetbeat flow має
# затримку flush ~10-30с)
INGEST_SAFETY_LAG_SECONDS = _env_int("INGEST_SAFETY_LAG_SECONDS", 30)

# скільки "перекриття" брати з попереднього вікна назад, про всяк випадок
# (документи, що встигли доіндексуватись заднім числом) — дублі нам не
# страшні, це лише злегка завищить count, тому overlap робимо невеликим
INGEST_OVERLAP_SECONDS = _env_int("INGEST_OVERLAP_SECONDS", 60)

# якщо курсора в БД ще нема (перший запуск) — з якого моменту почати
INGEST_INITIAL_LOOKBACK_MINUTES = _env_int("INGEST_INITIAL_LOOKBACK_MINUTES", 15)

# ключ курсора в ingest_state
STATE_KEY_LAST_INGESTED = "packetbeat_flow_last_event_ingested"

# логування
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
