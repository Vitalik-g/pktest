-- ============================================================================
-- netgraph schema
-- Базова частина (netw_node / netw_node_mac / netw_node_ip / netw_node_os /
-- netw_xact) — як у вихідному завданні, без змін по суті.
-- Додано лише: індекси для швидкого зворотнього пошуку по IP/MAC,
-- та дві службові таблиці, потрібні cron-скрипту (ingest_state, netw_agent_state).
-- ============================================================================

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
-- зворотній пошук вузла за MAC (яка "поточна" запис-версія вузла для цієї MAC)
CREATE INDEX IF NOT EXISTS idx_netw_node_mac_addr ON netw_node_mac (mac_address);

CREATE TABLE IF NOT EXISTS netw_node_ip (
    netw_node_id INT REFERENCES netw_node(netw_node_id) ON DELETE CASCADE,
    ip_address INET NOT NULL,
    PRIMARY KEY (netw_node_id, ip_address)
);
-- зворотній пошук вузла за IP — критично для швидкості інжесту
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

CREATE TABLE IF NOT EXISTS netw_xact (
    effective_at TIMESTAMP NOT NULL,

    count INT NOT NULL,
    src_node_id INT REFERENCES netw_node(netw_node_id) ON DELETE CASCADE,
    dst_node_id INT REFERENCES netw_node(netw_node_id) ON DELETE CASCADE,
    src_port INT NOT NULL,
    dst_port INT NOT NULL,
    protocol VARCHAR(10) NOT NULL
) WITH (
    timescaledb.hypertable,
    timescaledb.segmentby = 'src_node_id',
    timescaledb.orderby = 'effective_at DESC',
    timescaledb.compress,
    timescaledb.chunk_time_interval = INTERVAL '7 days'
);

SELECT create_compression_policy('netw_xact', INTERVAL '7 days');
SELECT add_retention_policy('netw_xact', INTERVAL '91 days');

CREATE INDEX IF NOT EXISTS idx_netw_xact_ident ON netw_xact (
    dst_node_id, dst_port, src_node_id, src_port, protocol
);
-- для вибірок візуалізатора "останні N годин" по часу
CREATE INDEX IF NOT EXISTS idx_netw_xact_time ON netw_xact (effective_at DESC);


-- ============================================================================
-- Службові таблиці для cron-інжестора (не є частиною "графової" моделі,
-- але потрібні, щоб коректно виявляти зміну IP/MAC вузла-агента між запусками
-- і не перечитувати Elasticsearch з нуля щоразу).
-- ============================================================================

-- Курсор інкрементального читання з Elasticsearch (last processed timestamp).
CREATE TABLE IF NOT EXISTS ingest_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Поточна "сигнатура" (набір MAC + набір IP) кожного відомого агента
-- packetbeat (host.id / agent.id зі сторони Elastic Agent — стабільний
-- ідентифікатор, який не змінюється навіть коли hostname/IP/MAC хосту
-- змінюються). Використовується, щоб вирішити:
--   - сигнатура не змінилась -> далі логуємо під тим самим netw_node_id
--   - сигнатура змінилась     -> створюємо НОВИЙ netw_node_id (по ТЗ)
CREATE TABLE IF NOT EXISTS netw_agent_state (
    agent_host_id TEXT PRIMARY KEY,
    netw_node_id INT NOT NULL REFERENCES netw_node(netw_node_id) ON DELETE CASCADE,
    mac_set TEXT NOT NULL,      -- відсортовані MAC через кому
    ip_set TEXT NOT NULL,       -- відсортовані IP через кому
    hostname VARCHAR(255),
    os_name VARCHAR(255),
    os_version VARCHAR(255),
    os_type VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
