PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recording_sessions (
    session_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    source TEXT NOT NULL,
    source_mode TEXT NOT NULL CHECK (source_mode IN ('offline', 'replay', 'live')),
    started_at TEXT NOT NULL,
    ended_at TEXT,
    trading_day TEXT,
    status TEXT NOT NULL CHECK (
        status IN ('recording', 'complete', 'degraded', 'failed', 'incomplete')
    ),
    config_fingerprint TEXT NOT NULL,
    last_ingest_sequence INTEGER NOT NULL DEFAULT -1,
    dropped_tick_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_sessions_trading_day_started
    ON recording_sessions (trading_day, started_at);

CREATE TABLE IF NOT EXISTS event_log (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES recording_sessions(session_id),
    ingest_sequence INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    event_type TEXT NOT NULL CHECK (
        event_type IN (
            'connection_status',
            'server_time',
            'instrument',
            'quote',
            'tick',
            'adapter_diagnostic'
        )
    ),
    source TEXT NOT NULL,
    source_mode TEXT NOT NULL CHECK (source_mode IN ('offline', 'replay', 'live')),
    connection_generation INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    broker_sequence INTEGER,
    dedupe_key TEXT NOT NULL,
    event_at TEXT,
    trading_day TEXT,
    received_at TEXT NOT NULL,
    metadata_version INTEGER,
    payload_json TEXT NOT NULL,
    raw_json TEXT,
    payload_sha256 TEXT NOT NULL,
    record_sha256 TEXT NOT NULL,
    UNIQUE (session_id, ingest_sequence),
    UNIQUE (session_id, dedupe_key)
);

CREATE INDEX IF NOT EXISTS idx_event_log_readback
    ON event_log (session_id, ingest_sequence);

CREATE INDEX IF NOT EXISTS idx_event_log_type_day
    ON event_log (event_type, trading_day);

CREATE TABLE IF NOT EXISTS instruments (
    instrument_id TEXT NOT NULL,
    metadata_version INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    venue TEXT NOT NULL,
    market_no INTEGER,
    stock_idx INTEGER,
    display_name TEXT,
    asset_class TEXT,
    currency TEXT,
    price_scale_text TEXT,
    quantity_scale_text TEXT,
    updated_at TEXT NOT NULL,
    raw_payload_json TEXT,
    PRIMARY KEY (instrument_id, metadata_version)
);

CREATE INDEX IF NOT EXISTS idx_instruments_symbol_version
    ON instruments (venue, symbol, metadata_version);

CREATE INDEX IF NOT EXISTS idx_instruments_market_stock
    ON instruments (market_no, stock_idx, updated_at);

CREATE TABLE IF NOT EXISTS quotes (
    quote_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL UNIQUE REFERENCES event_log(event_id),
    session_id TEXT NOT NULL REFERENCES recording_sessions(session_id),
    ingest_sequence INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    connection_generation INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    dedupe_key TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    metadata_version INTEGER,
    market_no_raw INTEGER NOT NULL,
    stock_idx_raw INTEGER NOT NULL,
    bid_raw INTEGER NOT NULL,
    ask_raw INTEGER NOT NULL,
    last_raw INTEGER NOT NULL,
    bid_qty_raw INTEGER,
    ask_qty_raw INTEGER,
    last_qty_raw INTEGER,
    bid_normalized_text TEXT,
    ask_normalized_text TEXT,
    last_normalized_text TEXT,
    event_at TEXT,
    trading_day TEXT,
    received_at TEXT NOT NULL,
    is_simulated INTEGER CHECK (is_simulated IN (0, 1) OR is_simulated IS NULL),
    is_long_callback INTEGER NOT NULL CHECK (is_long_callback IN (0, 1)),
    UNIQUE (session_id, ingest_sequence),
    UNIQUE (session_id, dedupe_key)
);

CREATE INDEX IF NOT EXISTS idx_quotes_instrument_event
    ON quotes (instrument_id, trading_day, event_at, quote_id);

CREATE TABLE IF NOT EXISTS ticks (
    tick_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL UNIQUE REFERENCES event_log(event_id),
    session_id TEXT NOT NULL REFERENCES recording_sessions(session_id),
    ingest_sequence INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    connection_generation INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    dedupe_key TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    metadata_version INTEGER,
    market_no_raw INTEGER NOT NULL,
    stock_idx_raw INTEGER NOT NULL,
    source_pointer_raw INTEGER NOT NULL,
    date_raw INTEGER NOT NULL,
    time_hms_raw INTEGER NOT NULL,
    time_subsecond_raw INTEGER NOT NULL,
    bid_raw INTEGER NOT NULL,
    ask_raw INTEGER NOT NULL,
    close_raw INTEGER NOT NULL,
    quantity_raw INTEGER NOT NULL,
    simulate_raw INTEGER NOT NULL,
    bid_normalized_text TEXT,
    ask_normalized_text TEXT,
    close_normalized_text TEXT,
    quantity_normalized_text TEXT,
    event_at TEXT,
    trading_day TEXT,
    received_at TEXT NOT NULL,
    is_simulated INTEGER CHECK (is_simulated IN (0, 1) OR is_simulated IS NULL),
    is_long_callback INTEGER NOT NULL CHECK (is_long_callback IN (0, 1)),
    UNIQUE (session_id, ingest_sequence),
    UNIQUE (session_id, dedupe_key)
);

CREATE INDEX IF NOT EXISTS idx_ticks_readback
    ON ticks (session_id, ingest_sequence, tick_id);

CREATE INDEX IF NOT EXISTS idx_ticks_instrument_day
    ON ticks (instrument_id, trading_day, event_at, tick_id);

CREATE INDEX IF NOT EXISTS idx_ticks_source_ptr
    ON ticks (
        session_id,
        connection_generation,
        instrument_id,
        source_pointer_raw
    );

CREATE TABLE IF NOT EXISTS connection_events (
    connection_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL UNIQUE REFERENCES event_log(event_id),
    session_id TEXT NOT NULL REFERENCES recording_sessions(session_id),
    ingest_sequence INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    connection_generation INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    dedupe_key TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'new',
            'starting',
            'com_ready',
            'logging_in',
            'logged_in',
            'entering_monitor',
            'connected',
            'stocks_ready',
            'subscribed',
            'disconnected',
            'reconnecting',
            'stopping',
            'error',
            'stopped'
        )
    ),
    broker_kind_raw INTEGER,
    broker_code_raw INTEGER,
    message TEXT,
    is_ready INTEGER NOT NULL CHECK (is_ready IN (0, 1)),
    changed_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    trading_day TEXT,
    UNIQUE (session_id, ingest_sequence),
    UNIQUE (session_id, dedupe_key)
);

CREATE INDEX IF NOT EXISTS idx_connection_events_session_time
    ON connection_events (session_id, changed_at, connection_event_id);
