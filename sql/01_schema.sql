CREATE TABLE IF NOT EXISTS etl_runs (
    run_id            UUID         PRIMARY KEY,
    source_file       TEXT         NOT NULL,
    started_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    status            VARCHAR(10)  NOT NULL DEFAULT 'running'
                      CHECK (status IN ('running', 'success', 'failed')),
    rows_extracted    INTEGER      CHECK (rows_extracted   >= 0),
    rows_duplicates   INTEGER      CHECK (rows_duplicates  >= 0),
    rows_rejected     INTEGER      CHECK (rows_rejected    >= 0),
    rows_inserted     INTEGER      CHECK (rows_inserted    >= 0),
    rows_updated      INTEGER      CHECK (rows_updated     >= 0),
    error_message     TEXT
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id    SERIAL        PRIMARY KEY,
    email          VARCHAR(255)  NOT NULL UNIQUE,      -- natural key
    customer_name  VARCHAR(150)  NOT NULL,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT chk_customer_email_format
        CHECK (email ~ '^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$')
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id     SERIAL        PRIMARY KEY,
    product_name   VARCHAR(150)  NOT NULL UNIQUE,      -- natural key
    category       VARCHAR(50)   NOT NULL,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_id     SMALLSERIAL   PRIMARY KEY,
    country_name   VARCHAR(100)  NOT NULL UNIQUE       -- natural key
);


CREATE TABLE IF NOT EXISTS fact_sales (
    order_id        VARCHAR(20)    PRIMARY KEY,
    customer_id     INTEGER        REFERENCES dim_customer (customer_id),  -- NULL = unknown customer
    product_id      INTEGER        NOT NULL REFERENCES dim_product (product_id),
    country_id      SMALLINT       NOT NULL REFERENCES dim_country (country_id),
    order_date      DATE           NOT NULL,
    quantity        INTEGER        NOT NULL CHECK (quantity > 0 AND quantity <= 1000),
    unit_price      NUMERIC(10, 2) NOT NULL CHECK (unit_price > 0),
    -- derived column computed by the database so it can never drift from its inputs
    total_amount    NUMERIC(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    rating          NUMERIC(2, 1)  CHECK (rating BETWEEN 1 AND 5),           -- NULL = not rated
    payment_method  VARCHAR(30)    NOT NULL
                    CHECK (payment_method IN ('Credit Card', 'Debit Card', 'PayPal',
                                              'Cash on Delivery', 'Bank Transfer', 'Unknown')),
    etl_run_id      UUID           NOT NULL REFERENCES etl_runs (run_id),
    loaded_at       TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT chk_order_id_format CHECK (order_id ~ '^ORD-[0-9]{6}$'),
    CONSTRAINT chk_order_date_range CHECK (order_date BETWEEN DATE '2000-01-01' AND DATE '2100-01-01')
);

CREATE UNLOGGED TABLE IF NOT EXISTS staging_sales (
    order_id        VARCHAR(20),
    customer_name   VARCHAR(150),
    customer_email  VARCHAR(255),
    product_name    VARCHAR(150),
    category        VARCHAR(50),
    quantity        INTEGER,
    unit_price      NUMERIC(10, 2),
    rating          NUMERIC(2, 1),
    country         VARCHAR(100),
    order_date      DATE,
    payment_method  VARCHAR(30)
);


CREATE TABLE IF NOT EXISTS rejected_records (
    reject_id      BIGSERIAL     PRIMARY KEY,
    run_id         UUID          NOT NULL REFERENCES etl_runs (run_id),
    source_row     INTEGER       NOT NULL,     -- 1-based line number in the raw file (excl. header)
    reject_reason  TEXT          NOT NULL,
    raw_record     JSONB         NOT NULL,     -- the original, untouched row
    rejected_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);
