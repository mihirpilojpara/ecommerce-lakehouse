-- Builds the "source" OLTP database from the Olist CSVs in ./data/raw.
-- Every table gets an _updated_at column, kept current by a trigger, so the
-- bronze layer can pull only rows that changed since the last run.

\connect olist

CREATE TABLE customers (
    customer_id              text PRIMARY KEY,
    customer_unique_id       text NOT NULL,
    customer_zip_code_prefix text,
    customer_city            text,
    customer_state           text
);

CREATE TABLE geolocation (
    geolocation_zip_code_prefix text,
    geolocation_lat             double precision,
    geolocation_lng             double precision,
    geolocation_city            text,
    geolocation_state           text
);

CREATE TABLE orders (
    order_id                      text PRIMARY KEY,
    customer_id                   text NOT NULL,
    order_status                  text,
    order_purchase_timestamp      timestamp,
    order_approved_at             timestamp,
    order_delivered_carrier_date  timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp
);

CREATE TABLE order_items (
    order_id            text NOT NULL,
    order_item_id       integer NOT NULL,
    product_id          text,
    seller_id           text,
    shipping_limit_date timestamp,
    price               numeric(12, 2),
    freight_value       numeric(12, 2),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE order_payments (
    order_id             text NOT NULL,
    payment_sequential   integer NOT NULL,
    payment_type         text,
    payment_installments integer,
    payment_value        numeric(12, 2),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE order_reviews (
    review_id               text NOT NULL,
    order_id                text NOT NULL,
    review_score            integer,
    review_comment_title    text,
    review_comment_message  text,
    review_creation_date    timestamp,
    review_answer_timestamp timestamp
);

-- Column names keep the original dataset's spelling ("lenght"); silver fixes it.
CREATE TABLE products (
    product_id                 text PRIMARY KEY,
    product_category_name      text,
    product_name_lenght        integer,
    product_description_lenght integer,
    product_photos_qty         integer,
    product_weight_g           integer,
    product_length_cm          integer,
    product_height_cm          integer,
    product_width_cm           integer
);

CREATE TABLE sellers (
    seller_id              text PRIMARY KEY,
    seller_zip_code_prefix text,
    seller_city            text,
    seller_state           text
);

CREATE TABLE category_translation (
    product_category_name         text PRIMARY KEY,
    product_category_name_english text
);

COPY customers            FROM '/data/raw/olist_customers_dataset.csv'          WITH (FORMAT csv, HEADER true);
COPY geolocation          FROM '/data/raw/olist_geolocation_dataset.csv'        WITH (FORMAT csv, HEADER true);
COPY orders               FROM '/data/raw/olist_orders_dataset.csv'             WITH (FORMAT csv, HEADER true);
COPY order_items          FROM '/data/raw/olist_order_items_dataset.csv'        WITH (FORMAT csv, HEADER true);
COPY order_payments       FROM '/data/raw/olist_order_payments_dataset.csv'     WITH (FORMAT csv, HEADER true);
COPY order_reviews        FROM '/data/raw/olist_order_reviews_dataset.csv'      WITH (FORMAT csv, HEADER true);
COPY products             FROM '/data/raw/olist_products_dataset.csv'           WITH (FORMAT csv, HEADER true);
COPY sellers              FROM '/data/raw/olist_sellers_dataset.csv'            WITH (FORMAT csv, HEADER true);
COPY category_translation FROM '/data/raw/product_category_name_translation.csv' WITH (FORMAT csv, HEADER true);

-- Change tracking: _updated_at on every table, refreshed on every UPDATE.
CREATE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW._updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'customers', 'geolocation', 'orders', 'order_items', 'order_payments',
        'order_reviews', 'products', 'sellers', 'category_translation'
    ] LOOP
        EXECUTE format('ALTER TABLE %I ADD COLUMN _updated_at timestamptz NOT NULL DEFAULT now()', t);
        EXECUTE format('CREATE INDEX %I ON %I (_updated_at)', t || '_updated_at_idx', t);
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION set_updated_at()',
            t || '_set_updated_at', t
        );
    END LOOP;
END $$;
