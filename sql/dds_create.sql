CREATE TABLE IF NOT EXISTS dds.brand(
    brand_id INTEGER PRIMARY KEY NOT NULL,
    brand VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dds.category(
    category_id VARCHAR(150) PRIMARY KEY NOT NULL,
    category_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dds.product(
    product_id INTEGER PRIMARY KEY NOT NULL,
    name_short VARCHAR(255) NOT NULL,
    category_id VARCHAR(150) REFERENCES dds.category(category_id) NOT NULL,
    pricing_line_id INTEGER,
    brand_id INTEGER REFERENCES dds.brand(brand_id) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.stores(
    pos VARCHAR(150) PRIMARY KEY NOT NULL,
    pos_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.transaction(
    transaction_id VARCHAR(150) NOT NULL,
    product_id INTEGER REFERENCES dds.product(product_id) NOT NULL,
    pos VARCHAR(150) REFERENCES dds.stores(pos) NOT NULL,
    recorded_on TIMESTAMP,
    quantity INTEGER NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    price_full NUMERIC(10,2) NOT NULL,
    order_type_id VARCHAR(150),
    PRIMARY KEY (transaction_id, product_id, pos)
);

CREATE TABLE IF NOT EXISTS dds.stock(
    available_on TIMESTAMP NOT NULL,
    product_id INTEGER REFERENCES dds.product(product_id) NOT NULL,
    pos VARCHAR(150) REFERENCES dds.stores(pos) NOT NULL,
    available_quantity NUMERIC(10,5) NOT NULL,
    cost_per_item NUMERIC(10,2),
    PRIMARY KEY(available_on, product_id, pos)
);