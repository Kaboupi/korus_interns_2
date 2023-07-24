CREATE TABLE IF NOT EXISTS dds.brand(
    brand_id INTEGER PRIMARY KEY,
    brand VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dds.category(
    category_id VARCHAR(150) PRIMARY KEY,
    category_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dds.product(
    product_id INTEGER PRIMARY KEY,
    name_short VARCHAR(255),
    category_id VARCHAR(150) REFERENCES dds.category(category_id),
    pricing_line_id INTEGER,
    brand_id INTEGER REFERENCES dds.brand(brand_id)
);

CREATE TABLE IF NOT EXISTS dds.stores(
    pos VARCHAR(150) PRIMARY KEY,
    pos_name VARCHAR(255)    
);

CREATE TABLE IF NOT EXISTS dds.transaction(
    transaction_id VARCHAR(150),
    product_id INTEGER REFERENCES dds.product(product_id),
    pos VARCHAR(150) REFERENCES dds.stores(pos),
    recorded_on TIMESTAMP,
    quantity INTEGER,
    price NUMERIC(10,2),
    price_full NUMERIC(10,2),
    order_type_id VARCHAR(150),
    PRIMARY KEY (transaction_id, product_id, pos)
);

CREATE TABLE IF NOT EXISTS dds.stock(
    available_on TIMESTAMP,
    product_id INTEGER REFERENCES dds.product(product_id),
    pos VARCHAR(150) REFERENCES dds.stores(pos),
    available_quantity NUMERIC(10,5),
    cost_per_item NUMERIC(10,2),
    PRIMARY KEY(available_on, product_id, pos)
);