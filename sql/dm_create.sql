CREATE TABLE IF NOT EXISTS dm.finalmart(
    date TIMESTAMP,
    product_id INTEGER,
    name_short VARCHAR(255),
    category_name VARCHAR(255),
    brand VARCHAR(255),
    pos_name VARCHAR(255),
    quantity INTEGER,
    available_quantity INTEGER,
    sale NUMERIC(10,2),
    price NUMERIC(10,2),
    revenue NUMERIC(10,2),
    cost_per_item NUMERIC(10,2),
    load_date TIMESTAMP
);