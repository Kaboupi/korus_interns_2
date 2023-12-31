CREATE TABLE IF NOT EXISTS error.brand(
    brand_id VARCHAR(50),
    brand VARCHAR(50),
    brand_error_type VARCHAR(50) REFERENCES ref_info.error_types(error_id)
);

CREATE TABLE IF NOT EXISTS error.category(
    category_id VARCHAR(50),
    category_name VARCHAR(50),
    category_error_type VARCHAR(50) REFERENCES ref_info.error_types(error_id)
);

CREATE TABLE IF NOT EXISTS error.product(
    product_id VARCHAR(50),
    name_short VARCHAR(50),
    category_id VARCHAR(50),
    pricing_line_id VARCHAR(50),
    brand_id VARCHAR(50),
    product_error_type VARCHAR(50) REFERENCES ref_info.error_types(error_id)
);

CREATE TABLE IF NOT EXISTS error.transaction(
    transaction_id VARCHAR(50),
    product_id VARCHAR(50),
    pos VARCHAR(50),
    recorded_on VARCHAR(50),
    quantity VARCHAR(50),
    price VARCHAR(50),
    price_full VARCHAR(50),
    order_type_id VARCHAR(50),
    transaction_error_type VARCHAR(50) REFERENCES ref_info.error_types(error_id)
);

CREATE TABLE IF NOT EXISTS error.stores(
    pos VARCHAR(50),
    pos_name VARCHAR(50),
    stores_error_type VARCHAR(50) REFERENCES ref_info.error_types(error_id)
);

CREATE TABLE IF NOT EXISTS error.stock(
    available_on VARCHAR(50),
    product_id VARCHAR(50),
    pos VARCHAR(50),
    available_quantity VARCHAR(50),
    cost_per_item VARCHAR(50),
    stock_error_type VARCHAR(50) REFERENCES ref_info.error_types(error_id)
);
