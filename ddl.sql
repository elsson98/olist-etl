
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BIT NOT NULL
);

CREATE TABLE dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix INT NOT NULL,
    customer_city VARCHAR(100) NOT NULL,
    customer_state CHAR(2) NOT NULL,
    geolocation_lat FLOAT NULL,
    geolocation_lng FLOAT NULL
);

CREATE TABLE dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name NVARCHAR(100) NULL,
    product_category_name_english NVARCHAR(100) NULL,
    product_name_lenght INT NULL,
    product_description_lenght INT NULL,
    product_photos_qty INT NULL,
    product_weight_g FLOAT NULL,
    product_length_cm FLOAT NULL,
    product_height_cm FLOAT NULL,
    product_width_cm FLOAT NULL
);

CREATE TABLE dim_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INT NOT NULL,
    seller_city VARCHAR(100) NOT NULL,
    seller_state CHAR(2) NOT NULL
);


CREATE TABLE fact_order_items (
    order_item_id INT NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    purchase_date_id INT NOT NULL,
    delivery_date_id INT NULL,
    price FLOAT NOT NULL,
    freight_value FLOAT NOT NULL,
    total_price FLOAT NOT NULL,
    profit_margin FLOAT NOT NULL,
    delivery_time INT NULL,
    payment_installments INT NULL,
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (purchase_date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (delivery_date_id) REFERENCES dim_date(date_id)
);
CREATE TABLE etl_error_log (
    error_id INT IDENTITY(1,1) PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    error_message VARCHAR(MAX) NOT NULL,
    error_time DATETIME NOT NULL
);

CREATE INDEX idx_fact_product ON fact_order_items(product_id);
CREATE INDEX idx_fact_seller ON fact_order_items(seller_id);
CREATE INDEX idx_fact_customer ON fact_order_items(customer_id);
CREATE INDEX idx_fact_purchase_date ON fact_order_items(purchase_date_id);
CREATE INDEX idx_fact_delivery_date ON fact_order_items(delivery_date_id);

CREATE INDEX idx_customer_state ON dim_customers(customer_state);
CREATE INDEX idx_product_category ON dim_products(product_category_name);
CREATE INDEX idx_seller_state ON dim_sellers(seller_state);
CREATE INDEX idx_date_year_month ON dim_date(year, month);