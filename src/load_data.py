from datetime import datetime
import logging

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


def log_error(conn, error_message, table_name="unknown"):
    create_table_sql = """
    IF NOT EXISTS (
        SELECT * 
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'etl_error_log' AND s.name = 'dw'
    )
    BEGIN
        CREATE TABLE dw.etl_error_log (
            id INT IDENTITY(1,1) PRIMARY KEY,
            table_name VARCHAR(255) DEFAULT 'unknown',
            error_message NVARCHAR(MAX) NOT NULL,
            error_time DATETIME NOT NULL
        );
    END
    """
    conn.execute(text(create_table_sql))
    conn.commit()

    current_time = datetime.now()
    insert_sql = """
    INSERT INTO dw.etl_error_log (table_name, error_message, error_time)
    VALUES (:table_name, :error_message, :error_time)
    """
    conn.execute(text(insert_sql), {
        "table_name": table_name,
        "error_message": str(error_message),
        "error_time": current_time
    })
    conn.commit()


def load_dataframe_to_sql(df, table_name, engine, schema='dw', logger=None):
    if logger is None:
        logger = logging.getLogger()

    df.to_sql(table_name, engine, if_exists='replace', index=False, schema=schema)
    logger.info(f"Loaded {table_name} into SQL Server.")


def create_date_dimension(orders_df):
    purchase_dates = orders_df['order_purchase_timestamp'].dropna().dt.date.unique()
    delivered_dates = orders_df['order_delivered_customer_date'].dropna().dt.date.unique()
    unique_dates = pd.Series(list(set(purchase_dates) | set(delivered_dates))).sort_values()

    date_dim = pd.DataFrame({'date': pd.to_datetime(unique_dates)})
    date_dim['date_id'] = date_dim['date'].dt.strftime('%Y%m%d').astype(int)
    date_dim['year'] = date_dim['date'].dt.year
    date_dim['month'] = date_dim['date'].dt.month
    date_dim['day'] = date_dim['date'].dt.day
    date_dim['quarter'] = date_dim['date'].dt.quarter
    date_dim['day_of_week'] = date_dim['date'].dt.dayofweek
    date_dim['is_weekend'] = date_dim['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

    return date_dim


def create_customers_dimension(customers_df, processed_dfs):
    geolocation_df = processed_dfs.get('olist_geolocation_dataset.csv')

    if geolocation_df is not None:

        geo_grouped = geolocation_df.groupby('geolocation_zip_code_prefix').agg({
            'geolocation_lat': 'mean',
            'geolocation_lng': 'mean'
        }).reset_index()

        customers_dim = pd.merge(
            customers_df,
            geo_grouped,
            left_on='customer_zip_code_prefix',
            right_on='geolocation_zip_code_prefix',
            how='left'
        )
        customers_dim = customers_dim[['customer_id', 'customer_unique_id',
                                       'customer_zip_code_prefix', 'customer_city',
                                       'customer_state', 'geolocation_lat', 'geolocation_lng']]
    else:
        customers_dim = customers_df.copy()
        customers_dim['geolocation_lat'] = np.nan
        customers_dim['geolocation_lng'] = np.nan

    return customers_dim


def create_products_dimension(products_df, processed_dfs):
    category_translation_df = processed_dfs.get('product_category_name_translation.csv')

    if category_translation_df is not None:
        products_dim = pd.merge(
            products_df,
            category_translation_df,
            on='product_category_name',
            how='left'
        )
    else:
        products_dim = products_df.copy()
        products_dim['product_category_name_english'] = 'Unknown'
    return products_dim


def create_fact_table(order_items_df, orders_df, processed_dfs, date_mapping):
    # Merge order items with orders to get dates
    fact_order_items = pd.merge(
        order_items_df,
        orders_df[['order_id', 'customer_id', 'order_purchase_timestamp', 'order_delivered_customer_date']],
        on='order_id',
        how='left'
    )

    # Calculate required fields if not already present
    if 'total_price' not in fact_order_items.columns:
        fact_order_items['total_price'] = fact_order_items['price'] + fact_order_items['freight_value']

    if 'profit_margin' not in fact_order_items.columns:
        fact_order_items['profit_margin'] = fact_order_items['price'] - fact_order_items['freight_value']

    # Calculate delivery time if not already present
    if 'delivery_time' not in fact_order_items.columns:
        fact_order_items['delivery_time'] = (
                (fact_order_items['order_delivered_customer_date'] - fact_order_items['order_purchase_timestamp'])
                .dt.total_seconds() / (60 * 60 * 24)
        ).fillna(0).astype(int)

    # Ensure payment_installments column exists
    if 'payment_installments' not in fact_order_items.columns:
        payment_df = processed_dfs.get('olist_order_payments_dataset.csv')
        if payment_df is not None:
            payment_counts = payment_df.groupby('order_id')['payment_installments'].sum().reset_index()
            fact_order_items = pd.merge(fact_order_items, payment_counts, on='order_id', how='left')
            fact_order_items['payment_installments'] = fact_order_items['payment_installments'].fillna(1)
        else:
            fact_order_items['payment_installments'] = 1

    fact_order_items['purchase_date'] = fact_order_items['order_purchase_timestamp'].dt.date
    fact_order_items['delivery_date'] = fact_order_items['order_delivered_customer_date'].dt.date


    fact_order_items['purchase_date_id'] = fact_order_items['purchase_date'].map(date_mapping)
    fact_order_items['delivery_date_id'] = fact_order_items['delivery_date'].map(date_mapping)


    fact_columns = [
        'order_id', 'order_item_id', 'product_id', 'seller_id', 'customer_id',
        'purchase_date_id', 'delivery_date_id', 'price', 'freight_value',
        'total_price', 'profit_margin', 'delivery_time', 'payment_installments'
    ]

    fact_table = fact_order_items[fact_columns].copy()
    fact_table['delivery_date_id'] = fact_table['delivery_date_id'].fillna(0).astype(int)

    return fact_table


def load_to_sql_server(processed_dfs, connection_string, logger=None):
    if logger is None:
        logger = logging.getLogger()

    engine = create_engine(connection_string)

    try:
        with engine.connect() as conn:
            order_items_df = processed_dfs['olist_order_items_dataset.csv']
            customers_df = processed_dfs['olist_customers_dataset.csv']
            products_df = processed_dfs['olist_products_dataset.csv']
            sellers_df = processed_dfs['olist_sellers_dataset.csv']
            orders_df = processed_dfs['olist_orders_dataset.csv']

            logger.info("Creating date dimension...")
            date_dim = create_date_dimension(orders_df)

            logger.info("Creating customers dimension...")
            customers_dim = create_customers_dimension(customers_df, processed_dfs)

            logger.info("Creating products dimension...")
            products_dim = create_products_dimension(products_df, processed_dfs)

            date_mapping = dict(zip(date_dim['date'].dt.date, date_dim['date_id']))

            logger.info("Creating fact order items table...")
            fact_table = create_fact_table(order_items_df, orders_df, processed_dfs, date_mapping)

            load_dataframe_to_sql(date_dim, 'dim_date', engine, logger=logger)
            load_dataframe_to_sql(customers_dim, 'dim_customers', engine, logger=logger)
            load_dataframe_to_sql(products_dim, 'dim_products', engine, logger=logger)
            load_dataframe_to_sql(sellers_df, 'dim_sellers', engine, logger=logger)
            load_dataframe_to_sql(fact_table, 'fact_order_items', engine, logger=logger)

            logger.info("Successfully loaded all tables into SQL Server.")

    except Exception as e:
        with engine.connect() as conn:
            log_error(conn, str(e))
        logger.error(f"Error loading data to SQL Server: {e}", exc_info=True)